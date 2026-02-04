import asyncio
import json
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, conint
from kubernetes import client, config
from kubernetes.client import ApiException

APP_TITLE = "Scaleout Controller"

TARGET_NAMESPACE = os.getenv("TARGET_NAMESPACE", "default")
TARGET_DEPLOYMENT = os.getenv("TARGET_DEPLOYMENT", "scaleout-ui")
MAX_PODS_PER_NODE = int(os.getenv("MAX_PODS_PER_NODE", os.getenv("REPLICAS_PER_NODE", "20")))
MAX_PODS_PER_NODE = max(1, min(MAX_PODS_PER_NODE, 20))
NODE_SELECTOR = os.getenv("NODE_SELECTOR", "").strip()
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
STATUS_INTERVAL = float(os.getenv("STATUS_INTERVAL", "15"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "3600"))
UI_ONLY_MODE = os.getenv("UI_ONLY_MODE", "false").lower() in {"1", "true", "yes"}

app = FastAPI(title=APP_TITLE)


class ScaleRequest(BaseModel):
    pods: conint(ge=0, le=200000) = Field(..., description="Desired pod replica count")


class TargetRequest(BaseModel):
    namespace: str = Field(..., min_length=1)
    deployment: str = Field(..., min_length=1)


@dataclass
class ScaleJob:
    job_id: str
    requested_pods: int
    expected_nodes: int
    logs: List[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    done: bool = False
    error: Optional[str] = None


jobs: Dict[str, ScaleJob] = {}
active_job_id: Optional[str] = None
jobs_lock = threading.Lock()


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(job: ScaleJob, message: str) -> None:
    line = f"{utc_ts()} | {message}"
    with job.lock:
        job.logs.append(line)


def load_k8s() -> bool:
    if UI_ONLY_MODE:
        return False
    try:
        config.load_incluster_config()
        return True
    except Exception:
        config.load_kube_config()
        return True



@app.on_event("startup")
def startup() -> None:
    app.state.k8s_enabled = load_k8s()
    if app.state.k8s_enabled:
        app.state.core = client.CoreV1Api()
        app.state.apps = client.AppsV1Api()
    app.state.target_namespace = TARGET_NAMESPACE
    app.state.target_deployment = TARGET_DEPLOYMENT
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(template_path, "r", encoding="utf-8") as handle:
        app.state.index_template = handle.read()





def get_target() -> tuple[str, str]:
    return app.state.target_namespace, app.state.target_deployment


def set_target(namespace: str, deployment: str) -> None:
    app.state.target_namespace = namespace
    app.state.target_deployment = deployment

def list_worker_nodes() -> List[client.V1Node]:
    if not getattr(app.state, "k8s_enabled", False):
        return []
    core: client.CoreV1Api = app.state.core
    nodes = core.list_node(label_selector=NODE_SELECTOR or None).items
    if NODE_SELECTOR:
        return nodes

    filtered = []
    for node in nodes:
        labels = node.metadata.labels or {}
        if "node-role.kubernetes.io/control-plane" in labels:
            continue
        if "node-role.kubernetes.io/master" in labels:
            continue
        taints = node.spec.taints or []
        if any(t.key in ("node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master") for t in taints):
            continue
        filtered.append(node)
    return filtered


def count_nodes(nodes: List[client.V1Node]) -> Tuple[int, int]:
    total = len(nodes)
    ready = 0
    for node in nodes:
        conditions = node.status.conditions or []
        for condition in conditions:
            if condition.type == "Ready" and condition.status == "True":
                ready += 1
                break
    return total, ready


def read_deployment_ready(namespace: str, name: str) -> Tuple[int, int]:
    if not getattr(app.state, "k8s_enabled", False):
        return 0, 0
    apps: client.AppsV1Api = app.state.apps
    dep = apps.read_namespaced_deployment(name=name, namespace=namespace)
    desired = dep.spec.replicas or 0
    ready = dep.status.ready_replicas or 0
    return desired, ready


def scale_deployment(namespace: str, name: str, replicas: int) -> None:
    if not getattr(app.state, "k8s_enabled", False):
        return
    apps: client.AppsV1Api = app.state.apps
    body = {"spec": {"replicas": replicas}}
    apps.patch_namespaced_deployment_scale(name=name, namespace=namespace, body=body)


def simulate_scale_job(job: ScaleJob) -> None:
    try:
        log(
            job,
            "Scale request received: "
            f"pods={job.requested_pods}, expected_nodes={job.expected_nodes}, "
            f"max_pods_per_node={MAX_PODS_PER_NODE}.",
        )
        log(job, "Node scaling disabled; relying on Kubernetes autoscaler.")
        log(job, f"Target deployment {get_target()[0]}/{get_target()[1]} -> {job.requested_pods} replicas.")

        start = time.monotonic()
        expected_nodes = job.expected_nodes
        nodes_ready = 0
        pods_ready = 0

        for step in range(1, 5):
            time.sleep(0.5)
            nodes_ready = min(expected_nodes, nodes_ready + max(1, expected_nodes // 3 or 1))
            pods_ready = min(job.requested_pods, pods_ready + max(1, job.requested_pods // 3 or 1))
            log(job, f"Status: nodes {nodes_ready} (ready {nodes_ready}, expected {expected_nodes}) | "
                     f"pods ready {pods_ready}/{job.requested_pods}.")

        t_nodes_seen = time.monotonic() - start
        t_nodes_ready = t_nodes_seen + 1.2
        t_pods_ready = t_nodes_ready + 1.5
        log(job, f"Node provisioning time: {t_nodes_seen:.1f}s.")
        log(job, f"Node ready time: {t_nodes_ready:.1f}s.")
        log(job, f"Pod readiness time: {t_pods_ready:.1f}s.")
    finally:
        job.done = True
        log(job, "Scale operation complete.")
        global active_job_id
        with jobs_lock:
            if active_job_id == job.job_id:
                active_job_id = None


def poll_until(job: ScaleJob, expected_nodes: int, desired_pods: int) -> None:
    start = time.monotonic()
    nodes_start = list_worker_nodes()
    current_nodes, current_ready = count_nodes(nodes_start)
    track_nodes = expected_nodes > current_nodes

    if track_nodes:
        log(
            job,
            f"Current worker nodes: {current_nodes} (ready {current_ready}); "
            f"target {expected_nodes} (scale out).",
        )
    else:
        log(
            job,
            f"Current worker nodes: {current_nodes} (ready {current_ready}); "
            f"expected {expected_nodes} already satisfied.",
        )

    log(job, f"Target deployment {get_target()[0]}/{get_target()[1]} -> {desired_pods} replicas.")

    t_nodes_seen: Optional[float] = None
    t_nodes_ready: Optional[float] = None
    t_pods_ready: Optional[float] = None

    if not track_nodes:
        t_nodes_seen = start
        t_nodes_ready = start
        log(job, "Skipping node scale-in timing; autoscaler handles node reductions.")

    last_status = 0.0
    while True:
        now = time.monotonic()
        if now - start > TIMEOUT_SECONDS:
            log(job, f"Timed out after {TIMEOUT_SECONDS}s.")
            break

        nodes = list_worker_nodes()
        total, ready = count_nodes(nodes)
        try:
            desired, ready_pods = read_deployment_ready(*get_target())
        except ApiException as exc:
            log(job, f"Failed to read deployment status: {exc}")
            desired, ready_pods = desired_pods, 0

        if track_nodes and t_nodes_seen is None and total >= expected_nodes:
            t_nodes_seen = now
            log(job, f"Worker node count reached {expected_nodes} in {now - start:.1f}s.")

        if track_nodes and t_nodes_ready is None and ready >= expected_nodes:
            t_nodes_ready = now
            log(job, f"Worker nodes Ready reached {expected_nodes} in {now - start:.1f}s.")

        if t_pods_ready is None and desired == desired_pods and ready_pods == desired_pods:
            t_pods_ready = now
            log(job, f"Pods Ready reached {desired_pods} in {now - start:.1f}s.")

        if now - last_status >= STATUS_INTERVAL:
            log(
                job,
                f"Status: nodes {total} (ready {ready}, expected {expected_nodes}) | "
                f"pods ready {ready_pods}/{desired_pods}.",
            )
            last_status = now

        if t_nodes_seen and t_nodes_ready and t_pods_ready:
            break

        time.sleep(POLL_INTERVAL)

    if t_nodes_seen:
        log(job, f"Node provisioning time: {t_nodes_seen - start:.1f}s.")
    if t_nodes_ready:
        log(job, f"Node ready time: {t_nodes_ready - start:.1f}s.")
    if t_pods_ready:
        log(job, f"Pod readiness time: {t_pods_ready - start:.1f}s.")



def run_scale_job(job: ScaleJob) -> None:
    try:
        log(
            job,
            "Scale request received: "
            f"pods={job.requested_pods}, expected_nodes={job.expected_nodes}, "
            f"max_pods_per_node={MAX_PODS_PER_NODE}.",
        )

        log(job, "Node scaling disabled; relying on Kubernetes autoscaler.")

        try:
            scale_deployment(*get_target(), job.requested_pods)
            log(job, f"Deployment scaled to {job.requested_pods} replicas.")
        except ApiException as exc:
            log(job, f"Deployment scale API error: {exc}")

        poll_until(job, job.expected_nodes, job.requested_pods)
    except Exception as exc:
        job.error = str(exc)
        log(job, f"Unhandled error: {exc}")
    finally:
        job.done = True
        log(job, "Scale operation complete.")
        global active_job_id
        with jobs_lock:
            if active_job_id == job.job_id:
                active_job_id = None


def render_index() -> str:
    config_payload = {
        "maxPodsPerNode": MAX_PODS_PER_NODE,
        "targetNamespace": get_target()[0],
        "targetDeployment": get_target()[1],
        "nodeSelector": NODE_SELECTOR,
    }
    config_json = json.dumps(config_payload)
    template = app.state.index_template
    page = template.replace("__CONFIG_JSON__", config_json)
    page = page.replace("__MAX_PODS_PER_NODE__", str(MAX_PODS_PER_NODE))
    return page



@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return render_index()


@app.get("/healthz")
def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/api/config")
def config_endpoint() -> JSONResponse:
    return JSONResponse(
        {
            "maxPodsPerNode": MAX_PODS_PER_NODE,
            "targetNamespace": get_target()[0],
            "targetDeployment": get_target()[1],
            "nodeSelector": NODE_SELECTOR,
        }
    )





@app.get("/api/deployments")
def list_deployments() -> JSONResponse:
    if not getattr(app.state, "k8s_enabled", False):
        return JSONResponse({"items": []})
    apps: client.AppsV1Api = app.state.apps
    items = []
    for dep in apps.list_deployment_for_all_namespaces().items:
        items.append({"namespace": dep.metadata.namespace, "name": dep.metadata.name})
    items.sort(key=lambda x: (x["namespace"], x["name"]))
    return JSONResponse({"items": items})


@app.post("/api/target")
def set_target_endpoint(req: TargetRequest) -> JSONResponse:
    set_target(req.namespace, req.deployment)
    return JSONResponse({"targetNamespace": req.namespace, "targetDeployment": req.deployment})
@app.post("/api/scale")
def scale(req: ScaleRequest) -> JSONResponse:
    desired_pods = req.pods
    expected_nodes = math.ceil(desired_pods / MAX_PODS_PER_NODE) if MAX_PODS_PER_NODE > 0 else 0

    with jobs_lock:
        global active_job_id
        if active_job_id:
            active_job = jobs.get(active_job_id)
            if active_job and not active_job.done:
                raise HTTPException(status_code=409, detail="A scale job is already running.")
        job_id = str(uuid.uuid4())
        job = ScaleJob(job_id=job_id, requested_pods=desired_pods, expected_nodes=expected_nodes)
        jobs[job_id] = job
        active_job_id = job_id

    target = simulate_scale_job if not getattr(app.state, "k8s_enabled", False) else run_scale_job
    thread = threading.Thread(target=target, args=(job,), daemon=True)
    thread.start()

    return JSONResponse({"jobId": job_id})


@app.get("/api/stream/{job_id}")
async def stream(job_id: str) -> StreamingResponse:
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    async def event_generator():
        cursor = 0
        while True:
            await asyncio.sleep(0.5)
            with job.lock:
                logs = job.logs[cursor:]
                done = job.done
            for line in logs:
                yield f"data: {line}\n\n"
            cursor += len(logs)
            if done:
                yield "event: done\ndata: done\n\n"
                break

    return StreamingResponse(event_generator(), media_type="text/event-stream")
