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
    namespace: str | None = None
    deployment: str | None = None


class TargetRequest(BaseModel):
    namespace: str = Field(..., min_length=1)
    deployment: str = Field(..., min_length=1)


@dataclass
class ScaleJob:
    job_id: str
    requested_pods: int
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



def _event_time_to_epoch(event_time):
    if event_time is None:
        return None
    try:
        return event_time.timestamp()
    except Exception:
        return None


def _event_source_name(ev) -> str:
    source = getattr(ev, "source", None)
    if source and getattr(source, "component", None):
        return str(source.component)
    reporting = getattr(ev, "reporting_controller", None)
    if reporting:
        return str(reporting)
    return ""


def _event_involved_name(ev) -> str:
    involved = getattr(ev, "involved_object", None)
    if involved and getattr(involved, "name", None):
        return str(involved.name)
    regarding = getattr(ev, "regarding", None)
    if regarding and getattr(regarding, "name", None):
        return str(regarding.name)
    return ""


def _event_timestamp(ev):
    for field in ("event_time", "last_timestamp", "first_timestamp"):
        value = getattr(ev, field, None)
        if value:
            return value
    return None


def _event_message(ev) -> str:
    message = getattr(ev, "message", None)
    if message:
        return str(message)
    note = getattr(ev, "note", None)
    if note:
        return str(note)
    return ""


def find_autoscaler_trigger_time(since_epoch: float) -> float | None:
    if not getattr(app.state, "k8s_enabled", False):
        return None

    trigger_reasons = {
        "TriggeredScaleUp",
        "ScaleUp",
        "TriggeredScaleUpLimit",
        "ScaleUpAttempt",
        "ScaleUpNotNeeded",
    }
    candidates = []

    try:
        core: client.CoreV1Api = app.state.core
        candidates.extend(core.list_namespaced_event(namespace="kube-system").items)
    except Exception:
        pass

    try:
        events_api = client.EventsV1Api()
        candidates.extend(events_api.list_namespaced_event(namespace="kube-system").items)
    except Exception:
        pass

    earliest = None
    for ev in candidates:
        reason = getattr(ev, "reason", None) or ""
        message = _event_message(ev).lower()
        reason_lower = str(reason).lower()
        if reason and reason not in trigger_reasons:
            if not any(token in reason_lower for token in ("scaleup", "scale-up", "scale up", "scale")) and "scale" not in message:
                continue
        source_name = _event_source_name(ev).lower()
        involved_name = _event_involved_name(ev).lower()
        if "cluster-autoscaler" not in source_name and "cluster-autoscaler" not in involved_name:
            continue
        ts = _event_timestamp(ev)
        epoch = _event_time_to_epoch(ts)
        if epoch is None or epoch < since_epoch:
            continue
        if earliest is None or epoch < earliest:
            earliest = epoch
    return earliest


def find_first_new_node_time(start_epoch: float) -> float | None:
    nodes = list_worker_nodes()
    earliest = None
    for node in nodes:
        created = node.metadata.creation_timestamp
        epoch = _event_time_to_epoch(created)
        if epoch is None or epoch < start_epoch:
            continue
        if earliest is None or epoch < earliest:
            earliest = epoch
    return earliest



def count_all_nodes() -> tuple[int, int]:
    if not getattr(app.state, "k8s_enabled", False):
        return 0, 0
    core: client.CoreV1Api = app.state.core
    nodes = core.list_node().items
    total = len(nodes)
    ready = 0
    for node in nodes:
        conditions = node.status.conditions or []
        for condition in conditions:
            if condition.type == "Ready" and condition.status == "True":
                ready += 1
                break
    return total, ready

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
            f"pods={job.requested_pods}, "
            f"max_pods_per_node={MAX_PODS_PER_NODE}.",
        )
        log(job, "Node scaling disabled; relying on Kubernetes autoscaler.")
        log(job, f"Target deployment {get_target()[0]}/{get_target()[1]} -> {job.requested_pods} replicas.")

        start = time.monotonic()
        nodes_ready = 0
        pods_ready = 0

        for step in range(1, 5):
            time.sleep(0.5)
            nodes_ready = nodes_ready + 1
            pods_ready = min(job.requested_pods, pods_ready + max(1, job.requested_pods // 3 or 1))
            log(job, f"Status: nodes {nodes_ready} (ready {nodes_ready}) | "
                     f"pods ready {pods_ready}/{job.requested_pods}.")

        t_nodes_seen = time.monotonic() - start
        t_nodes_ready = t_nodes_seen + 1.2
        t_pods_ready = t_nodes_ready + 1.5
        node_count = max(1, nodes_ready)
        total_provision = t_nodes_seen
        avg_provision = total_provision / node_count
        total_ready = t_nodes_ready
        avg_ready = total_ready / node_count
        total_pods = t_pods_ready
        avg_pods = total_pods / max(1, job.requested_pods)
        log(job, f"Node provisioning total time: {total_provision:.1f}s (avg {avg_provision:.1f}s over {node_count} nodes).")
        log(job, f"Node ready total time: {total_ready:.1f}s (avg {avg_ready:.1f}s over {node_count} nodes).")
        log(job, f"Pod readiness total time: {total_pods:.1f}s (avg {avg_pods:.2f}s per pod).")
    finally:
        job.done = True
        log(job, "Scale operation complete.")
        global active_job_id
        with jobs_lock:
            if active_job_id == job.job_id:
                active_job_id = None


def poll_until(job: ScaleJob, desired_pods: int) -> None:
    start = time.monotonic()
    start_epoch = time.time()
    nodes_start = list_worker_nodes()
    start_total, start_ready = count_nodes(nodes_start)
    all_total, all_ready = count_all_nodes()
    seen_nodes = {node.metadata.name for node in nodes_start if node.metadata and node.metadata.name}
    node_created: dict[str, float] = {}
    node_ready: dict[str, float] = {}

    log(job, f"Current worker nodes: {start_total} (ready {start_ready}); all nodes: {all_total} (ready {all_ready}).")
    log(job, f"Target deployment {get_target()[0]}/{get_target()[1]} -> {desired_pods} replicas.")

    t_autoscaler: Optional[float] = None
    t_node_obj: Optional[float] = None
    t_nodes_ready: Optional[float] = None
    t_pods_ready: Optional[float] = None
    last_ready_logged = start_ready
    max_total_seen = start_total
    last_all_ready_total: Optional[int] = None
    logged_autoscaler = False
    logged_node_obj = False
    logged_provision = False
    logged_provision_start = False

    last_status = 0.0
    while True:
        now = time.monotonic()
        if now - start > TIMEOUT_SECONDS:
            log(job, f"Timed out after {TIMEOUT_SECONDS}s.")
            break

        nodes = list_worker_nodes()
        total, ready = count_nodes(nodes)
        if total > max_total_seen:
            max_total_seen = total
        for node in nodes:
            name = node.metadata.name if node.metadata else None
            created = node.metadata.creation_timestamp if node.metadata else None
            if name and name not in seen_nodes:
                seen_nodes.add(name)
                ts = created.strftime("%H:%M:%S") + " UTC" if created else "unknown"
                created_epoch = _event_time_to_epoch(created)
                if created_epoch is not None and created_epoch >= start_epoch:
                    node_created[name] = created_epoch
                log(job, f"New node object detected: {name} at {ts}.")
        for node in nodes:
            name = node.metadata.name if node.metadata else None
            if not name or name not in node_created or name in node_ready:
                continue
            conditions = node.status.conditions or []
            for condition in conditions:
                if condition.type == "Ready" and condition.status == "True":
                    ready_epoch = _event_time_to_epoch(condition.last_transition_time) or time.time()
                    node_ready[name] = ready_epoch
                    break

        try:
            desired, ready_pods = read_deployment_ready(*get_target())
        except ApiException as exc:
            log(job, f"Failed to read deployment status: {exc}")
            desired, ready_pods = desired_pods, 0

        if t_autoscaler is None:
            trigger_epoch = find_autoscaler_trigger_time(start_epoch - 60)
            if trigger_epoch is not None:
                t_autoscaler = trigger_epoch
        if t_autoscaler is not None and not logged_autoscaler:
            logged_autoscaler = True
            log(job, f"Autoscaler triggered scale-up at {time.strftime('%H:%M:%S', time.gmtime(t_autoscaler))} UTC.")

        if t_node_obj is None:
            node_epoch = find_first_new_node_time(start_epoch)
            if node_epoch is not None:
                t_node_obj = node_epoch
        if t_node_obj is not None and not logged_node_obj:
            logged_node_obj = True
            log(job, f"First new node object at {time.strftime('%H:%M:%S', time.gmtime(t_node_obj))} UTC.")

        if not logged_provision_start and (t_autoscaler or t_node_obj):
            logged_provision_start = True
            if t_autoscaler:
                log(job, "Node provisioning started (autoscaler trigger observed).")
            else:
                log(job, "Node provisioning started (node object observed).")

        if not logged_provision and t_autoscaler and t_node_obj and t_node_obj >= t_autoscaler:
            logged_provision = True
            log(job, f"Node provisioning time (autoscaler->node object): {t_node_obj - t_autoscaler:.1f}s.")

        if not logged_provision and t_node_obj and not t_autoscaler:
            logged_provision = True
            log(job, f"Node provisioning time (start->node object): {t_node_obj - start_epoch:.1f}s.")

        if ready != last_ready_logged:
            if ready > last_ready_logged:
                log(job, f"Worker nodes Ready increased to {ready} in {now - start:.1f}s.")
            else:
                log(job, f"Worker nodes Ready changed to {ready} in {now - start:.1f}s.")
            last_ready_logged = ready

        if ready == total and total == max_total_seen and total > start_total:
            if last_all_ready_total != total:
                last_all_ready_total = total
                t_nodes_ready = now
                log(job, f"All worker nodes Ready reached {total} in {now - start:.1f}s.")

        if t_pods_ready is None and desired == desired_pods and ready_pods == desired_pods:
            t_pods_ready = now
            log(job, f"Pods Ready reached {desired_pods} in {now - start:.1f}s.")

        if now - last_status >= STATUS_INTERVAL:
            all_total, all_ready = count_all_nodes()
            log(job, f"Status: nodes {total} (ready {ready}); all nodes: {all_total} (ready {all_ready}) | pods ready {ready_pods}/{desired_pods}.")
            last_status = now

        if t_pods_ready:
            break

        time.sleep(POLL_INTERVAL)

    created_epochs = list(node_created.values())
    if created_epochs:
        total_provision = max(created_epochs) - start_epoch
        avg_provision = sum(max(0.0, t - start_epoch) for t in created_epochs) / len(created_epochs)
        log(job, f"Node provisioning total time: {total_provision:.1f}s (avg {avg_provision:.1f}s over {len(created_epochs)} nodes).")

    ready_epochs = [node_ready[name] for name in node_ready if name in node_created]
    ready_durations = []
    for name, ready_epoch in node_ready.items():
        created_epoch = node_created.get(name)
        if created_epoch is not None:
            ready_durations.append(max(0.0, ready_epoch - created_epoch))
    if ready_epochs:
        total_ready = max(ready_epochs) - start_epoch
        avg_ready = sum(ready_durations) / len(ready_durations) if ready_durations else 0.0
        log(job, f"Node ready total time: {total_ready:.1f}s (avg {avg_ready:.1f}s over {len(ready_durations)} nodes).")

    if t_pods_ready is not None:
        total_pods = t_pods_ready - start
        avg_pods = (total_pods / desired_pods) if desired_pods > 0 else 0.0
        log(job, f"Pod readiness total time: {total_pods:.1f}s (avg {avg_pods:.2f}s per pod).")



def run_scale_job(job: ScaleJob) -> None:
    try:
        log(
            job,
            "Scale request received: "
            f"pods={job.requested_pods}, "
            f"max_pods_per_node={MAX_PODS_PER_NODE}.",
        )

        log(job, "Node scaling disabled; relying on Kubernetes autoscaler.")

        try:
            scale_deployment(*get_target(), job.requested_pods)
            log(job, f"Deployment scaled to {job.requested_pods} replicas.")
        except ApiException as exc:
            log(job, f"Deployment scale API error: {exc}")

        poll_until(job, job.requested_pods)
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



@app.get("/api/deployment")
def get_deployment(namespace: str, name: str) -> JSONResponse:
    if not getattr(app.state, "k8s_enabled", False):
        return JSONResponse({"replicas": 0, "readyReplicas": 0})
    apps: client.AppsV1Api = app.state.apps
    dep = apps.read_namespaced_deployment(name=name, namespace=namespace)
    return JSONResponse({
        "replicas": dep.spec.replicas or 0,
        "readyReplicas": dep.status.ready_replicas or 0,
    })
@app.post("/api/scale")
def scale(req: ScaleRequest) -> JSONResponse:
    if req.namespace and req.deployment:
        set_target(req.namespace, req.deployment)
    desired_pods = req.pods

    with jobs_lock:
        global active_job_id
        if active_job_id:
            active_job = jobs.get(active_job_id)
            if active_job and not active_job.done:
                raise HTTPException(status_code=409, detail="A scale job is already running.")
        job_id = str(uuid.uuid4())
        job = ScaleJob(job_id=job_id, requested_pods=desired_pods)
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
            await asyncio.sleep(0.12)
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
