import asyncio
import json
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Pattern, Set, Tuple

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, conint
from kubernetes import client, config
from kubernetes.client import ApiException

APP_TITLE = "Scaleout Controller"

TARGET_NAMESPACE = os.getenv("TARGET_NAMESPACE", "default")
TARGET_DEPLOYMENT = os.getenv("TARGET_DEPLOYMENT", "scaleout-ui")
NODE_SELECTOR = os.getenv("NODE_SELECTOR", "").strip()
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
STATUS_INTERVAL = float(os.getenv("STATUS_INTERVAL", "15"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "3600"))
UI_ONLY_MODE = os.getenv("UI_ONLY_MODE", "false").lower() in {"1", "true", "yes"}
STREAM_HEARTBEAT_INTERVAL = float(os.getenv("STREAM_HEARTBEAT_INTERVAL", "10"))
STREAM_RETRY_MILLISECONDS = int(os.getenv("STREAM_RETRY_MILLISECONDS", "2000"))
CONTROLLER_LOG_OVERLAP_SECONDS = float(os.getenv("CONTROLLER_LOG_OVERLAP_SECONDS", "5"))
CONTROLLER_LOG_TAIL_LINES = int(os.getenv("CONTROLLER_LOG_TAIL_LINES", "200"))
CONTROLLER_DISCOVERY_INTERVAL = float(os.getenv("CONTROLLER_DISCOVERY_INTERVAL", "30"))
MAX_COMPLETED_JOBS = int(os.getenv("MAX_COMPLETED_JOBS", "20"))
CLIENT_MAX_LOG_LINES = int(os.getenv("CLIENT_MAX_LOG_LINES", "1500"))
CLIENT_MAX_EVENT_ROWS_PER_RUN = int(os.getenv("CLIENT_MAX_EVENT_ROWS_PER_RUN", "500"))
CLIENT_MAX_EVENT_HISTORY_RUNS = int(os.getenv("CLIENT_MAX_EVENT_HISTORY_RUNS", "12"))
CLIENT_MAX_STATUS_SAMPLES = int(os.getenv("CLIENT_MAX_STATUS_SAMPLES", "1500"))

app = FastAPI(title=APP_TITLE)


def _compile_patterns(raw_patterns: Tuple[str, ...]) -> Tuple[Pattern[str], ...]:
    return tuple(re.compile(pattern, re.IGNORECASE) for pattern in raw_patterns if pattern)


def _pattern_config(env_key: str, defaults: Tuple[str, ...]) -> Tuple[Pattern[str], ...]:
    raw = os.getenv(env_key, "").strip()
    if not raw:
        return _compile_patterns(defaults)
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return _compile_patterns(tuple(str(item) for item in parsed))
    except json.JSONDecodeError:
        pass
    return _compile_patterns(tuple(part.strip() for part in raw.split("||") if part.strip()))


CONTROLLER_SOURCES = (
    {
        "name": "Autoscaler",
        "namespaces": ("kube-system",),
        "pod_tokens": ("cluster-autoscaler",),
        "event_source_tokens": ("cluster-autoscaler",),
        "event_exact_reasons": ("TriggeredScaleUp", "ScaleUp", "TriggeredScaleUpLimit", "ScaleUpAttempt"),
        "event_reason_patterns": _pattern_config(
            "AUTOSCALER_EVENT_REASON_PATTERNS",
            (r"^(triggered)?scaleup(limit|attempt)?$",),
        ),
        "event_message_patterns": _pattern_config(
            "AUTOSCALER_EVENT_MESSAGE_PATTERNS",
            (
                r"\bscale[- ]?up\b",
                r"\bunschedulable\b",
                r"\bnode group\b",
                r"\bupcoming \d+ nodes?\b",
            ),
        ),
        "log_relevant_patterns": _pattern_config(
            "AUTOSCALER_LOG_RELEVANT_PATTERNS",
            (
                r"\bscale[- ]?up\b",
                r"\bunschedulable\b",
                r"\bnode group\b",
                r"\bupcoming \d+ nodes?\b",
                r"\bprovision\w*\b",
            ),
        ),
        "log_trigger_patterns": _pattern_config(
            "AUTOSCALER_LOG_TRIGGER_PATTERNS",
            (
                r"\btriggeredscaleup\b",
                r"\bscale[- ]?up\b",
                r"\bupcoming \d+ nodes?\b",
            ),
        ),
    },
    {
        "name": "Karpenter",
        "namespaces": ("karpenter", "kube-system"),
        "pod_tokens": ("karpenter",),
        "event_source_tokens": ("karpenter",),
        "event_exact_reasons": (),
        "event_reason_patterns": _pattern_config(
            "KARPENTER_EVENT_REASON_PATTERNS",
            (
                r"\bprovision\w*\b",
                r"\blaunch\w*\b",
                r"\bcreate\w*\b",
                r"\bnodeclaim\b",
            ),
        ),
        "event_message_patterns": _pattern_config(
            "KARPENTER_EVENT_MESSAGE_PATTERNS",
            (
                r"\bprovision\w*\b",
                r"\bnodeclaim\b",
                r"\blaunch\w*\b",
                r"\bregister\w*\b",
                r"\binitializ\w*\b",
                r"\bcreated node\b",
            ),
        ),
        "log_relevant_patterns": _pattern_config(
            "KARPENTER_LOG_RELEVANT_PATTERNS",
            (
                r"\bprovision\w*\b",
                r"\bnodeclaim\b",
                r"\blaunch\w*\b",
                r"\bregister\w*\b",
                r"\binitializ\w*\b",
                r"\bcreated node\b",
                r"\bready\b",
            ),
        ),
        "log_trigger_patterns": _pattern_config(
            "KARPENTER_LOG_TRIGGER_PATTERNS",
            (
                r"\bprovisionable\b",
                r"\bcomputed new nodeclaim\b",
                r"\bcreated nodeclaim\b",
                r"\blaunched nodeclaim\b",
                r"\blaunching nodeclaim\b",
                r"\bcreated node\b",
            ),
        ),
    },
)


@dataclass
class LogEntry:
    seq: int
    line: str


@dataclass(frozen=True)
class ControllerPodRef:
    namespace: str
    name: str
    containers: Tuple[str, ...]


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
    logs: List[LogEntry] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    done: bool = False
    error: Optional[str] = None
    next_log_seq: int = 0
    controller_log_signatures: Set[str] = field(default_factory=set)
    controller_trigger_times: Dict[str, float] = field(default_factory=dict)
    finished_at: Optional[float] = None


jobs: Dict[str, ScaleJob] = {}
active_job_id: Optional[str] = None
jobs_lock = threading.Lock()


def utc_ts(at: datetime | None = None) -> str:
    instant = at.astimezone(timezone.utc) if at else datetime.now(timezone.utc)
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def append_job_log(job: ScaleJob, line: str) -> None:
    with job.lock:
        job.next_log_seq += 1
        job.logs.append(LogEntry(seq=job.next_log_seq, line=line))


def log(job: ScaleJob, message: str, at: datetime | None = None) -> None:
    append_job_log(job, f"{utc_ts(at)} | {message}")


def finish_job(job: ScaleJob) -> None:
    log(job, "Scale operation complete.")
    with job.lock:
        job.done = True
        if job.finished_at is None:
            job.finished_at = time.time()


def prune_completed_jobs() -> None:
    if MAX_COMPLETED_JOBS < 0:
        return

    with jobs_lock:
        completed_jobs = [
            (job_id, job)
            for job_id, job in jobs.items()
            if job.done and job_id != active_job_id
        ]
        if len(completed_jobs) <= MAX_COMPLETED_JOBS:
            return

        completed_jobs.sort(key=lambda item: item[1].finished_at or 0.0, reverse=True)
        for job_id, _ in completed_jobs[MAX_COMPLETED_JOBS:]:
            jobs.pop(job_id, None)


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
        app.state.events = client.EventsV1Api()
        app.state.controller_pod_cache = {}
        app.state.controller_pod_cache_at = {}
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


def _parse_rfc3339(value: str) -> datetime | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        return None


def _split_k8s_log_line(raw_line: str) -> tuple[datetime | None, str]:
    line = raw_line.strip()
    if not line:
        return None, ""
    prefix, sep, remainder = line.partition(" ")
    if not sep:
        return None, line
    parsed = _parse_rfc3339(prefix)
    if parsed is None:
        return None, line
    return parsed, remainder.strip()


def _normalize_controller_log_line(raw_line: str) -> tuple[datetime | None, str]:
    ts, payload = _split_k8s_log_line(raw_line)
    text = payload.strip()
    if not text:
        return ts, ""

    if text.startswith("{") and text.endswith("}"):
        try:
            record = json.loads(text)
        except json.JSONDecodeError:
            return ts, text
        message = ""
        for key in ("message", "msg", "log", "note"):
            if record.get(key):
                message = str(record[key]).strip()
                break
        if not message:
            return ts, text
        if ts is None:
            for key in ("time", "ts", "timestamp"):
                value = record.get(key)
                if value:
                    ts = _parse_rfc3339(str(value))
                    if ts is not None:
                        break
        logger_name = record.get("logger")
        level = record.get("level")
        prefixes = []
        if logger_name:
            prefixes.append(str(logger_name))
        if level and str(level).upper() not in {"INFO", ""}:
            prefixes.append(str(level))
        if prefixes:
            return ts, f"{' / '.join(prefixes)}: {message}"
        return ts, message

    return ts, text


def _matches_tokens(parts: List[str], tokens: tuple[str, ...]) -> bool:
    text = " ".join(part for part in parts if part).lower()
    return any(token in text for token in tokens)


def _matches_patterns(text: str, patterns: Tuple[Pattern[str], ...]) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def _pod_matches_controller(pod: client.V1Pod, pod_tokens: tuple[str, ...]) -> bool:
    metadata = pod.metadata
    labels = metadata.labels or {} if metadata else {}
    container_names = [container.name for container in (pod.spec.containers or [])]
    parts = [
        metadata.name if metadata else "",
        *labels.keys(),
        *[str(value) for value in labels.values()],
        *container_names,
    ]
    return _matches_tokens(parts, pod_tokens)


def _list_candidate_controller_pods(controller_config: dict) -> List[client.V1Pod]:
    core: client.CoreV1Api = app.state.core
    namespaces = list(dict.fromkeys(controller_config["namespaces"]))
    pods: List[client.V1Pod] = []

    for namespace in namespaces:
        try:
            pods.extend(core.list_namespaced_pod(namespace=namespace).items)
        except Exception:
            continue

    matched = [pod for pod in pods if _pod_matches_controller(pod, controller_config["pod_tokens"])]
    if matched:
        return matched

    try:
        all_pods = core.list_pod_for_all_namespaces().items
    except Exception:
        return []
    return [pod for pod in all_pods if _pod_matches_controller(pod, controller_config["pod_tokens"])]


def discover_controller_pods(controller_config: dict) -> List[ControllerPodRef]:
    if not getattr(app.state, "k8s_enabled", False):
        return []

    cache_key = controller_config["name"]
    cache = getattr(app.state, "controller_pod_cache", {})
    cache_at = getattr(app.state, "controller_pod_cache_at", {})
    now = time.monotonic()
    cached_at = cache_at.get(cache_key)
    if cached_at is not None and now - cached_at < CONTROLLER_DISCOVERY_INTERVAL:
        return cache.get(cache_key, [])

    refs = []
    seen = set()
    for pod in _list_candidate_controller_pods(controller_config):
        namespace = pod.metadata.namespace if pod.metadata and pod.metadata.namespace else "default"
        name = pod.metadata.name if pod.metadata else "unknown"
        containers = tuple(container.name for container in (pod.spec.containers or []))
        signature = (namespace, name, containers)
        if signature in seen:
            continue
        seen.add(signature)
        refs.append(ControllerPodRef(namespace=namespace, name=name, containers=containers))

    cache[cache_key] = refs
    cache_at[cache_key] = now
    app.state.controller_pod_cache = cache
    app.state.controller_pod_cache_at = cache_at
    return refs


def _is_controller_log_relevant(config_item: dict, message: str) -> bool:
    return _matches_patterns(message, config_item["log_relevant_patterns"])


def _is_controller_trigger_log(config_item: dict, message: str) -> bool:
    return _matches_patterns(message, config_item["log_trigger_patterns"])


def _event_matches_controller(config_item: dict, ev) -> bool:
    source_name = _event_source_name(ev).lower()
    involved_name = _event_involved_name(ev).lower()
    if not any(token in source_name or token in involved_name for token in config_item["event_source_tokens"]):
        return False

    reason = str(getattr(ev, "reason", "") or "")
    if reason and reason in config_item["event_exact_reasons"]:
        return True

    message = _event_message(ev)
    if _matches_patterns(reason, config_item["event_reason_patterns"]):
        return True
    return _matches_patterns(message, config_item["event_message_patterns"])


def _record_controller_trigger(job: ScaleJob, controller_name: str, when: datetime | None) -> None:
    epoch = when.timestamp() if when else time.time()
    with job.lock:
        existing = job.controller_trigger_times.get(controller_name)
        if existing is None or epoch < existing:
            job.controller_trigger_times[controller_name] = epoch


def ingest_controller_logs(job: ScaleJob, since_epoch: float) -> None:
    if not getattr(app.state, "k8s_enabled", False):
        return

    core: client.CoreV1Api = app.state.core
    now_epoch = time.time()
    since_seconds = max(1, int(now_epoch - since_epoch) + 1)

    for controller_config in CONTROLLER_SOURCES:
        for pod_ref in discover_controller_pods(controller_config):
            for container_name in pod_ref.containers:
                try:
                    payload = core.read_namespaced_pod_log(
                        name=pod_ref.name,
                        namespace=pod_ref.namespace,
                        container=container_name,
                        since_seconds=since_seconds,
                        tail_lines=CONTROLLER_LOG_TAIL_LINES,
                        timestamps=True,
                    )
                except Exception:
                    continue

                for raw_line in payload.splitlines():
                    observed_at, normalized = _normalize_controller_log_line(raw_line)
                    if not normalized or not _is_controller_log_relevant(controller_config, normalized):
                        continue
                    signature = "|".join(
                        [
                            controller_config["name"],
                            pod_ref.namespace,
                            pod_ref.name,
                            container_name,
                            utc_ts(observed_at) if observed_at else "",
                            normalized,
                        ]
                    )
                    with job.lock:
                        if signature in job.controller_log_signatures:
                            continue
                        job.controller_log_signatures.add(signature)
                    if _is_controller_trigger_log(controller_config, normalized):
                        _record_controller_trigger(job, controller_config["name"], observed_at)
                    source = f"{pod_ref.namespace}/{pod_ref.name}"
                    if len(pod_ref.containers) > 1:
                        source = f"{source}/{container_name}"
                    log(job, f"{controller_config['name']} log [{source}]: {normalized}", at=observed_at)


def find_controller_trigger_time(controller_name: str, since_epoch: float) -> float | None:
    if not getattr(app.state, "k8s_enabled", False):
        return None

    controller_config = next((item for item in CONTROLLER_SOURCES if item["name"] == controller_name), None)
    if controller_config is None:
        return None

    candidates = []
    namespaces = list(dict.fromkeys(controller_config["namespaces"]))

    for namespace in namespaces:
        try:
            core: client.CoreV1Api = app.state.core
            candidates.extend(core.list_namespaced_event(namespace=namespace).items)
        except Exception:
            pass

        try:
            events_api: client.EventsV1Api = app.state.events
            candidates.extend(events_api.list_namespaced_event(namespace=namespace).items)
        except Exception:
            pass

    earliest = None
    for ev in candidates:
        if not _event_matches_controller(controller_config, ev):
            continue
        ts = _event_timestamp(ev)
        epoch = _event_time_to_epoch(ts)
        if epoch is None or epoch < since_epoch:
            continue
        if earliest is None or epoch < earliest:
            earliest = epoch
    return earliest


def find_autoscaler_trigger_time(since_epoch: float) -> float | None:
    return find_controller_trigger_time("Autoscaler", since_epoch)


def find_karpenter_trigger_time(since_epoch: float) -> float | None:
    return find_controller_trigger_time("Karpenter", since_epoch)



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
        if any(t.key.startswith("karpenter.sh/disruption") or t.key == "karpenter.sh/disrupted" for t in taints if t.key):
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
            f"pods={job.requested_pods}.",
        )
        log(job, "Node scaling is external; observing cluster-autoscaler and Karpenter.")
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
        t_pods_ready = t_nodes_seen + 2.7
        node_count = max(1, nodes_ready)
        total_provision = t_nodes_seen
        avg_provision = total_provision / node_count
        total_ready = t_nodes_seen + 1.2
        avg_ready = total_ready / node_count
        total_pods = t_pods_ready
        avg_pods = total_pods / max(1, job.requested_pods)
        log(job, f"Node provisioning total time: {total_provision:.1f}s (avg {avg_provision:.1f}s over {node_count} nodes).")
        log(job, f"Node ready total time: {total_ready:.1f}s (avg {avg_ready:.1f}s over {node_count} nodes).")
        log(job, f"Pod readiness total time: {total_pods:.1f}s (avg {avg_pods:.2f}s per pod).")
    finally:
        finish_job(job)
        global active_job_id
        with jobs_lock:
            if active_job_id == job.job_id:
                active_job_id = None
        prune_completed_jobs()


def poll_until(job: ScaleJob, desired_pods: int) -> None:
    start = time.monotonic()
    start_epoch = time.time()
    trigger_search_since = max(0.0, start_epoch - max(CONTROLLER_LOG_OVERLAP_SECONDS, 10))
    nodes_start = list_worker_nodes()
    start_total, start_ready = count_nodes(nodes_start)
    all_total, all_ready = count_all_nodes()
    seen_nodes = {node.metadata.name for node in nodes_start if node.metadata and node.metadata.name}
    node_created: dict[str, float] = {}
    node_ready: dict[str, float] = {}
    for node in nodes_start:
        name = node.metadata.name if node.metadata else None
        created = node.metadata.creation_timestamp if node.metadata else None
        created_epoch = _event_time_to_epoch(created)
        if name and created_epoch is not None and created_epoch >= start_epoch:
            node_created[name] = created_epoch

    log(job, f"Current worker nodes: {start_total} (ready {start_ready}); all nodes: {all_total} (ready {all_ready}).")
    log(job, f"Target deployment {get_target()[0]}/{get_target()[1]} -> {desired_pods} replicas.")

    t_autoscaler: Optional[float] = None
    t_karpenter: Optional[float] = None
    t_node_obj: Optional[float] = None
    t_pods_ready: Optional[float] = None
    last_ready_logged = start_ready
    max_total_seen = start_total
    last_all_ready_total: Optional[int] = None
    logged_autoscaler = False
    logged_karpenter = False
    logged_node_obj = False
    logged_provision = False
    logged_provision_start = False
    controller_log_since_epoch = trigger_search_since

    last_status = 0.0
    while True:
        now = time.monotonic()
        if now - start > TIMEOUT_SECONDS:
            log(job, f"Timed out after {TIMEOUT_SECONDS}s.")
            break

        scan_started_epoch = time.time()
        ingest_controller_logs(job, controller_log_since_epoch)
        controller_log_since_epoch = max(start_epoch, scan_started_epoch - CONTROLLER_LOG_OVERLAP_SECONDS)

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

        with job.lock:
            autoscaler_from_logs = job.controller_trigger_times.get("Autoscaler")
            karpenter_from_logs = job.controller_trigger_times.get("Karpenter")

        if t_autoscaler is None:
            if autoscaler_from_logs is not None:
                t_autoscaler = autoscaler_from_logs
        if t_autoscaler is None:
            trigger_epoch = find_autoscaler_trigger_time(trigger_search_since)
            if trigger_epoch is not None:
                t_autoscaler = trigger_epoch
        if t_autoscaler is not None and not logged_autoscaler:
            logged_autoscaler = True
            log(job, f"Autoscaler triggered scale-up at {time.strftime('%H:%M:%S', time.gmtime(t_autoscaler))} UTC.")

        if t_karpenter is None:
            if karpenter_from_logs is not None:
                t_karpenter = karpenter_from_logs
        if t_karpenter is None:
            karpenter_epoch = find_karpenter_trigger_time(trigger_search_since)
            if karpenter_epoch is not None:
                t_karpenter = karpenter_epoch
        if t_karpenter is not None and not logged_karpenter:
            logged_karpenter = True
            log(job, f"Karpenter triggered scale-up at {time.strftime('%H:%M:%S', time.gmtime(t_karpenter))} UTC.")

        if t_node_obj is None and node_created:
            t_node_obj = min(node_created.values())
        if t_node_obj is not None and not logged_node_obj:
            logged_node_obj = True
            log(job, f"First new node object at {time.strftime('%H:%M:%S', time.gmtime(t_node_obj))} UTC.")

        if not logged_provision_start and (t_autoscaler or t_karpenter or t_node_obj):
            logged_provision_start = True
            if t_autoscaler:
                log(job, "Node provisioning started (autoscaler trigger observed).")
            elif t_karpenter:
                log(job, "Node provisioning started (karpenter trigger observed).")
            else:
                log(job, "Node provisioning started (node object observed).")

        if not logged_provision and t_autoscaler and t_node_obj and t_node_obj >= t_autoscaler:
            logged_provision = True
            log(job, f"Node provisioning time (autoscaler->node object): {t_node_obj - t_autoscaler:.1f}s.")

        if not logged_provision and t_karpenter and t_node_obj and t_node_obj >= t_karpenter:
            logged_provision = True
            log(job, f"Node provisioning time (karpenter->node object): {t_node_obj - t_karpenter:.1f}s.")

        if not logged_provision and t_node_obj and not t_autoscaler and not t_karpenter:
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
            f"pods={job.requested_pods}.",
        )

        log(job, "Node scaling is external; observing cluster-autoscaler and Karpenter.")

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
        finish_job(job)
        global active_job_id
        with jobs_lock:
            if active_job_id == job.job_id:
                active_job_id = None
        prune_completed_jobs()


def render_index() -> str:
    config_payload = {
        "targetNamespace": get_target()[0],
        "targetDeployment": get_target()[1],
        "nodeSelector": NODE_SELECTOR,
        "clientLimits": {
            "maxLogLines": CLIENT_MAX_LOG_LINES,
            "maxEventRowsPerRun": CLIENT_MAX_EVENT_ROWS_PER_RUN,
            "maxEventHistoryRuns": CLIENT_MAX_EVENT_HISTORY_RUNS,
            "maxStatusSamples": CLIENT_MAX_STATUS_SAMPLES,
        },
    }
    config_json = json.dumps(config_payload)
    template = app.state.index_template
    page = template.replace("__CONFIG_JSON__", config_json)
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
            "targetNamespace": get_target()[0],
            "targetDeployment": get_target()[1],
            "nodeSelector": NODE_SELECTOR,
            "clientLimits": {
                "maxLogLines": CLIENT_MAX_LOG_LINES,
                "maxEventRowsPerRun": CLIENT_MAX_EVENT_ROWS_PER_RUN,
                "maxEventHistoryRuns": CLIENT_MAX_EVENT_HISTORY_RUNS,
                "maxStatusSamples": CLIENT_MAX_STATUS_SAMPLES,
            },
        }
    )





@app.get("/api/namespaces")
def list_namespaces() -> JSONResponse:
    if not getattr(app.state, "k8s_enabled", False):
        return JSONResponse({"items": []})
    core: client.CoreV1Api = app.state.core
    items = [
        ns.metadata.name
        for ns in core.list_namespace().items
        if ns.metadata and ns.metadata.name
    ]
    items.sort()
    return JSONResponse({"items": items})


@app.get("/api/deployments")
def list_deployments(namespace: str | None = None) -> JSONResponse:
    if not getattr(app.state, "k8s_enabled", False):
        return JSONResponse({"items": []})
    apps: client.AppsV1Api = app.state.apps
    target_namespace = namespace or get_target()[0]
    items = []
    for dep in apps.list_namespaced_deployment(namespace=target_namespace).items:
        items.append({"namespace": target_namespace, "name": dep.metadata.name})
    items.sort(key=lambda x: x["name"])
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
async def stream(job_id: str, request: Request, since_id: int | None = None) -> StreamingResponse:
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    last_event_id = since_id
    if last_event_id is None:
        header_value = request.headers.get("last-event-id", "").strip()
        if header_value.isdigit():
            last_event_id = int(header_value)

    async def event_generator():
        with job.lock:
            if last_event_id is None:
                cursor = 0
            else:
                cursor = next((idx for idx, entry in enumerate(job.logs) if entry.seq > last_event_id), len(job.logs))

        yield f"retry: {STREAM_RETRY_MILLISECONDS}\n\n"
        last_emit = time.monotonic()
        while True:
            if await request.is_disconnected():
                break

            with job.lock:
                logs = job.logs[cursor:]
                done = job.done
            for entry in logs:
                yield f"id: {entry.seq}\ndata: {entry.line}\n\n"
                last_emit = time.monotonic()
            cursor += len(logs)
            if done and not logs:
                yield "event: done\ndata: done\n\n"
                break
            if not logs and time.monotonic() - last_emit >= STREAM_HEARTBEAT_INTERVAL:
                yield ": keep-alive\n\n"
                last_emit = time.monotonic()
            await asyncio.sleep(0.25)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
