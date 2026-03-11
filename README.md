# Scaleout UI for Kubernetes

A lightweight web UI that scales a target Deployment and monitors worker nodes (via autoscaler or Karpenter), while streaming live timing logs for controller activity, node provisioning, node readiness, and pod readiness.

## What it does
- Accepts a desired pod replica count from the web UI.
- Lets you choose a namespace and deployment, then scopes deployment discovery to that namespace.
- Scales a Deployment to that replica count (pods). Worker nodes scale via cluster-autoscaler or Karpenter.
- Streams a live log with timestamps and timing metrics for:
  - Cluster-autoscaler and Karpenter scaling logs
  - Worker node objects appearing (provisioning)
  - Worker nodes reaching Ready
  - Pods reaching Ready
- Keeps the live SSE stream open with heartbeats and resumes after reconnect using event ids.
- Web UI tabs for: pod input, live events, timing summary, and a scaling graph export.

## Node scaling
The app does not directly scale nodes. Use OCI Kubernetes Autoscaler to add/remove nodes as pods scale.

## Environment variables
- `TARGET_NAMESPACE` (default: `default`)
- `TARGET_DEPLOYMENT` (default: `sample-app`)
- `NODE_SELECTOR` (default: empty) label selector for worker nodes, e.g. `node-role.kubernetes.io/worker=`
- `POLL_INTERVAL` (default: `5` seconds)
- `STATUS_INTERVAL` (default: `15` seconds)
- `TIMEOUT_SECONDS` (default: `3600`)
- `UI_ONLY_MODE` (default: `false`) set `true` to run the UI without a Kubernetes cluster.
- `STREAM_HEARTBEAT_INTERVAL` (default: `10` seconds) heartbeat interval for the live SSE log stream.
- `STREAM_RETRY_MILLISECONDS` (default: `2000`) reconnect hint sent to the browser SSE client.
- `CLIENT_MAX_LOG_LINES` (default: `1500`) max live log lines retained in the browser.
- `CLIENT_MAX_EVENT_ROWS_PER_RUN` (default: `500`) max event rows retained for the current run in the browser.
- `CLIENT_MAX_EVENT_HISTORY_RUNS` (default: `12`) max completed runs retained in browser history.
- `CLIENT_MAX_STATUS_SAMPLES` (default: `1500`) max chart samples retained in the browser.

## Build and run locally
```bash
docker build -t scaleout-ui:local .

docker run --rm -p 8000:8000 \
  -e TARGET_NAMESPACE=default \
  -e TARGET_DEPLOYMENT=sample-app \
  -e UI_ONLY_MODE=true \
  scaleout-ui:local
```

When running outside the cluster, the app uses your local kubeconfig. Inside a cluster it uses the in-cluster service account.

## Deploy to Kubernetes
1. Build and push the image.
2. Update `k8s/scaleout-ui.yaml` with your image and desired env values.
3. Apply the manifest:

```bash
kubectl apply -f k8s/scaleout-ui.yaml
```

Service is exposed via NodePort. Access the UI at:

```
http://<worker-node-ip>:30080
```

## Notes and limitations
- The app targets a single Deployment and scales its replicas to the desired pod count.
- For stricter distribution, consider setting resource requests and adding topology spread constraints to the target Deployment.
- To scale non-CAPI node pools, replace the node-scaler logic with your cloud provider API.
