# Scaleout UI for Kubernetes

A lightweight web UI that scales a target Deployment and monitors worker nodes (via autoscaler), while streaming live timing logs for node provisioning, node readiness, and pod readiness.

## What it does
- Accepts a desired pod replica count from the web UI.
- Scales a Deployment to that replica count (pods). Worker nodes scale via autoscaler.
- Streams a live log with timestamps and timing metrics for:
  - Worker node objects appearing (provisioning)
  - Worker nodes reaching Ready
  - Pods reaching Ready
- Web UI tabs for: pod input, live events, timing summary, and a scaling graph export.

> Note: The app caps `MAX_PODS_PER_NODE` at 20 to honor the “no more than 20 pods per worker node” requirement.

## Node scaling
The app does not directly scale nodes. Use OCI Kubernetes Autoscaler to add/remove nodes as pods scale.

## Environment variables
- `TARGET_NAMESPACE` (default: `default`)
- `TARGET_DEPLOYMENT` (default: `sample-app`)
- `MAX_PODS_PER_NODE` (default: `20`, maximum enforced)
- `NODE_SELECTOR` (default: empty) label selector for worker nodes, e.g. `node-role.kubernetes.io/worker=`
- `POLL_INTERVAL` (default: `5` seconds)
- `STATUS_INTERVAL` (default: `15` seconds)
- `TIMEOUT_SECONDS` (default: `3600`)

## Build and run locally
```bash
docker build -t scaleout-ui:local .

docker run --rm -p 8000:8000 \
  -e TARGET_NAMESPACE=default \
  -e TARGET_DEPLOYMENT=sample-app \
  -e MAX_PODS_PER_NODE=20 \
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

Then port-forward or expose the service to access the UI:
```bash
kubectl port-forward svc/scaleout-ui 8080:80
```

Open `http://localhost:8080` in your browser.

## Notes and limitations
- The app targets a single Deployment and scales its replicas to the desired pod count.
- `MAX_PODS_PER_NODE` is used to estimate required nodes for timing; Kubernetes scheduling may still place pods unevenly if other workloads are present.
- For stricter distribution, consider setting resource requests and adding topology spread constraints to the target Deployment.
- To scale non-CAPI node pools, replace the node-scaler logic with your cloud provider API.
