# Pertisk KOS Scaler

Rust Kubernetes worker-node autoscaler for clusters provisioned by `pertisk-mgmt`.

The controller reads unschedulable pending pods and requested CPU/memory from
the Kubernetes API. It adds workers through Pertisk's management API:

```text
Kubernetes scheduling pressure → kos-scaler → pertisk-mgmt → hypervisor worker VMs
```

This is node autoscaling. It complements, rather than replaces, Kubernetes HPA.

## Current behavior

- Enforces `workerPool.minSize` and `maxSize`.
- Scales up from unschedulable pending pods or requested CPU/memory utilization.
- Applies independent scale-up and scale-down cooldowns.
- Uses `POST /api/clusters/{clusterId}/nodes` to provision workers (batched ≤16).
- Scale-down cordons the candidate, then drains with the Eviction API so
  PodDisruptionBudgets are honored, waiting up to `drainTimeout` before removal.
- Recovers in-flight drains from node annotations and persisted state after restart.
- Uses `DELETE /api/clusters/{clusterId}/nodes/{nodeId}` to remove the drained worker.
- Pauses while Pertisk lifecycle jobs (`add_node` / `remove_node` / `upgrade_cluster`)
  are `queued` or `running`.

## Prerequisites

1. A reachable `pertisk-mgmt` API.
2. Management auth as an **admin** or **operator** account. Prefer a pre-issued JWT:

   ```bash
   export PERTISK_MGMT_TOKEN='...'   # or PERTISK_TOKEN
   ```

   Or log in at startup:

   ```bash
   export PERTISK_MGMT_USERNAME='operator'
   export PERTISK_MGMT_PASSWORD='...'
   ```

3. Kubernetes API access through in-cluster configuration or `KUBECONFIG`.
4. A one-to-one match between the Kubernetes node name and the Pertisk management
   node name. This is required to safely map a drained node to its management ID.

## Run locally

```bash
cp config.example.yaml config.yaml
# Edit mgmtEndpoint and clusterId.
export PERTISK_MGMT_TOKEN='...'
cargo run -- --config ./config.yaml --kubeconfig /path/to/kubeconfig
```

The dashboard and probes listen on `0.0.0.0:8080` by default:

- `GET /` operational dashboard
- `GET /api/status` JSON snapshot
- `GET /healthz` liveness
- `GET /readyz` readiness after Kubernetes and management auth succeed

## Deploy with Helm

```bash
helm upgrade --install kos-scaler ./helm/kos-scaler \
  --namespace kos-scaler --create-namespace \
  --set mgmt.endpoint=https://mgmt.example.com:8080 \
  --set mgmt.clusterId=<pertisk-cluster-uuid> \
  --set mgmt.token="$PERTISK_MGMT_TOKEN"
```

Or point at an existing secret:

```bash
helm upgrade --install kos-scaler ./helm/kos-scaler \
  --namespace kos-scaler --create-namespace \
  --set mgmt.endpoint=https://mgmt.example.com:8080 \
  --set mgmt.clusterId=<pertisk-cluster-uuid> \
  --set mgmt.existingSecret=pertisk-mgmt-token \
  --set mgmt.secretKey=token
```

The chart runs a single replica, mounts a PVC at `stateDir` for event history, and
exposes the dashboard on a ClusterIP Service. Ingress is opt-in.

RBAC grants node cordon/uncordon, pod list, and `pods/eviction` create so drains
respect PodDisruptionBudgets.

## Configuration

See [`config.example.yaml`](./config.example.yaml). Hardware fields under
`workerPool` are optional and map directly to Pertisk's `memory`, `cores`, and
`disk_gb` node-create overrides.
