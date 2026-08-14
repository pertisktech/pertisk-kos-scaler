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
- Uses `POST /api/clusters/{clusterId}/nodes` to provision workers.
- Cordon-only scale-down: a worker is removed only after it has no non-DaemonSet
  running pods, which avoids forced pod eviction and PDB bypasses.
- Uses `DELETE /api/clusters/{clusterId}/nodes/{nodeId}` to remove the drained worker.

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
   node name. This is required to safely map a cordoned node to its management ID.

## Run locally

```bash
cp config.example.yaml config.yaml
# Edit mgmtEndpoint and clusterId.
export PERTISK_MGMT_TOKEN='...'
cargo run -- --config ./config.yaml --kubeconfig /path/to/kubeconfig
```

## Configuration

See [`config.example.yaml`](./config.example.yaml). Hardware fields under
`workerPool` are optional and map directly to Pertisk's `memory`, `cores`, and
`disk_gb` node-create overrides.

## Follow-up work

- Replace cordon-only scale-down with PDB-aware eviction and a timeout.
- Add Helm packaging, an event history, and operational dashboard from the Omni
  scaler template.
