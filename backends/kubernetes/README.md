# Maestro Kubernetes backend

A real `activities.Backend` implementation that drives the [Apache Flink Kubernetes
Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/)
(>= 1.15). It reconciles `FlinkDeployment` and `FlinkStateSnapshot` custom resources so
that the deterministic Maestro control-plane workflows operate **real** Flink jobs.

It is a separate Go module so the public Maestro core library keeps no `client-go` dependency.

## What each activity does

| Backend method | Kubernetes action |
|---|---|
| `ValidateDeployment` | Shared `activities.ValidateSpec` admission checks |
| `AcquireCapacityLease` / `ReleaseCapacityLease` | Durable per-node-pool reservation in a `ConfigMap` (multi-replica safe) |
| `ApplyDeployment` | Server-side apply of a `FlinkDeployment` (image, `flinkVersion: v2_2`, job, resources, upgradeMode) |
| `ObserveDeployment` | Reads CR status (informer cache) and maps it to a health summary, polling until settled |
| `TriggerSavepoint` / `ObserveSavepoint` | Creates a `FlinkStateSnapshot` and polls `status.path` |
| `SetJobState` | Patches `spec.job.state` (`running`/`suspended`) for resume/suspend |
| `RecordAudit` | Structured log + best-effort Kubernetes `Event` |

`ObserveDeployment` reads from a shared informer cache rather than hitting the API server
per call — this is what keeps the adapter viable at thousands of concurrent jobs.

## Build & run

```bash
# from the repository root (go.work ties the two modules together)
go build ./backends/kubernetes/...

# container image (build context = repo root)
docker build -f backends/kubernetes/Dockerfile -t maestro-k8s-worker .
```

### Configuration (environment)

| Variable | Default | Purpose |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal frontend (BYO / Temporal Cloud) |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_API_KEY` | _(unset)_ | Temporal Cloud API key (enables TLS) |
| `TEMPORAL_TLS` | `false` | Force TLS to the frontend |
| `ACTOR_TASK_QUEUE` | `flink-control-actors` | Actor + child workflows |
| `ACTIVITY_TASK_QUEUE` | `flink-control-activities` | External I/O activities |
| `FLINK_LEASE_NAMESPACE` | `maestro-system` | Namespace for capacity-lease ConfigMaps |
| `FLINK_SLOT_BUDGET` | `4096` | Max reserved task slots per node pool |
| `KUBE_CLIENT_QPS` / `KUBE_CLIENT_BURST` | `50` / `100` | API client rate limits |
| `WORKER_MAX_CONCURRENT_ACTIVITIES` | `200` | Activity concurrency per worker |
| `WORKER_MAX_CONCURRENT_WORKFLOW_TASKS` | `200` | Workflow-task concurrency per worker |
| `WORKER_ACTIVITIES_PER_SECOND` | _(unset)_ | Task-queue activity rate cap |

In-cluster the worker uses its ServiceAccount; locally it falls back to `KUBECONFIG`.
