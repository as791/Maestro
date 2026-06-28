# Scaling Maestro to ~10,000 Flink jobs

Maestro models every Flink deployment as a long-lived Temporal *actor workflow*
(`flink-deployment/<env>/<ns>/<name>`) plus a per-namespace cluster actor. Operating
10,000 jobs therefore means ~10,000 continuously-open workflows whose commands are
serialized per actor. This document describes the bottlenecks at that scale and the knobs
this repository exposes to address them.

## Where the load actually is

| Layer | At 10k jobs | Mitigation in this repo |
|---|---|---|
| Kubernetes API | `ObserveDeployment` polling per rollout would dominate API traffic | Adapter reads from a **shared informer cache**, not per-call GETs (`backends/kubernetes/adapter.go`); client `QPS`/`Burst` + activity rate caps |
| Temporal task matching | One task queue funnels all actor tasks | **Task-queue sharding** (`ActorTaskQueues` / `ShardTaskQueue`, `ACTOR_TASK_QUEUE_SHARDS`) |
| Worker sticky cache | 10k open workflows thrash the workflow cache | Tunable worker concurrency + multiple worker replicas |
| Capacity leases | In-process maps are wrong across replicas | **Durable ConfigMap-backed lease store** (`backends/kubernetes/leases.go`) |
| Listing deployments | Per-workflow queries don't enumerate a fleet | Use Temporal **Advanced Visibility** + search attributes (below) |
| Control API | Stateless, but query fan-out is costly | Horizontally scale + HPA; list via visibility, not fan-out |
| Temporal persistence | History grows with command volume | History shard count + scaled persistence / Temporal Cloud; `continue-as-new` compaction (already implemented) |

## 1. Task-queue sharding

A single matching task queue is fine for hundreds of actors. Beyond that, spread actors
across shards:

```
ACTOR_TASK_QUEUE=flink-control-actors
ACTOR_TASK_QUEUE_SHARDS=16        # -> flink-control-actors-0 .. -15
```

- The control plane starts each actor on `ShardTaskQueue(base, shards, workflowID)` (stable
  FNV hash of the workflow ID), so an actor always lands on the same shard.
- Each worker process polls **all** shard queues (`ActorTaskQueues`), so sharding is about
  spreading *matching*, not pinning workers to shards.
- Signals and queries route by workflow ID and are unaffected — sharding is safe to raise
  for newly started actors on a running system.

Helm: `--set taskQueue.shards=16` wires both the control-api and worker tiers.

## 2. Worker tuning

Per-worker (env, surfaced in `deploy/helm/maestro` `worker.tuning.*`):

| Env | Default | Purpose |
|---|---|---|
| `WORKER_MAX_CONCURRENT_WORKFLOW_TASKS` | 200 | Workflow-task executors per worker |
| `WORKER_MAX_CONCURRENT_ACTIVITIES` | 200 | Activity executors per worker |
| `WORKER_ACTIVITIES_PER_SECOND` | unset | Per-queue activity rate cap (protects k8s API) |
| `KUBE_CLIENT_QPS` / `KUBE_CLIENT_BURST` | 50 / 100 | Adapter ↔ API-server rate limits |

Rule of thumb: run enough worker replicas that the **sum** of their workflow-cache capacity
comfortably exceeds the number of frequently-active actors, so hot actors stay sticky.
Idle actors evict cheaply and rehydrate on the next command.

## 3. Durable capacity leases

`AcquireCapacityLease`/`ReleaseCapacityLease` persist to a per-node-pool `ConfigMap`
(`maestro-leases-<pool>`) with optimistic concurrency. This keeps reservations consistent across
a horizontally-scaled worker tier — an in-process map (the simulated backend) cannot.
`FLINK_SLOT_BUDGET` caps reserved slots per pool.

## 4. Listing a fleet (Advanced Visibility)

Per-workflow queries describe one actor; they do not enumerate 10k. For dashboards/lists,
enable Temporal **Advanced Visibility** (Elasticsearch/OpenSearch, or Temporal Cloud) and
register search attributes (`env`, `namespace`, `owner`, `status`) at start, then back a
`GET /api/v1/deployments` list endpoint with `ListWorkflowExecutions` filtering on those
attributes instead of fanning out queries. (Visibility infra is an operator concern; this
repo's contracts are compatible with it.)

## 5. Temporal & persistence

- Size **history shards** for the target write rate (changing shard count requires a new
  namespace/cluster — pick generously up front, e.g. 512–4096 for large fleets).
- Use scaled persistence (Cassandra/managed SQL) or **Temporal Cloud** (`temporal.mode=external`,
  `temporal.tls=true`, `temporal.apiKey.*`).
- `continue-as-new` after `CONTINUE_AS_NEW_AFTER` commands keeps per-actor history bounded
  (already implemented in `workflows/deployment_actor.go`).

## 6. Autoscaling

Both tiers ship optional HPAs (`deploy/helm/maestro` `controlApi.autoscaling` /
`worker.autoscaling`). CPU works as a baseline; for the worker tier, scaling on Temporal
task-queue backlog (via the Temporal/SDK metrics + a custom metric) tracks demand more
directly.

## Validating the tuning

Run the simulated worker and drive a fleet with the load harness:

```bash
# terminal 1: Temporal + simulated worker (sharded)
docker compose up -d postgresql temporal
ACTOR_TASK_QUEUE_SHARDS=16 go run ./cmd/worker

# terminal 2: create 10k actors and deploy each
go run ./cmd/loadgen --count 10000 --concurrency 128 --shards 16
```

`loadgen` reports registration/deploy throughput and failures. Watch worker CPU, Temporal
task-queue backlog, and sticky-cache eviction rate while adjusting shards, replicas, and the
concurrency knobs above. Against the **real** Kubernetes backend, additionally watch
API-server request rate and raise `KUBE_CLIENT_*` / `WORKER_ACTIVITIES_PER_SECOND` to keep
it bounded.
