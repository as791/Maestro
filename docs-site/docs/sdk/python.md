---
sidebar_position: 1
title: Python SDK
---

# Python SDK

```bash
pip install maestro-flink-sdk
```

## Client Setup

```python
from maestro_sdk import MaestroClient

client = MaestroClient(
    base_url="https://maestro.yourcluster:8080",
    token="your-bearer-token",  # optional
)
```

## Register a Deployment

```python
client.register("prod", "streaming", "orders",
    owner="platform-team",
    service_account="flink",
    node_pool="default",
)
```

## Deploy

```python
from maestro_sdk import DeploymentSpec, ResourceShape, StateCompatibility

orders = client.deployment("prod", "streaming", "orders")

orders.deploy(
    DeploymentSpec(
        image_digest="registry.example/orders@sha256:abc123",
        parallelism=8,
        max_parallelism=128,
        flink_version="2.2",
        resources=ResourceShape(
            task_manager_cpu=2,
            task_manager_memory_mib=4096,
            task_manager_count=2,
            slots_per_manager=4,
        ),
        job_args={"source.topic": "orders", "sink.table": "enriched_orders"},
        flink_config={"state.backend.type": "rocksdb"},
        state_compatibility=StateCompatibility(
            job_graph_compatible=True,
            operator_uids_stable=True,
        ),
    ),
    requester="ci-pipeline",
    approved=True,
    reason="release v2.3.1",
)

# Wait for healthy
view = orders.wait_healthy(timeout=300)
print(f"Version {view['currentVersion']['versionId']} is healthy")
```

## Scale

```python
orders.scale(16, approved=True, reason="traffic spike")
orders.wait_healthy()
```

## All Operations

```python
# Savepoint
orders.savepoint()

# Suspend (takes savepoint, stops job)
orders.suspend(reason="maintenance window")

# Resume
orders.resume()

# Rollback to specific version
orders.rollback(target_version=3, reason="regression in v4")

# Rollback to last healthy
orders.rollback(reason="emergency")

# Autoscaler
orders.enable_autoscaler()
orders.freeze_autoscaler()
```

## Query State

```python
# Deployment status
status = orders.status()
print(status["status"])                    # "IDLE"
print(status["currentVersion"]["versionId"])  # 5
print(status["lastError"])                    # ""

# Version history
for v in orders.versions():
    health = v["healthSummary"]
    print(f"v{v['versionId']}: healthy={health['healthy']} lag={health['kafkaLag']}")

# All deployments
cards = client.summary()
deployments = client.list_deployments(environment="prod", namespace="streaming")
```

## Cluster Operations

```python
# Freeze during incident
client.freeze_cluster("prod", "streaming",
    requester="incident-commander",
    reason="active P0 — no deploys",
)

# Unfreeze
client.unfreeze_cluster("prod", "streaming",
    requester="incident-commander",
    reason="incident resolved",
)
```

## Custom Autoscaler

See [Autoscaling Overview](../autoscaling/overview) for building custom autoscalers with the SDK.

```python
from maestro_sdk import AutoscalerBase, ScaleDecision

class MyAutoscaler(AutoscalerBase):
    def evaluate(self, status: dict) -> ScaleDecision | None:
        lag = status["currentVersion"]["healthSummary"]["kafkaLag"]
        current = status["currentVersion"]["spec"]["parallelism"]
        if lag > 100_000 and current < 32:
            return ScaleDecision(current * 2, reason=f"lag={lag}")
        return None

scaler = MyAutoscaler(client, "prod", "streaming", "orders")
scaler.run_loop(interval=60)  # or scaler.execute() for one-shot (Lambda)
```
