# maestro-flink-sdk

Python SDK for the **Maestro Flink Control Plane** — deploy, scale, rollback, and observe Apache Flink jobs on any Kubernetes cluster.

## Install

```bash
pip install maestro-flink-sdk
# with Lambda extras:
pip install maestro-flink-sdk[lambda]
```

## Quick start

```python
from maestro_sdk import MaestroClient, DeploymentSpec, ResourceShape

client = MaestroClient("https://maestro.yourcluster.internal:8080", token="...")

# Register a deployment
client.register("prod", "streaming", "orders", owner="platform-team")

# Get a deployment handle
orders = client.deployment("prod", "streaming", "orders")

# Deploy a new version
orders.deploy(
    DeploymentSpec(
        image_digest="registry.example/orders@sha256:abc123",
        parallelism=8,
        max_parallelism=128,
        resources=ResourceShape(task_manager_cpu=2, task_manager_memory_mib=4096,
                                task_manager_count=2, slots_per_manager=4),
        flink_config={"state.backend.type": "rocksdb"},
    ),
    requester="ci-pipeline",
    approved=True,
)

# Wait for healthy
orders.wait_healthy(timeout=300)

# Check status
print(orders.status())
```

## Operations

```python
orders.scale(16, approved=True, reason="traffic spike")
orders.savepoint()
orders.suspend(reason="maintenance window")
orders.resume()
orders.rollback(target_version=3, reason="regression in v4")
orders.enable_autoscaler()
orders.freeze_autoscaler()
```

## Custom autoscaler

```python
from maestro_sdk import MaestroClient, AutoscalerBase, ScaleDecision

class MyAutoscaler(AutoscalerBase):
    def evaluate(self, status):
        lag = status["currentVersion"]["healthSummary"]["kafkaLag"]
        current = status["currentVersion"]["spec"]["parallelism"]
        if lag > 100_000 and current < 32:
            return ScaleDecision(current * 2, reason=f"lag={lag}")
        return None

client = MaestroClient("http://localhost:8080")
scaler = MyAutoscaler(client, "prod", "streaming", "orders")
scaler.run_loop(interval=60)
```

See [`examples/kafka_lag_autoscaler.py`](examples/kafka_lag_autoscaler.py) for an AWS Lambda-ready implementation.

## Cluster operations

```python
client.freeze_cluster("prod", "streaming", reason="incident")
client.unfreeze_cluster("prod", "streaming")
```
