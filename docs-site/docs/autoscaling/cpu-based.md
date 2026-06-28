---
sidebar_position: 3
title: CPU-Based Autoscaler
---

# CPU-Based Autoscaler

Scale Flink parallelism based on TaskManager CPU utilization — best for CPU-bound jobs where Kafka lag isn't the bottleneck.

## Strategy

```
if avg_cpu > 80%:
    parallelism = ceil(current * avg_cpu / target_cpu)  # scale up
elif avg_cpu < 30% and parallelism > min:
    parallelism = max(ceil(current * avg_cpu / target_cpu), min)  # scale down
```

## Using Prometheus (Flink Metrics)

Query Flink's TaskManager CPU metrics exposed via the Prometheus reporter.

```python
import math
import requests
from maestro_sdk import MaestroClient, AutoscalerBase, ScaleDecision

class CPUAutoscaler(AutoscalerBase):
    TARGET_CPU = 0.60       # 60% target utilization
    SCALE_UP_CPU = 0.80     # scale up above 80%
    SCALE_DOWN_CPU = 0.30   # scale down below 30%
    MIN_PARALLELISM = 2
    MAX_PARALLELISM = 64

    def __init__(self, client, env, ns, name, prometheus_url):
        super().__init__(client, env, ns, name)
        self.prometheus = prometheus_url

    def _get_avg_cpu(self) -> float:
        query = (
            'avg(rate(flink_taskmanager_Status_JVM_CPU_Load'
            '{job="flink-taskmanager"}[5m]))'
        )
        resp = requests.get(
            f"{self.prometheus}/api/v1/query",
            params={"query": query},
        )
        resp.raise_for_status()
        results = resp.json().get("data", {}).get("result", [])
        if not results:
            return 0.0
        return float(results[0]["value"][1])

    def evaluate(self, status):
        current = status["currentVersion"]["spec"]["parallelism"]
        avg_cpu = self._get_avg_cpu()

        if avg_cpu > self.SCALE_UP_CPU:
            target = min(
                math.ceil(current * avg_cpu / self.TARGET_CPU),
                self.MAX_PARALLELISM,
            )
            if target > current:
                return ScaleDecision(target, reason=f"cpu={avg_cpu:.0%}")

        if avg_cpu < self.SCALE_DOWN_CPU and current > self.MIN_PARALLELISM:
            target = max(
                math.ceil(current * avg_cpu / self.TARGET_CPU),
                self.MIN_PARALLELISM,
            )
            if target < current:
                return ScaleDecision(target, reason=f"cpu={avg_cpu:.0%} low")

        return None
```

## Using CloudWatch Container Insights (EKS)

For EKS clusters with Container Insights enabled, read TaskManager pod CPU from CloudWatch.

```python
import boto3
import math
from datetime import datetime, timedelta
from maestro_sdk import MaestroClient, AutoscalerBase, ScaleDecision

class EKSCPUAutoscaler(AutoscalerBase):
    def __init__(self, client, env, ns, name, cluster_name, flink_deployment):
        super().__init__(client, env, ns, name)
        self.cw = boto3.client("cloudwatch")
        self.cluster = cluster_name
        self.deployment = flink_deployment

    def _get_avg_cpu(self) -> float:
        resp = self.cw.get_metric_statistics(
            Namespace="ContainerInsights",
            MetricName="pod_cpu_utilization",
            Dimensions=[
                {"Name": "ClusterName", "Value": self.cluster},
                {"Name": "Namespace", "Value": "streaming"},
                {"Name": "PodName", "Value": f"{self.deployment}-taskmanager"},
            ],
            StartTime=datetime.utcnow() - timedelta(minutes=5),
            EndTime=datetime.utcnow(),
            Period=300,
            Statistics=["Average"],
        )
        points = resp.get("Datapoints", [])
        if not points:
            return 0.0
        return max(p["Average"] for p in points) / 100.0

    def evaluate(self, status):
        current = status["currentVersion"]["spec"]["parallelism"]
        cpu = self._get_avg_cpu()

        if cpu > 0.80:
            target = min(math.ceil(current * cpu / 0.60), 64)
            if target > current:
                return ScaleDecision(target, reason=f"eks_cpu={cpu:.0%}")

        if cpu < 0.30 and current > 2:
            target = max(math.ceil(current * cpu / 0.60), 2)
            if target < current:
                return ScaleDecision(target, reason=f"eks_cpu={cpu:.0%} low")

        return None
```

## Using Maestro Backpressure Ratio

Maestro's health summary includes `backpressureRatio` — a proxy for CPU saturation. No external metric source needed.

```python
from maestro_sdk import AutoscalerBase, ScaleDecision

class BackpressureAutoscaler(AutoscalerBase):
    def evaluate(self, status):
        health = status["currentVersion"]["healthSummary"]
        current = status["currentVersion"]["spec"]["parallelism"]
        bp = health.get("backpressureRatio", 0)

        if bp > 0.7 and current < 64:
            return ScaleDecision(min(current * 2, 64), reason=f"backpressure={bp:.0%}")

        if bp < 0.1 and current > 4:
            return ScaleDecision(max(current // 2, 4), reason="low backpressure")

        return None
```

## Tuning

| Parameter | Default | Description |
|---|---|---|
| `TARGET_CPU` | 60% | Desired utilization. Scale decisions aim for this level. |
| `SCALE_UP_CPU` | 80% | Trigger scale-up above this threshold |
| `SCALE_DOWN_CPU` | 30% | Trigger scale-down below this threshold |
| `MIN/MAX_PARALLELISM` | 2 / 64 | Bounds for scaling decisions |

**Tip:** The gap between TARGET_CPU and SCALE_UP_CPU provides headroom for traffic spikes without triggering a scale. A wider gap means fewer scaling events but less efficient resource usage.
