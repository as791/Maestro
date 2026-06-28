---
sidebar_position: 2
title: Kafka Lag Autoscaler
---

# Kafka Lag Autoscaler

Scale Flink parallelism based on Kafka consumer group lag — the most common autoscaling strategy for streaming jobs.

## Strategy

```
if lag > threshold:
    parallelism = ceil(lag / lag_per_slot)   # scale up proportionally
elif lag < low_threshold and parallelism > min:
    parallelism = max(parallelism / 2, min)  # scale down conservatively
```

## Using Maestro Health Summary

Maestro's health summary includes `kafkaLag` when the Flink job reports it. Simplest approach — no external metric source needed.

```python
import math
from maestro_sdk import MaestroClient, AutoscalerBase, ScaleDecision

class KafkaLagAutoscaler(AutoscalerBase):
    MIN_PARALLELISM = 2
    MAX_PARALLELISM = 64
    LAG_PER_SLOT = 50_000       # target lag per parallelism unit
    SCALE_DOWN_LAG = 10_000     # lag below which we consider scaling down

    def evaluate(self, status):
        health = status["currentVersion"]["healthSummary"]
        current = status["currentVersion"]["spec"]["parallelism"]
        lag = health.get("kafkaLag", 0)

        if lag > self.LAG_PER_SLOT:
            target = min(math.ceil(lag / self.LAG_PER_SLOT), self.MAX_PARALLELISM)
            if target > current:
                return ScaleDecision(target, reason=f"lag={lag:,}")

        if lag < self.SCALE_DOWN_LAG and current > self.MIN_PARALLELISM:
            target = max(current // 2, self.MIN_PARALLELISM)
            if target < current:
                return ScaleDecision(target, reason=f"lag={lag:,} low")

        return None
```

## Using AWS CloudWatch (MSK)

For Amazon MSK, read the `SumOffsetLag` metric from CloudWatch for more accurate lag data.

```python
import boto3
import math
from datetime import datetime, timedelta
from maestro_sdk import MaestroClient, AutoscalerBase, ScaleDecision

class MSKLagAutoscaler(AutoscalerBase):
    def __init__(self, client, env, ns, name, cluster_name, consumer_group, topic):
        super().__init__(client, env, ns, name)
        self.cw = boto3.client("cloudwatch")
        self.cluster_name = cluster_name
        self.consumer_group = consumer_group
        self.topic = topic

    def _get_lag(self) -> int:
        response = self.cw.get_metric_statistics(
            Namespace="AWS/Kafka",
            MetricName="SumOffsetLag",
            Dimensions=[
                {"Name": "Cluster Name", "Value": self.cluster_name},
                {"Name": "Consumer Group", "Value": self.consumer_group},
                {"Name": "Topic", "Value": self.topic},
            ],
            StartTime=datetime.utcnow() - timedelta(minutes=5),
            EndTime=datetime.utcnow(),
            Period=60,
            Statistics=["Maximum"],
        )
        points = response.get("Datapoints", [])
        if not points:
            return 0
        return int(max(p["Maximum"] for p in points))

    def evaluate(self, status):
        current = status["currentVersion"]["spec"]["parallelism"]
        lag = self._get_lag()

        lag_per_slot = 50_000
        if lag > lag_per_slot:
            target = min(math.ceil(lag / lag_per_slot), 64)
            if target > current:
                return ScaleDecision(target, reason=f"msk_lag={lag:,}")

        if lag < 10_000 and current > 2:
            return ScaleDecision(max(current // 2, 2), reason=f"msk_lag={lag:,} low")

        return None
```

## Using Confluent Cloud Metrics API

For Confluent Cloud, use the Metrics API to get consumer lag.

```python
import requests
import math
from maestro_sdk import MaestroClient, AutoscalerBase, ScaleDecision

class ConfluentLagAutoscaler(AutoscalerBase):
    def __init__(self, client, env, ns, name, api_key, api_secret, cluster_id, consumer_group):
        super().__init__(client, env, ns, name)
        self.session = requests.Session()
        self.session.auth = (api_key, api_secret)
        self.cluster_id = cluster_id
        self.consumer_group = consumer_group

    def _get_lag(self) -> int:
        resp = self.session.post(
            "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query",
            json={
                "aggregations": [{"metric": "io.confluent.kafka.server/consumer_lag_offsets", "agg": "SUM"}],
                "filter": {
                    "op": "AND",
                    "filters": [
                        {"field": "resource.kafka.id", "op": "EQ", "value": self.cluster_id},
                        {"field": "metric.consumer_group_id", "op": "EQ", "value": self.consumer_group},
                    ],
                },
                "granularity": "PT1M",
                "intervals": ["last-5-minutes"],
            },
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            return 0
        return int(max(d["value"] for d in data))

    def evaluate(self, status):
        current = status["currentVersion"]["spec"]["parallelism"]
        lag = self._get_lag()

        if lag > 50_000:
            target = min(math.ceil(lag / 50_000), 64)
            if target > current:
                return ScaleDecision(target, reason=f"confluent_lag={lag:,}")

        if lag < 10_000 and current > 2:
            return ScaleDecision(max(current // 2, 2), reason="confluent lag low")

        return None
```

## Tuning Parameters

| Parameter | Default | Description |
|---|---|---|
| `LAG_PER_SLOT` | 50,000 | Target lag per parallelism unit. Lower = more aggressive scaling. |
| `MIN_PARALLELISM` | 2 | Floor — never scale below this |
| `MAX_PARALLELISM` | 64 | Ceiling — never scale above this |
| `SCALE_DOWN_LAG` | 10,000 | Only scale down when lag is below this |
| `COOLDOWN` | 300s | Minimum time between scale operations |

**Tip:** Start with `LAG_PER_SLOT = 50000` and adjust based on your job's processing rate. If your job processes 10,000 records/second per slot, a lag of 50,000 means ~5 seconds of catch-up — a reasonable target.
