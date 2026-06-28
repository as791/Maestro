---
sidebar_position: 1
title: Autoscaling Overview
---

# Custom Autoscaling

Maestro replaces the Flink Kubernetes Operator's built-in autoscaler with a pluggable SDK-based approach. The Operator autoscaler has known stability issues — rescaling storms, flapping under transient load, and opaque decision-making. Maestro gives you full control.

## Why Replace the Operator Autoscaler?

| Issue | Operator Autoscaler | Maestro SDK Autoscaler |
|---|---|---|
| **Stability** | Known rescaling storms under bursty load | You control cooldown and thresholds |
| **Metrics** | Limited to Flink JMX metrics | Any metric source (CloudWatch, Prometheus, Confluent, Datadog) |
| **Logic** | Fixed algorithm, limited tuning | Your code, your logic |
| **Observability** | Opaque scaling decisions | Full audit trail via Temporal |
| **Deployment** | Coupled to Operator version | Independent Lambda/CronJob/process |

## Architecture

```
Metric Source                Your Autoscaler              Maestro API
(CloudWatch / Prometheus)  →  (Lambda / CronJob)  →  POST .../scale
     ↓                            ↓                       ↓
  Kafka lag                  evaluate()              DeploymentActor
  TM CPU %                  ScaleDecision             → RolloutWorkflow
  Custom metric              parallelism=N              → FlinkDeployment
```

## SDK Autoscaler Interface

All three SDKs provide the same pattern:

1. **Subclass `AutoscalerBase`** — implement `evaluate(status)` 
2. **Return a scale decision** — target parallelism + reason, or `None`/`null` to hold
3. **Run** — as a Lambda (one-shot), CronJob, or long-running loop

The SDK handles:
- Fetching current deployment status
- Checking health before scaling (won't scale an unhealthy job)
- Skipping if already at target parallelism
- Idempotency key generation

## Recommended Strategies

### 1. Kafka Consumer Lag (Most Common)

Best for Kafka-source jobs. Scale based on `kafkaLag` from Maestro health summary, CloudWatch MSK metrics, or Confluent Cloud metrics API.

See: [Kafka Lag Autoscaler](./kafka-lag)

### 2. TaskManager CPU Utilization

Best for CPU-bound jobs. Scale based on Flink TaskManager CPU percentage from Prometheus or CloudWatch Container Insights.

See: [CPU-Based Autoscaler](./cpu-based)

### 3. Composite

Combine multiple signals — scale up on lag OR CPU, scale down only when BOTH are low.

```python
class CompositeAutoscaler(AutoscalerBase):
    def evaluate(self, status):
        health = status["currentVersion"]["healthSummary"]
        current = status["currentVersion"]["spec"]["parallelism"]
        
        lag = health.get("kafkaLag", 0)
        bp = health.get("backpressureRatio", 0)
        
        # Scale up if either signal is hot
        if lag > 100_000 or bp > 0.7:
            return ScaleDecision(min(current * 2, 64), reason=f"lag={lag} bp={bp:.0%}")
        
        # Scale down only if both are cold
        if lag < 10_000 and bp < 0.1 and current > 4:
            return ScaleDecision(max(current // 2, 4), reason="both signals cold")
        
        return None
```

## Deployment Options

| Option | Best For | Latency |
|---|---|---|
| **AWS Lambda + EventBridge** | EKS, serverless, low cost | 1-minute intervals |
| **Kubernetes CronJob** | Any K8s, no cloud deps | 1-minute intervals |
| **Long-running Pod** | Fastest reaction time | Configurable (10s+) |
| **CI/CD step** | Post-deploy scaling | On-demand |

See: [Lambda Deployment Guide](./lambda-deployment)
