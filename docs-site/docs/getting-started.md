---
sidebar_position: 2
title: Getting Started
---

# Getting Started

Deploy Maestro on your Kubernetes cluster and run your first Flink job in under 10 minutes.

## Prerequisites

- Kubernetes cluster (EKS, GKE, AKS, kind, or any conformant cluster)
- [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/) v1.15+
- Helm 3.x
- kubectl configured for your cluster

## 1. Install Maestro

```bash
helm repo add maestro https://maestro-flink.github.io/charts
helm install maestro maestro/maestro \
  --namespace maestro-system --create-namespace \
  --set temporal.enabled=true \
  --set temporal.web.enabled=true
```

This deploys:
- **Maestro API Server** — REST API + Operations Console on port 8080
- **Maestro Worker** — Temporal workflow and activity worker
- **Temporal Server** — durable workflow engine (bundled for trials; bring your own for production)
- **Temporal Web UI** — workflow visibility on port 8088

Verify:

```bash
kubectl get pods -n maestro-system
curl http://localhost:8080/healthz
# {"status":"ok"}
```

## 2. Register a Deployment

Tell Maestro about the Flink job you want to manage:

```bash
curl -X PUT http://localhost:8080/api/v1/deployments/prod/streaming/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "owner": "platform-team",
    "serviceAccount": "flink",
    "nodePool": "default"
  }'
```

Or with the Python SDK:

```python
from maestro_sdk import MaestroClient

client = MaestroClient("http://localhost:8080")
client.register("prod", "streaming", "orders", owner="platform-team")
```

## 3. Deploy Your Flink Job

```bash
curl -X POST http://localhost:8080/api/v1/deployments/prod/streaming/orders/deploy \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: deploy-001' \
  -d '{
    "requester": "ci-pipeline",
    "approved": true,
    "spec": {
      "imageDigest": "your-registry/orders-job@sha256:abc123",
      "flinkVersion": "2.2",
      "parallelism": 8,
      "maxParallelism": 128,
      "resources": {
        "taskManagerCpu": 2,
        "taskManagerMemoryMiB": 4096,
        "taskManagerCount": 2,
        "slotsPerManager": 4
      },
      "stateCompatibility": {
        "jobGraphCompatible": true,
        "operatorUidsStable": true
      }
    }
  }'
```

Maestro will:
1. Take a savepoint of the running job (if upgrading)
2. Apply the new FlinkDeployment spec via the Kubernetes Operator
3. Wait for the job to reach RUNNING state
4. Verify health gates (checkpoints, restarts, backpressure, Kafka lag)
5. Promote the version or rollback automatically on failure

## 4. Check Status

```bash
curl http://localhost:8080/api/v1/deployments/prod/streaming/orders/actor | jq
```

Response:

```json
{
  "identity": {"environment": "prod", "namespace": "streaming", "name": "orders"},
  "status": "IDLE",
  "currentVersion": {
    "versionId": 1,
    "spec": {...},
    "healthSummary": {
      "healthy": true,
      "running": true,
      "checkpointCompleted": true
    }
  },
  "recentOperations": [
    {"operationId": "deploy-001", "commandType": "DeployVersion", "status": "SUCCEEDED"}
  ]
}
```

## 5. Open the Operations Console

Navigate to `http://localhost:8080` to see the Maestro dashboard with deployment cards, version history, and operation logs.

## Next Steps

- [Scale your deployment](./api-reference#scale)
- [Set up custom autoscaling](./autoscaling/overview)
- [Deploy on EKS](./eks-deployment)
- [Compare Maestro vs AWS MSF](./comparison)
