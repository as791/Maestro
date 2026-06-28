---
sidebar_position: 1
slug: /
title: Introduction
---

# Maestro — Flink Control Plane

**Open-source deployment management for Apache Flink on any Kubernetes cluster.**

Maestro is an Apache 2.0-licensed control plane that manages the full lifecycle of stateful Apache Flink deployments. It replaces proprietary managed services like AWS Managed Service for Apache Flink (MSF) with an open, portable solution that runs on any Kubernetes cluster — EKS, GKE, AKS, or on-prem.

## Why Maestro?

| Pain Point | Managed Services | Maestro |
|---|---|---|
| **Vendor lock-in** | Locked to one cloud | Any Kubernetes cluster |
| **Flink version lag** | Months behind OSS | Day-one support for Flink 2.x |
| **Opaque operations** | Black-box deploys | Full operation history via Temporal |
| **Limited autoscaling** | Basic KPU scaling | Custom autoscaler SDK |
| **Cost** | Per-KPU pricing | Just your Kubernetes nodes |
| **State ownership** | Provider-managed S3 | You own checkpoints and savepoints |

## Key Features

- **Controlled Rollouts** — Savepoint-gated deployments with automatic health checks and rollback on failure
- **Custom Autoscaling** — Replace the Flink Operator autoscaler with your own logic using the SDK (Kafka lag, CPU, custom metrics)
- **Multi-language SDKs** — Python, Go, and Java clients for programmatic control
- **Durable Operation History** — Every deploy, scale, rollback, and savepoint tracked in Temporal workflows
- **Cluster Freeze** — Namespace-level mutation freeze during incidents
- **GitOps Ready** — API-driven, idempotent operations with Idempotency-Key headers

## Architecture at a Glance

Maestro uses the **actor model** implemented via long-running Temporal workflows — the same pattern [used by Netflix to orchestrate 12,000+ Flink clusters](https://temporal.io/resources/on-demand/actor-workflows-reliably-orchestrating-thousands-of-flink-clusters-at).

```
SDK / CLI / CI  →  Maestro API Server  →  Temporal Server
                                              ↓
                                        DeploymentActor (long-running)
                                          ↙          ↘
                                   RolloutWorkflow  SavepointWorkflow
                                          ↓
                                      Activities  →  Flink Kubernetes Operator  →  Flink Jobs
```

Each Flink deployment gets a dedicated **DeploymentActor** workflow that serializes all operations, maintains version history, and orchestrates child workflows for rollouts and savepoints.

## Quick Install

```bash
helm repo add maestro https://maestro-flink.github.io/charts
helm install maestro maestro/maestro \
  --namespace maestro-system --create-namespace \
  --set temporal.enabled=true
```

Then register and deploy your first Flink job — see [Getting Started](./getting-started).
