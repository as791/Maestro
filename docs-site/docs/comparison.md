---
sidebar_position: 3
title: Maestro vs AWS MSF
---

# Maestro vs AWS Managed Service for Apache Flink

Maestro is a drop-in replacement for AWS MSF (Managed Service for Apache Flink) and similar managed Flink services. You bring your own Kubernetes cluster; Maestro provides the management layer.

## Feature Comparison

| Feature | AWS MSF | Maestro |
|---|---|---|
| **Infrastructure** | AWS-managed, no cluster access | Any Kubernetes (EKS, GKE, AKS, on-prem) |
| **Flink Version** | Managed, often months behind | Any version — you control the image |
| **Deployment** | AWS Console / CloudFormation | REST API / SDK / GitOps / CI pipeline |
| **Rollback** | Manual redeploy from console | One-command with savepoint preservation |
| **Autoscaling** | Basic KPU-based | Custom SDK — Kafka lag, CPU, any metric |
| **State Management** | Opaque S3 buckets | You own checkpoint/savepoint storage |
| **Operation History** | CloudWatch logs | Durable Temporal workflows with full audit |
| **Cluster Freeze** | Not available | Namespace-level mutation freeze |
| **Health Gates** | Basic | Checkpoint, restart, backpressure, lag, sink |
| **Multi-Cloud** | AWS only | Any cloud or on-prem |
| **Cost** | Per-KPU pricing (~$0.11/KPU-hour) | Kubernetes node cost only |
| **License** | Proprietary | Apache 2.0 |
| **Vendor Lock-in** | High | None |

## Why Replace MSF?

### 1. Cost at Scale

MSF charges per KPU (Kinesis Processing Unit). A modest job with 8 KPUs costs ~$630/month. At 50 jobs, that's $31,500/month in MSF fees alone — before data transfer and storage.

With Maestro on EKS, the same workloads run on your existing nodes. Typical savings: **60-80%** at scale.

### 2. Flink Version Control

MSF locks you to specific Flink versions. When Flink 2.x shipped with major performance improvements, MSF users waited months. With Maestro, update your Docker image and deploy.

### 3. Custom Autoscaling

MSF's autoscaling is a black box. Maestro's [Autoscaler SDK](./autoscaling/overview) lets you build autoscalers that react to your actual metrics — Kafka consumer lag from CloudWatch or Confluent, TaskManager CPU, custom business metrics.

### 4. Operational Transparency

Every Maestro operation is a durable Temporal workflow. You can query any deployment's full history, see exactly what savepoint was used for a rollback, and audit who approved a production deploy. MSF gives you CloudWatch logs.

### 5. No Vendor Lock-in

Switching away from MSF means rewriting CloudFormation templates, migration tooling, and operational runbooks. Maestro runs on standard Kubernetes — move between EKS, GKE, and on-prem with the same Helm chart and API.

## Migration Guide

### From MSF to Maestro on EKS

1. **Set up Maestro** — See [EKS Deployment Guide](./eks-deployment)
2. **Containerize your Flink job** — Build a Docker image with your JAR
3. **Register with Maestro** — `PUT /api/v1/deployments/{env}/{ns}/{name}`
4. **Deploy** — `POST .../deploy` with your image digest
5. **Set up autoscaling** — Replace KPU scaling with [custom autoscaler](./autoscaling/overview)
6. **Decommission MSF** — Delete MSF application after verification

### Mapping MSF Concepts to Maestro

| MSF Concept | Maestro Equivalent |
|---|---|
| Application | Deployment (registered via PUT) |
| Snapshot | Savepoint (via POST .../savepoint) |
| Application update | Deploy (via POST .../deploy) |
| Scaling | Scale (via POST .../scale) |
| CloudWatch metrics | Health summary (via GET .../actor) |
| KPU autoscaling | Custom autoscaler SDK |
