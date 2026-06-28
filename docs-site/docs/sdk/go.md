---
sidebar_position: 2
title: Go SDK
---

# Go SDK

```bash
go get github.com/maestro-flink/maestro-sdk-go
```

Zero external dependencies — stdlib only.

## Client Setup

```go
package main

import "github.com/maestro-flink/maestro-sdk-go/maestro"

func main() {
    client := maestro.NewClient("https://maestro.yourcluster:8080",
        maestro.WithToken("your-bearer-token"),
    )
}
```

## Register and Deploy

```go
d := client.Deployment("prod", "streaming", "orders")

// Register
d.Register(ctx, maestro.RegisterRequest{
    Owner:          "platform-team",
    ServiceAccount: "flink",
    NodePool:       "default",
})

// Deploy
d.Deploy(ctx, maestro.DeployRequest{
    Requester: "ci-pipeline",
    Approved:  true,
    Reason:    "release v2.3.1",
    Spec: maestro.DeploymentSpec{
        ImageDigest:    "registry.example/orders@sha256:abc123",
        FlinkVersion:   "2.2",
        Parallelism:    8,
        MaxParallelism: 128,
        Resources: maestro.ResourceShape{
            TaskManagerCPU:    2,
            TaskManagerMemory: 4096,
            TaskManagerCount:  2,
            SlotsPerManager:   4,
        },
        JobArgs:     map[string]string{"source.topic": "orders"},
        FlinkConfig: map[string]string{"state.backend.type": "rocksdb"},
        StateCompatibility: maestro.StateCompat{
            JobGraphCompatible: true,
            OperatorUIDsStable: true,
        },
    },
})

// Wait for healthy
view, err := d.WaitHealthy(ctx, 5*time.Minute)
```

## Scale

```go
d.Scale(ctx, maestro.ScaleRequest{
    Parallelism: 16,
    Approved:    true,
    Reason:      "traffic spike",
})
```

## All Operations

```go
d.Savepoint(ctx, maestro.SimpleRequest{Requester: "operator"})
d.Suspend(ctx, maestro.SimpleRequest{Reason: "maintenance"})
d.Resume(ctx, maestro.SimpleRequest{Requester: "operator"})
d.Rollback(ctx, maestro.RollbackRequest{TargetVersion: 3, Approved: true})
d.EnableAutoscaler(ctx, maestro.SimpleRequest{})
d.FreezeAutoscaler(ctx, maestro.SimpleRequest{})
```

## Query State

```go
// Deployment status
view, _ := d.Status(ctx)
fmt.Println(view["status"])

// Version history
versions, _ := d.Versions(ctx)

// All deployments
list, _ := client.ListDeployments(ctx, maestro.ListOptions{Environment: "prod"})
cards, _ := client.Summary(ctx)
```

## Cluster Operations

```go
client.FreezeCluster(ctx, "prod", "streaming", maestro.SimpleRequest{
    Requester: "incident-commander",
    Reason:    "active P0",
})

client.UnfreezeCluster(ctx, "prod", "streaming", maestro.SimpleRequest{
    Requester: "incident-commander",
})
```

## Custom Autoscaler

```go
type lagScaler struct{}

func (l *lagScaler) Evaluate(status map[string]any) *int {
    cv := status["currentVersion"].(map[string]any)
    health := cv["healthSummary"].(map[string]any)
    lag := int(health["kafkaLag"].(float64))
    current := int(cv["spec"].(map[string]any)["parallelism"].(float64))

    if lag > 100000 && current < 32 {
        target := current * 2
        return &target
    }
    return nil
}

// Run in a loop
maestro.RunAutoscaler(ctx, client, "prod", "streaming", "orders",
    &lagScaler{}, 60*time.Second)
```
