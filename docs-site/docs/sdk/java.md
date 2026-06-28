---
sidebar_position: 3
title: Java SDK
---

# Java SDK

```xml
<dependency>
  <groupId>io.maestro</groupId>
  <artifactId>maestro-sdk</artifactId>
  <version>0.1.0</version>
</dependency>
```

Java 17+, zero external dependencies (uses `java.net.http.HttpClient`).

## Client Setup

```java
import io.maestro.sdk.*;

var client = new MaestroClient("https://maestro.yourcluster:8080", "your-bearer-token");
// or without auth:
var client = new MaestroClient("http://localhost:8080");
```

## Register and Deploy

```java
var orders = client.deployment("prod", "streaming", "orders");

// Register
orders.register("platform-team", "flink", "default");

// Deploy
var spec = DeploymentSpec.builder()
    .imageDigest("registry.example/orders@sha256:abc123")
    .flinkVersion("2.2")
    .parallelism(8)
    .maxParallelism(128)
    .resources(new ResourceShape(2.0, 4096, 2, 4))
    .stateCompatibility(StateCompatibility.safe())
    .jobArg("source.topic", "orders")
    .flinkConfig("state.backend.type", "rocksdb")
    .build();

orders.deploy(spec, "ci-pipeline", true, "release v2.3.1");

// Wait for healthy
var view = orders.waitHealthy(Duration.ofMinutes(5));
System.out.println("Version " + view.getString("currentVersion.versionId") + " is healthy");
```

## Scale

```java
orders.scale(16, "operator", true, "traffic spike");
```

## All Operations

```java
orders.savepoint("operator");
orders.suspend("operator", "maintenance window");
orders.resume("operator");
orders.rollback(3, "operator", true, "regression in v4");
orders.enableAutoscaler("operator");
orders.freezeAutoscaler("operator");
```

## Query State

```java
// Deployment status
var actor = orders.actor();
System.out.println(actor.getString("status"));
System.out.println(actor.getObject("currentVersion").getString("healthSummary.message"));

// Version history
var versions = orders.versions();

// All deployments
var list = client.listDeployments("prod", "streaming", 100);
var summary = client.summary();
```

## Cluster Operations

```java
client.clusterFreeze("prod", "streaming", "incident-commander", "active P0");
client.clusterUnfreeze("prod", "streaming", "incident-commander", "resolved");
```

## Custom Autoscaler

```java
import io.maestro.sdk.AutoscalerBase;

public class LagAutoscaler extends AutoscalerBase {
    public LagAutoscaler(MaestroClient client) {
        super(client, "prod", "streaming", "orders");
    }

    @Override
    protected Integer evaluate(MaestroResponse status) {
        var cv = status.getObject("currentVersion");
        long lag = cv.getObject("healthSummary").getLong("kafkaLag");
        int current = cv.getObject("spec").getInt("parallelism");

        if (lag > 100_000 && current < 32) {
            return current * 2;
        }
        if (lag < 10_000 && current > 4) {
            return current / 2;
        }
        return null; // no change
    }
}

// Run
var scaler = new LagAutoscaler(client);
while (true) {
    scaler.tick();
    Thread.sleep(60_000);
}
```
