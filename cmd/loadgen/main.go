// Command loadgen exercises the Maestro control plane at fleet scale. It registers N
// deployment actors and optionally drives a deploy on each, reporting throughput
// and error counts. Point it at a control plane running the SIMULATED worker
// (./cmd/worker) to validate Temporal/worker tuning for ~10k actors without a
// real Kubernetes cluster. See docs/SCALING.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maestro-flink/maestro/control"
	"github.com/maestro-flink/maestro/domain"
	"go.temporal.io/sdk/client"
)

func main() {
	var (
		count       = flag.Int("count", 10000, "number of deployment actors to create")
		concurrency = flag.Int("concurrency", 64, "concurrent requests")
		address     = flag.String("temporal", "localhost:7233", "Temporal frontend address")
		namespace   = flag.String("namespace", "default", "Temporal namespace")
		actorQueue  = flag.String("actor-queue", "flink-control-actors", "actor task queue base")
		actQueue    = flag.String("activity-queue", "flink-control-activities", "activity task queue")
		shards      = flag.Int("shards", 1, "actor task queue shards")
		environment = flag.String("env", "loadtest", "deployment environment")
		nsBuckets   = flag.Int("ns-buckets", 50, "spread actors across this many namespaces")
		doDeploy    = flag.Bool("deploy", true, "also send a deploy command to each actor")
	)
	flag.Parse()

	temporalClient, err := client.Dial(client.Options{HostPort: *address, Namespace: *namespace})
	if err != nil {
		log.Fatalf("dial temporal: %v", err)
	}
	defer temporalClient.Close()

	svc := control.NewService(temporalClient, *actorQueue, *actQueue, 500, *shards)

	var (
		registered atomic.Int64
		deployed   atomic.Int64
		failures   atomic.Int64
	)
	jobs := make(chan int, *concurrency*2)
	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				identity := domain.DeploymentIdentity{
					Environment: *environment,
					Namespace:   fmt.Sprintf("ns-%03d", i%*nsBuckets),
					Name:        fmt.Sprintf("job-%06d", i),
					NodePool:    "default",
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := svc.EnsureDeploymentActor(ctx, identity, nil); err != nil {
					failures.Add(1)
					cancel()
					continue
				}
				registered.Add(1)
				if *doDeploy {
					if err := svc.SendCommand(ctx, identity, deployCommand(i)); err != nil {
						failures.Add(1)
					} else {
						deployed.Add(1)
					}
				}
				cancel()
			}
		}()
	}

	go progress(start, &registered, &deployed, &failures, *count)
	for i := 0; i < *count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("\n--- loadgen complete ---\n")
	fmt.Printf("registered: %d\n", registered.Load())
	fmt.Printf("deployed:   %d\n", deployed.Load())
	fmt.Printf("failures:   %d\n", failures.Load())
	fmt.Printf("elapsed:    %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f actors/sec\n", float64(registered.Load())/elapsed.Seconds())
}

func deployCommand(i int) domain.DeploymentCommand {
	key := fmt.Sprintf("loadgen-deploy-%06d", i)
	spec := domain.DeploymentSpec{
		ImageDigest:    fmt.Sprintf("registry.local/loadgen@sha256:%064x", i),
		FlinkVersion:   "2.2",
		Parallelism:    1,
		MaxParallelism: 4,
		Resources: domain.ResourceShape{
			TaskManagerCPU:    1,
			TaskManagerMemory: 1024,
			TaskManagerCount:  1,
			SlotsPerManager:   1,
		},
		State: domain.StateCompatibility{JobGraphCompatible: true, OperatorUIDsStable: true},
	}
	return domain.DeploymentCommand{
		OperationID:    key,
		IdempotencyKey: key,
		Type:           domain.CommandDeployVersion,
		Requester:      "loadgen",
		Approved:       true,
		TargetSpec:     &spec,
		RequestedAt:    time.Now().UTC(),
	}
}

func progress(start time.Time, registered, deployed, failures *atomic.Int64, total int) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		r := registered.Load()
		fmt.Printf("\r[%6.1fs] registered=%d deployed=%d failures=%d (%.0f/sec)   ",
			time.Since(start).Seconds(), r, deployed.Load(), failures.Load(),
			float64(r)/time.Since(start).Seconds())
		if int(r)+int(failures.Load()) >= total {
			return
		}
	}
}
