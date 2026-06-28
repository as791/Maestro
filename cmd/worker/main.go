package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/maestro-flink/maestro"
	"github.com/maestro-flink/maestro/activities"
	"github.com/maestro-flink/maestro/internal/config"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	cfg := config.Load()
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer temporalClient.Close()

	simulated := activities.NewSimulated(cfg.Simulator.Delay)
	activityWorker := worker.New(temporalClient, cfg.Actor.ActivityTaskQueue, worker.Options{})
	maestro.RegisterActivities(activityWorker, simulated)
	if err := activityWorker.Start(); err != nil {
		log.Fatal(err)
	}
	defer activityWorker.Stop()

	actorQueues := maestro.ActorTaskQueues(cfg.Actor.TaskQueue, cfg.Actor.Shards)
	for _, queue := range actorQueues[1:] {
		actorWorker := worker.New(temporalClient, queue, worker.Options{})
		maestro.RegisterWorkflows(actorWorker)
		if err := actorWorker.Start(); err != nil {
			log.Fatal(err)
		}
		defer actorWorker.Stop()
	}

	primary := worker.New(temporalClient, actorQueues[0], worker.Options{})
	maestro.RegisterWorkflows(primary)

	slog.Info("Temporal workers started",
		"actorTaskQueues", actorQueues,
		"activityTaskQueue", cfg.Actor.ActivityTaskQueue,
	)
	if err := primary.Run(worker.InterruptCh()); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
