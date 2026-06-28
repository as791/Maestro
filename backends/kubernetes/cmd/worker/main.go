// Command worker runs Maestro Temporal workers backed by the real Kubernetes/Flink
// Operator adapter. It is the production-shaped counterpart to the simulated
// ./cmd/worker in the core module.
package main

import (
	"context"
	"crypto/tls"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/maestro-flink/maestro"
	k8sbackend "github.com/maestro-flink/maestro/backends/kubernetes"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	temporalClient, err := dialTemporal()
	if err != nil {
		log.Fatalf("dial temporal: %v", err)
	}
	defer temporalClient.Close()

	restCfg, err := buildRestConfig()
	if err != nil {
		log.Fatalf("kubernetes config: %v", err)
	}
	restCfg.QPS = float32(floatEnv("KUBE_CLIENT_QPS", 50))
	restCfg.Burst = intEnv("KUBE_CLIENT_BURST", 100)

	backend, err := k8sbackend.New(restCfg, k8sbackend.Config{
		LeaseNamespace:   env("FLINK_LEASE_NAMESPACE", "fcp-system"),
		SlotBudget:       intEnv("FLINK_SLOT_BUDGET", 4096),
		WatchNamespaces:  splitNonEmpty(os.Getenv("FLINK_WATCH_NAMESPACES")),
		LocalStoragePath: os.Getenv("FLINK_LOCAL_STORAGE_PATH"),
	})
	if err != nil {
		log.Fatalf("build kubernetes backend: %v", err)
	}
	if err := backend.Start(ctx); err != nil {
		log.Fatalf("start informer cache: %v", err)
	}
	slog.Info("kubernetes backend informer cache synced")

	workerOptions := worker.Options{
		MaxConcurrentActivityExecutionSize:     intEnv("WORKER_MAX_CONCURRENT_ACTIVITIES", 200),
		MaxConcurrentWorkflowTaskExecutionSize: intEnv("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", 200),
	}
	if perSecond := floatEnv("WORKER_ACTIVITIES_PER_SECOND", 0); perSecond > 0 {
		workerOptions.TaskQueueActivitiesPerSecond = perSecond
	}

	activityWorker := worker.New(temporalClient, env("ACTIVITY_TASK_QUEUE", "flink-control-activities"), workerOptions)
	maestro.RegisterActivities(activityWorker, backend)
	if err := activityWorker.Start(); err != nil {
		log.Fatalf("start activity worker: %v", err)
	}
	defer activityWorker.Stop()

	// One actor worker per shard task queue.
	actorQueues := maestro.ActorTaskQueues(env("ACTOR_TASK_QUEUE", "flink-control-actors"), intEnv("ACTOR_TASK_QUEUE_SHARDS", 1))
	for _, queue := range actorQueues[1:] {
		actorWorker := worker.New(temporalClient, queue, workerOptions)
		maestro.RegisterWorkflows(actorWorker)
		if err := actorWorker.Start(); err != nil {
			log.Fatalf("start actor worker %s: %v", queue, err)
		}
		defer actorWorker.Stop()
	}

	primary := worker.New(temporalClient, actorQueues[0], workerOptions)
	maestro.RegisterWorkflows(primary)

	slog.Info("kubernetes-backed Maestro workers started",
		"actorTaskQueues", actorQueues,
		"activityTaskQueue", env("ACTIVITY_TASK_QUEUE", "flink-control-activities"),
	)
	if err := primary.Run(worker.InterruptCh()); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

// dialTemporal connects to an external (customer-owned) Temporal frontend or
// Temporal Cloud. TLS and API-key auth are enabled via environment.
func dialTemporal() (client.Client, error) {
	options := client.Options{
		HostPort:  env("TEMPORAL_ADDRESS", "localhost:7233"),
		Namespace: env("TEMPORAL_NAMESPACE", "default"),
	}
	if apiKey := os.Getenv("TEMPORAL_API_KEY"); apiKey != "" {
		options.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
	}
	if boolEnv("TEMPORAL_TLS", false) || os.Getenv("TEMPORAL_API_KEY") != "" {
		options.ConnectionOptions = client.ConnectionOptions{TLS: &tls.Config{MinVersion: tls.VersionTLS12}}
	}
	return client.Dial(options)
}

// buildRestConfig prefers in-cluster config and falls back to the local
// kubeconfig for development against a kind cluster.
func buildRestConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}

func floatEnv(key string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return fallback
}

func boolEnv(key string, fallback bool) bool {
	if v, err := strconv.ParseBool(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}

// splitNonEmpty parses a comma-separated env value into a trimmed, non-empty slice.
func splitNonEmpty(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
