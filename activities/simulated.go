package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/maestro-flink/maestro/domain"
	"go.temporal.io/sdk/activity"
)

type runtimeDeployment struct {
	Version    domain.DeploymentVersion
	State      string
	Generation int64
}

type SimulatedActivities struct {
	mu          sync.Mutex
	delay       time.Duration
	deployments map[string]runtimeDeployment
	leases      map[string]domain.Lease
}

var _ Backend = (*SimulatedActivities)(nil)

func NewSimulated(delay time.Duration) *SimulatedActivities {
	return &SimulatedActivities{
		delay:       delay,
		deployments: make(map[string]runtimeDeployment),
		leases:      make(map[string]domain.Lease),
	}
}

func (a *SimulatedActivities) ValidateDeployment(ctx context.Context, input ValidateDeploymentInput) error {
	if err := sleep(ctx, a.delay); err != nil {
		return err
	}
	return ValidateSpec(input)
}

func (a *SimulatedActivities) AcquireCapacityLease(ctx context.Context, input AcquireLeaseInput) (domain.Lease, error) {
	if err := sleep(ctx, a.delay); err != nil {
		return domain.Lease{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now().UTC()
	for id, lease := range a.leases {
		if lease.ExpiresAt.Before(now) {
			delete(a.leases, id)
		}
	}

	var reservedSlots int
	for _, lease := range a.leases {
		if lease.NodePool == input.Identity.NodePool {
			reservedSlots += lease.Slots
		}
	}
	const simulatedSlotBudget = 4096
	if reservedSlots+input.Resources.Slots() > simulatedSlotBudget {
		return domain.Lease{}, fmt.Errorf("nodepool %s slot budget exhausted", input.Identity.NodePool)
	}

	info := activity.GetInfo(ctx)
	id := fmt.Sprintf("lease-%s-%d", info.ActivityID, now.UnixNano())
	lease := domain.Lease{
		ID:            id,
		NodePool:      input.Identity.NodePool,
		CPU:           input.Resources.TaskManagerCPU * float64(input.Resources.TaskManagerCount),
		MemoryMiB:     input.Resources.TaskManagerMemory * int64(input.Resources.TaskManagerCount),
		Slots:         input.Resources.Slots(),
		OwnerWorkflow: input.OwnerWorkflow,
		ExpiresAt:     now.Add(input.TTL),
	}
	a.leases[id] = lease
	return lease, nil
}

func (a *SimulatedActivities) ReleaseCapacityLease(ctx context.Context, leaseID string) error {
	if err := sleep(ctx, a.delay); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.leases, leaseID)
	return nil
}

func (a *SimulatedActivities) ApplyDeployment(ctx context.Context, input ApplyDeploymentInput) (ApplyDeploymentResult, error) {
	if err := sleep(ctx, a.delay); err != nil {
		return ApplyDeploymentResult{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	key := input.Identity.WorkflowID()
	current := a.deployments[key]
	generation := current.Generation + 1
	state := "RUNNING"
	a.deployments[key] = runtimeDeployment{
		Version:    input.Version,
		State:      state,
		Generation: generation,
	}
	mode := "gitops"
	if input.Incident && !input.GitOpsOnly {
		mode = "direct-patch"
	}
	return ApplyDeploymentResult{
		ObservedGeneration: generation,
		ApplyReference:     fmt.Sprintf("%s://%s/%d", mode, input.Identity.Name, input.Version.VersionID),
	}, nil
}

func (a *SimulatedActivities) ObserveDeployment(ctx context.Context, input ObserveDeploymentInput) (domain.HealthSummary, error) {
	if err := sleep(ctx, a.delay); err != nil {
		return domain.HealthSummary{}, err
	}
	a.mu.Lock()
	runtime, ok := a.deployments[input.Identity.WorkflowID()]
	a.mu.Unlock()
	if !ok {
		return domain.HealthSummary{}, errors.New("deployment not found in runtime")
	}

	failing := strings.Contains(strings.ToLower(input.Version.Spec.ImageDigest), "deadbeef")
	summary := domain.HealthSummary{
		Healthy:             !failing && runtime.State == "RUNNING",
		Running:             runtime.State == "RUNNING",
		CheckpointCompleted: !failing,
		RestartCount:        0,
		BackpressureRatio:   0.05,
		KafkaLag:            10,
		SinkHealthy:         !failing,
		ObservedAt:          time.Now().UTC(),
	}
	if failing {
		summary.Message = "simulated health failure for image digest containing deadbeef"
	}
	return summary, nil
}

func (a *SimulatedActivities) TriggerSavepoint(ctx context.Context, input TriggerSavepointInput) (SavepointTrigger, error) {
	if err := sleep(ctx, a.delay); err != nil {
		return SavepointTrigger{}, err
	}
	info := activity.GetInfo(ctx)
	return SavepointTrigger{
		TriggerID:  "trigger-" + info.ActivityID,
		FlinkJobID: "job-" + input.Identity.Name,
	}, nil
}

func (a *SimulatedActivities) ObserveSavepoint(ctx context.Context, input ObserveSavepointInput) (domain.SavepointRecord, error) {
	if err := sleep(ctx, a.delay); err != nil {
		return domain.SavepointRecord{}, err
	}
	return domain.SavepointRecord{
		URI:               fmt.Sprintf("s3://flink-savepoints/%s/%s/%s", input.Identity.Environment, input.Identity.Name, input.Trigger.TriggerID),
		TriggerID:         input.Trigger.TriggerID,
		FlinkJobID:        input.Trigger.FlinkJobID,
		DeploymentVersion: input.Version.VersionID,
		ImageDigest:       input.Version.Spec.ImageDigest,
		JobArgsHash:       input.Version.JobArgsHash,
		Parallelism:       input.Version.Spec.Parallelism,
		MaxParallelism:    input.Version.Spec.MaxParallelism,
		CreatedAt:         time.Now().UTC(),
	}, nil
}

func (a *SimulatedActivities) SetJobState(ctx context.Context, input SetJobStateInput) error {
	if err := sleep(ctx, a.delay); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := input.Identity.WorkflowID()
	runtime := a.deployments[key]
	runtime.State = input.State
	a.deployments[key] = runtime
	return nil
}

func (a *SimulatedActivities) RecordAudit(ctx context.Context, event AuditEvent) error {
	slog.InfoContext(ctx, "control-plane audit", "operationId", event.OperationID, "type", event.Type, "message", event.Message)
	return nil
}

func sleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
