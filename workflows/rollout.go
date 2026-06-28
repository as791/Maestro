package workflows

import (
	"fmt"
	"time"

	"github.com/maestro-flink/maestro/activities"
	"github.com/maestro-flink/maestro/domain"
	"go.temporal.io/sdk/workflow"
)

type RolloutInput struct {
	Identity          domain.DeploymentIdentity `json:"identity"`
	Current           *domain.DeploymentVersion `json:"current,omitempty"`
	Target            domain.DeploymentSpec     `json:"target"`
	Policy            domain.Policy             `json:"policy"`
	OperationID       string                    `json:"operationId"`
	Approved          bool                      `json:"approved"`
	Incident          bool                      `json:"incident"`
	ActivityTaskQueue string                    `json:"activityTaskQueue"`
}

type RolloutResult struct {
	Version    domain.DeploymentVersion `json:"version"`
	Savepoint  *domain.SavepointRecord  `json:"savepoint,omitempty"`
	LeaseID    string                   `json:"leaseId,omitempty"`
	RolledBack bool                     `json:"rolledBack"`
}

func RolloutWorkflow(ctx workflow.Context, input RolloutInput) (RolloutResult, error) {
	ctx = withActivities(ctx, input.ActivityTaskQueue)
	classification := domain.ClassifyChange(input.Current, input.Target, input.Policy)

	if err := workflow.ExecuteActivity(ctx, activities.NameValidateDeployment, activities.ValidateDeploymentInput{
		Identity:       input.Identity,
		CurrentVersion: input.Current,
		Target:         input.Target,
		Classification: classification,
		Approved:       input.Approved,
		Incident:       input.Incident,
		Policy:         input.Policy,
	}).Get(ctx, nil); err != nil {
		return RolloutResult{}, err
	}

	versionID := int64(1)
	if input.Current != nil {
		versionID = input.Current.VersionID + 1
	}
	version := domain.BuildVersion(versionID, input.Target)
	version.CreatedAt = workflow.Now(ctx)
	result := RolloutResult{Version: version}

	if classification.RequiresCapacityLease {
		var lease domain.Lease
		err := workflow.ExecuteActivity(ctx, activities.NameAcquireLease, activities.AcquireLeaseInput{
			Identity:      input.Identity,
			Resources:     input.Target.Resources,
			OwnerWorkflow: workflow.GetInfo(ctx).WorkflowExecution.ID,
			TTL:           30 * time.Minute,
		}).Get(ctx, &lease)
		if err != nil {
			return RolloutResult{}, err
		}
		result.LeaseID = lease.ID
		defer func() {
			_ = workflow.ExecuteActivity(ctx, activities.NameReleaseLease, lease.ID).Get(ctx, nil)
		}()
	}

	if classification.RequiresSavepoint && input.Current != nil {
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: "flink-savepoint/" + input.Identity.Environment + "/" + input.Identity.Namespace + "/" + input.Identity.Name + "/" + input.OperationID,
		})
		var savepoint domain.SavepointRecord
		if err := workflow.ExecuteChildWorkflow(childCtx, SavepointWorkflow, SavepointInput{
			Identity:          input.Identity,
			Version:           *input.Current,
			ActivityTaskQueue: input.ActivityTaskQueue,
		}).Get(childCtx, &savepoint); err != nil {
			return RolloutResult{}, err
		}
		result.Savepoint = &savepoint
		result.Version.SavepointURI = savepoint.URI
	}

	var applied activities.ApplyDeploymentResult
	if err := workflow.ExecuteActivity(ctx, activities.NameApplyDeployment, activities.ApplyDeploymentInput{
		Identity:     input.Identity,
		Version:      result.Version,
		SavepointURI: result.Version.SavepointURI,
		Previous:     input.Current,
		Incident:     input.Incident,
		GitOpsOnly:   input.Policy.GitOpsOnly,
	}).Get(ctx, &applied); err != nil {
		return RolloutResult{}, err
	}
	result.Version.OperatorObservedGeneration = applied.ObservedGeneration

	var health domain.HealthSummary
	if err := workflow.ExecuteActivity(ctx, activities.NameObserveDeployment, activities.ObserveDeploymentInput{
		Identity: input.Identity,
		Version:  result.Version,
	}).Get(ctx, &health); err != nil {
		return RolloutResult{}, err
	}
	result.Version.HealthSummary = health

	if err := domain.EvaluateHealth(health, input.Policy); err != nil {
		if input.Current == nil {
			return RolloutResult{}, fmt.Errorf("initial rollout failed health gate: %w", err)
		}
		rollback := *input.Current
		rollback.CreatedAt = workflow.Now(ctx)
		if rollback.SavepointURI == "" && result.Savepoint != nil {
			rollback.SavepointURI = result.Savepoint.URI
		}
		if applyErr := workflow.ExecuteActivity(ctx, activities.NameApplyDeployment, activities.ApplyDeploymentInput{
			Identity:     input.Identity,
			Version:      rollback,
			SavepointURI: rollback.SavepointURI,
			Previous:     &result.Version,
			Rollback:     true,
			Incident:     input.Incident,
			GitOpsOnly:   input.Policy.GitOpsOnly,
		}).Get(ctx, nil); applyErr != nil {
			return RolloutResult{}, fmt.Errorf("health gate failed (%v) and rollback failed: %w", err, applyErr)
		}
		result.RolledBack = true
		return result, fmt.Errorf("health gate failed and previous version was restored: %w", err)
	}
	result.Version.HealthSummary.Healthy = true
	return result, nil
}
