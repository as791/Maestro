package workflows

import (
	"github.com/maestro-flink/maestro/activities"
	"github.com/maestro-flink/maestro/domain"
	"go.temporal.io/sdk/workflow"
)

type SavepointInput struct {
	Identity          domain.DeploymentIdentity `json:"identity"`
	Version           domain.DeploymentVersion  `json:"version"`
	ActivityTaskQueue string                    `json:"activityTaskQueue"`
}

func SavepointWorkflow(ctx workflow.Context, input SavepointInput) (domain.SavepointRecord, error) {
	ctx = withActivities(ctx, input.ActivityTaskQueue)

	var trigger activities.SavepointTrigger
	if err := workflow.ExecuteActivity(ctx, activities.NameTriggerSavepoint, activities.TriggerSavepointInput{
		Identity: input.Identity,
		Version:  input.Version,
	}).Get(ctx, &trigger); err != nil {
		return domain.SavepointRecord{}, err
	}

	var record domain.SavepointRecord
	if err := workflow.ExecuteActivity(ctx, activities.NameObserveSavepoint, activities.ObserveSavepointInput{
		Identity: input.Identity,
		Version:  input.Version,
		Trigger:  trigger,
	}).Get(ctx, &record); err != nil {
		return domain.SavepointRecord{}, err
	}
	return record, nil
}
