// Package maestro provides registration helpers for embedding the Flink Control
// Plane workflows and activities in a Temporal worker.
package maestro

import (
	"github.com/maestro-flink/maestro/activities"
	"github.com/maestro-flink/maestro/workflows"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
)

// RegisterWorkflows installs all stable Maestro workflow entry points.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(workflows.ClusterActorWorkflow)
	w.RegisterWorkflow(workflows.DeploymentActorWorkflow)
	w.RegisterWorkflow(workflows.RolloutWorkflow)
	w.RegisterWorkflow(workflows.SavepointWorkflow)
}

// RegisterActivities installs a caller-provided integration backend using
// stable activity names. The backend may be community-built or proprietary.
func RegisterActivities(w worker.Worker, backend activities.Backend) {
	w.RegisterActivityWithOptions(backend.ValidateDeployment, activity.RegisterOptions{Name: activities.NameValidateDeployment})
	w.RegisterActivityWithOptions(backend.AcquireCapacityLease, activity.RegisterOptions{Name: activities.NameAcquireLease})
	w.RegisterActivityWithOptions(backend.ReleaseCapacityLease, activity.RegisterOptions{Name: activities.NameReleaseLease})
	w.RegisterActivityWithOptions(backend.ApplyDeployment, activity.RegisterOptions{Name: activities.NameApplyDeployment})
	w.RegisterActivityWithOptions(backend.ObserveDeployment, activity.RegisterOptions{Name: activities.NameObserveDeployment})
	w.RegisterActivityWithOptions(backend.TriggerSavepoint, activity.RegisterOptions{Name: activities.NameTriggerSavepoint})
	w.RegisterActivityWithOptions(backend.ObserveSavepoint, activity.RegisterOptions{Name: activities.NameObserveSavepoint})
	w.RegisterActivityWithOptions(backend.SetJobState, activity.RegisterOptions{Name: activities.NameSetJobState})
	w.RegisterActivityWithOptions(backend.RecordAudit, activity.RegisterOptions{Name: activities.NameRecordAudit})
}
