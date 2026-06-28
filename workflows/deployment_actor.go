package workflows

import (
	"fmt"

	"github.com/maestro-flink/maestro/activities"
	"github.com/maestro-flink/maestro/domain"
	"go.temporal.io/sdk/workflow"
)

type DeploymentActorInput struct {
	State             domain.DeploymentActorState `json:"state"`
	ActivityTaskQueue string                      `json:"activityTaskQueue"`
}

func DeploymentActorWorkflow(ctx workflow.Context, input DeploymentActorInput) error {
	state := input.State
	if workflow.GetVersion(ctx, "deployment-actor-state-v1", workflow.DefaultVersion, 1) >= 1 {
		state.SchemaVersion = 1
	}
	normalizeState(&state)
	ctx = withActivities(ctx, input.ActivityTaskQueue)

	if err := workflow.SetQueryHandler(ctx, DeploymentQueryName, func() (domain.DeploymentActorView, error) {
		return view(state), nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, VersionsQueryName, func() ([]domain.DeploymentVersion, error) {
		return state.Versions, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, OperationsQueryName, func() ([]domain.OperationRecord, error) {
		return state.Operations, nil
	}); err != nil {
		return err
	}

	signals := workflow.GetSignalChannel(ctx, DeploymentSignalName)
	for {
		var command domain.DeploymentCommand
		signals.Receive(ctx, &command)
		state.Pending = append(state.Pending, command)

		for len(state.Pending) > 0 {
			next := state.Pending[0]
			state.Pending = state.Pending[1:]
			processCommand(ctx, &state, input.ActivityTaskQueue, next)
			state.ProcessedEventCount++
			if next.Type == domain.CommandContinueAsNew || state.ProcessedEventCount >= state.ContinueAfter {
				state.ActiveOperation = nil
				state.ProcessedEventCount = 0
				return workflow.NewContinueAsNewError(ctx, DeploymentActorWorkflow, DeploymentActorInput{
					State:             compact(state),
					ActivityTaskQueue: input.ActivityTaskQueue,
				})
			}
		}
	}
}

func processCommand(ctx workflow.Context, state *domain.DeploymentActorState, activityTaskQueue string, command domain.DeploymentCommand) {
	if command.IdempotencyKey == "" {
		command.IdempotencyKey = command.OperationID
	}
	if _, exists := state.ProcessedKeys[command.IdempotencyKey]; exists {
		return
	}
	now := workflow.Now(ctx)
	if command.RequestedAt.IsZero() {
		command.RequestedAt = now
	}
	record := domain.OperationRecord{
		OperationID:    command.OperationID,
		IdempotencyKey: command.IdempotencyKey,
		Requester:      command.Requester,
		CommandType:    command.Type,
		Status:         domain.OperationRunning,
		StartedAt:      now,
	}
	state.ActiveOperation = &record
	state.Status = domain.ActorOperating
	state.LastError = ""

	var err error
	switch command.Type {
	case domain.CommandDeployVersion:
		err = deploy(ctx, state, activityTaskQueue, command, &record)
	case domain.CommandRequestSavepoint:
		err = savepoint(ctx, state, activityTaskQueue, command, &record)
	case domain.CommandSuspend:
		err = setState(ctx, state, "SUSPENDED")
		if err == nil {
			state.Status = domain.ActorSuspended
		}
	case domain.CommandResume:
		err = resume(ctx, state, activityTaskQueue)
	case domain.CommandRollback:
		err = rollback(ctx, state, activityTaskQueue, command, &record)
	case domain.CommandScaleTo:
		err = scale(ctx, state, activityTaskQueue, command, &record)
	case domain.CommandEnableAutoscaler:
		state.AutoscalerEnabled = true
	case domain.CommandFreezeAutoscaler:
		state.AutoscalerFrozen = true
	case domain.CommandContinueAsNew:
	default:
		err = fmt.Errorf("unsupported command type %q", command.Type)
	}

	record.CompletedAt = workflow.Now(ctx)
	if err != nil {
		record.Status = domain.OperationFailed
		record.Result = err.Error()
		state.Status = domain.ActorFailed
		state.LastError = err.Error()
	} else {
		record.Status = domain.OperationSucceeded
		if record.Result == "" {
			record.Result = "completed"
		}
		if state.Status != domain.ActorSuspended {
			state.Status = domain.ActorIdle
		}
	}
	state.Operations = append(state.Operations, record)
	state.ProcessedKeys[command.IdempotencyKey] = command.OperationID
	state.ActiveOperation = nil

	_ = workflow.ExecuteActivity(ctx, activities.NameRecordAudit, activities.AuditEvent{
		Identity:    state.Identity,
		OperationID: command.OperationID,
		Type:        string(command.Type),
		Message:     record.Result,
		At:          record.CompletedAt,
	}).Get(ctx, nil)
}

func deploy(ctx workflow.Context, state *domain.DeploymentActorState, activityTaskQueue string, command domain.DeploymentCommand, record *domain.OperationRecord) error {
	if command.TargetSpec == nil {
		return fmt.Errorf("targetSpec is required")
	}
	childID := "flink-rollout/" + state.Identity.Environment + "/" + state.Identity.Namespace + "/" + state.Identity.Name + "/" + command.OperationID
	record.ChildWorkflowID = childID
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: childID})
	var result RolloutResult
	err := workflow.ExecuteChildWorkflow(childCtx, RolloutWorkflow, RolloutInput{
		Identity:          state.Identity,
		Current:           state.CurrentVersion,
		Target:            *command.TargetSpec,
		Policy:            state.Policy,
		OperationID:       command.OperationID,
		Approved:          command.Approved,
		Incident:          command.Incident,
		ActivityTaskQueue: activityTaskQueue,
	}).Get(childCtx, &result)
	if result.LeaseID != "" {
		record.LeaseID = result.LeaseID
	}
	if result.Savepoint != nil {
		state.LastSavepoint = result.Savepoint
	}
	if err != nil {
		return err
	}
	state.CurrentVersion = &result.Version
	state.LastHealthyVersion = &result.Version
	state.Versions = append(state.Versions, result.Version)
	state.AutoscalerEnabled = result.Version.Spec.AutoscalerEnabled
	record.Result = fmt.Sprintf("deployment version %d is healthy", result.Version.VersionID)
	return nil
}

func savepoint(ctx workflow.Context, state *domain.DeploymentActorState, activityTaskQueue string, command domain.DeploymentCommand, record *domain.OperationRecord) error {
	if state.CurrentVersion == nil {
		return fmt.Errorf("cannot savepoint before a deployment version exists")
	}
	childID := "flink-savepoint/" + state.Identity.Environment + "/" + state.Identity.Namespace + "/" + state.Identity.Name + "/" + command.OperationID
	record.ChildWorkflowID = childID
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: childID})
	var result domain.SavepointRecord
	if err := workflow.ExecuteChildWorkflow(childCtx, SavepointWorkflow, SavepointInput{
		Identity:          state.Identity,
		Version:           *state.CurrentVersion,
		ActivityTaskQueue: activityTaskQueue,
	}).Get(childCtx, &result); err != nil {
		return err
	}
	state.LastSavepoint = &result
	record.Result = result.URI
	return nil
}

func setState(ctx workflow.Context, state *domain.DeploymentActorState, desired string) error {
	if state.CurrentVersion == nil {
		return fmt.Errorf("cannot change job state before a deployment version exists")
	}
	return workflow.ExecuteActivity(ctx, activities.NameSetJobState, activities.SetJobStateInput{
		Identity: state.Identity,
		State:    desired,
	}).Get(ctx, nil)
}

func rollback(ctx workflow.Context, state *domain.DeploymentActorState, activityTaskQueue string, command domain.DeploymentCommand, record *domain.OperationRecord) error {
	var target *domain.DeploymentVersion
	for i := range state.Versions {
		if state.Versions[i].VersionID == command.TargetVersion {
			version := state.Versions[i]
			target = &version
			break
		}
	}
	if target == nil && state.LastHealthyVersion != nil {
		target = state.LastHealthyVersion
	}
	if target == nil {
		return fmt.Errorf("rollback target was not found")
	}
	spec := target.Spec
	rollbackCommand := command
	rollbackCommand.TargetSpec = &spec
	rollbackCommand.Approved = true
	return deploy(ctx, state, activityTaskQueue, rollbackCommand, record)
}

func resume(ctx workflow.Context, state *domain.DeploymentActorState, activityTaskQueue string) error {
	if err := setState(ctx, state, "RUNNING"); err != nil {
		return err
	}
	if state.CurrentVersion == nil {
		return nil
	}
	var health domain.HealthSummary
	if err := workflow.ExecuteActivity(ctx, activities.NameObserveDeployment, activities.ObserveDeploymentInput{
		Identity: state.Identity,
		Version:  *state.CurrentVersion,
	}).Get(ctx, &health); err != nil {
		return err
	}
	state.CurrentVersion.HealthSummary = health
	return nil
}

func scale(ctx workflow.Context, state *domain.DeploymentActorState, activityTaskQueue string, command domain.DeploymentCommand, record *domain.OperationRecord) error {
	if state.CurrentVersion == nil {
		return fmt.Errorf("cannot scale before a deployment version exists")
	}
	if state.AutoscalerEnabled && !state.AutoscalerFrozen {
		return fmt.Errorf("manual scale rejected while autoscaler is enabled and not frozen")
	}
	spec := state.CurrentVersion.Spec
	spec.Parallelism = command.Parallelism
	scaleCommand := command
	scaleCommand.TargetSpec = &spec
	return deploy(ctx, state, activityTaskQueue, scaleCommand, record)
}

func normalizeState(state *domain.DeploymentActorState) {
	if state.SchemaVersion == 0 {
		state.SchemaVersion = 1
	}
	if state.Policy == (domain.Policy{}) {
		state.Policy = domain.DefaultPolicy(state.Identity.Environment)
	}
	if state.Status == "" {
		state.Status = domain.ActorIdle
	}
	if state.ProcessedKeys == nil {
		state.ProcessedKeys = make(map[string]string)
	}
	if state.ContinueAfter <= 0 {
		state.ContinueAfter = 500
	}
}

func compact(state domain.DeploymentActorState) domain.DeploymentActorState {
	const historyLimit = 50
	if len(state.Operations) > historyLimit {
		state.Operations = append([]domain.OperationRecord(nil), state.Operations[len(state.Operations)-historyLimit:]...)
	}
	if len(state.Versions) > historyLimit {
		state.Versions = append([]domain.DeploymentVersion(nil), state.Versions[len(state.Versions)-historyLimit:]...)
	}
	state.ProcessedKeys = make(map[string]string, len(state.Operations))
	for _, operation := range state.Operations {
		state.ProcessedKeys[operation.IdempotencyKey] = operation.OperationID
	}
	return state
}

func view(state domain.DeploymentActorState) domain.DeploymentActorView {
	operations := state.Operations
	if len(operations) > 20 {
		operations = operations[len(operations)-20:]
	}
	return domain.DeploymentActorView{
		Identity:           state.Identity,
		Status:             state.Status,
		CurrentVersion:     state.CurrentVersion,
		LastHealthyVersion: state.LastHealthyVersion,
		LastSavepoint:      state.LastSavepoint,
		ActiveOperation:    state.ActiveOperation,
		PendingOperations:  len(state.Pending),
		RecentOperations:   operations,
		AutoscalerEnabled:  state.AutoscalerEnabled,
		AutoscalerFrozen:   state.AutoscalerFrozen,
		LastError:          state.LastError,
	}
}
