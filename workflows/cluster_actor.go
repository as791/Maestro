package workflows

import (
	"github.com/maestro-flink/maestro/domain"
	"go.temporal.io/sdk/workflow"
)

func ClusterActorWorkflow(ctx workflow.Context, state domain.ClusterActorState) error {
	if err := workflow.SetQueryHandler(ctx, domain.ClusterQueryName, func() (domain.ClusterActorState, error) {
		return state, nil
	}); err != nil {
		return err
	}

	signals := workflow.GetSignalChannel(ctx, domain.ClusterCommandSignal)
	for {
		var command domain.ClusterCommand
		signals.Receive(ctx, &command)
		switch command.Type {
		case domain.ClusterFreeze:
			state.Frozen = true
			state.Reason = command.Reason
			state.Requester = command.Requester
			state.UpdatedAt = workflow.Now(ctx)
		case domain.ClusterUnfreeze:
			state.Frozen = false
			state.Reason = command.Reason
			state.Requester = command.Requester
			state.UpdatedAt = workflow.Now(ctx)
		}
	}
}
