package control

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/maestro-flink/maestro"
	"github.com/maestro-flink/maestro/domain"
	"github.com/maestro-flink/maestro/workflows"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

type Service struct {
	client            client.Client
	actorTaskQueue    string
	actorShards       int
	activityTaskQueue string
	continueAfter     int
}

func NewService(temporalClient client.Client, actorTaskQueue, activityTaskQueue string, continueAfter, actorShards int) *Service {
	if actorShards < 1 {
		actorShards = 1
	}
	return &Service{
		client:            temporalClient,
		actorTaskQueue:    actorTaskQueue,
		actorShards:       actorShards,
		activityTaskQueue: activityTaskQueue,
		continueAfter:     continueAfter,
	}
}

// actorQueueFor returns the sharded actor task queue that owns a workflow ID.
func (s *Service) actorQueueFor(workflowID string) string {
	return maestro.ShardTaskQueue(s.actorTaskQueue, s.actorShards, workflowID)
}

func (s *Service) EnsureDeploymentActor(ctx context.Context, identity domain.DeploymentIdentity, policy *domain.Policy) error {
	if err := s.EnsureClusterActor(ctx, identity.Environment, identity.Namespace); err != nil {
		return err
	}
	selectedPolicy := domain.DefaultPolicy(identity.Environment)
	if policy != nil {
		selectedPolicy = *policy
	}
	_, err := s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    identity.WorkflowID(),
		TaskQueue:             s.actorQueueFor(identity.WorkflowID()),
		WorkflowIDReusePolicy: 1,
	}, workflows.DeploymentActorWorkflow, workflows.DeploymentActorInput{
		State: domain.DeploymentActorState{
			SchemaVersion: 1,
			Identity:      identity,
			Policy:        selectedPolicy,
			Status:        domain.ActorIdle,
			ProcessedKeys: make(map[string]string),
			ContinueAfter: s.continueAfter,
		},
		ActivityTaskQueue: s.activityTaskQueue,
	})
	if err == nil {
		return nil
	}
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	return fmt.Errorf("start deployment actor: %w", err)
}

func (s *Service) SendCommand(ctx context.Context, identity domain.DeploymentIdentity, command domain.DeploymentCommand) error {
	if err := s.EnsureDeploymentActor(ctx, identity, nil); err != nil {
		return err
	}
	cluster, err := s.DescribeCluster(ctx, identity.Environment, identity.Namespace)
	if err != nil {
		return err
	}
	if cluster.Frozen && mutatesRuntime(command.Type) {
		return &domain.ClusterFrozenError{Requester: cluster.Requester, Reason: cluster.Reason}
	}
	if err := s.client.SignalWorkflow(ctx, identity.WorkflowID(), "", workflows.DeploymentSignalName, command); err != nil {
		return fmt.Errorf("signal deployment actor: %w", err)
	}
	return nil
}

func (s *Service) EnsureClusterActor(ctx context.Context, environment, namespace string) error {
	state := domain.ClusterActorState{Environment: environment, Namespace: namespace}
	_, err := s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    state.WorkflowID(),
		TaskQueue:             s.actorQueueFor(state.WorkflowID()),
		WorkflowIDReusePolicy: 1,
	}, workflows.ClusterActorWorkflow, state)
	if err == nil {
		return nil
	}
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	return fmt.Errorf("start cluster actor: %w", err)
}

func (s *Service) SetClusterFreeze(ctx context.Context, environment, namespace string, command domain.ClusterCommand) error {
	if err := s.EnsureClusterActor(ctx, environment, namespace); err != nil {
		return err
	}
	state := domain.ClusterActorState{Environment: environment, Namespace: namespace}
	if err := s.client.SignalWorkflow(ctx, state.WorkflowID(), "", domain.ClusterCommandSignal, command); err != nil {
		return fmt.Errorf("signal cluster actor: %w", err)
	}
	return nil
}

func (s *Service) DescribeCluster(ctx context.Context, environment, namespace string) (domain.ClusterActorState, error) {
	state := domain.ClusterActorState{Environment: environment, Namespace: namespace}
	response, err := s.client.QueryWorkflow(ctx, state.WorkflowID(), "", domain.ClusterQueryName)
	if err != nil {
		return domain.ClusterActorState{}, fmt.Errorf("query cluster actor: %w", err)
	}
	if err := response.Get(&state); err != nil {
		return domain.ClusterActorState{}, fmt.Errorf("decode cluster actor: %w", err)
	}
	return state, nil
}

func (s *Service) Describe(ctx context.Context, identity domain.DeploymentIdentity) (domain.DeploymentActorView, error) {
	response, err := s.client.QueryWorkflow(ctx, identity.WorkflowID(), "", workflows.DeploymentQueryName)
	if err != nil {
		return domain.DeploymentActorView{}, fmt.Errorf("query deployment actor: %w", err)
	}
	var view domain.DeploymentActorView
	if err := response.Get(&view); err != nil {
		return domain.DeploymentActorView{}, fmt.Errorf("decode deployment actor view: %w", err)
	}
	return view, nil
}

func (s *Service) Versions(ctx context.Context, identity domain.DeploymentIdentity) ([]domain.DeploymentVersion, error) {
	response, err := s.client.QueryWorkflow(ctx, identity.WorkflowID(), "", workflows.VersionsQueryName)
	if err != nil {
		return nil, fmt.Errorf("query versions: %w", err)
	}
	var versions []domain.DeploymentVersion
	if err := response.Get(&versions); err != nil {
		return nil, fmt.Errorf("decode versions: %w", err)
	}
	return versions, nil
}

type deploymentListCursor struct {
	NextPageToken []byte `json:"nextPageToken,omitempty"`
	Offset        int    `json:"offset,omitempty"`
}

func (s *Service) ListDeployments(ctx context.Context, options domain.DeploymentListOptions) (domain.DeploymentList, error) {
	limit := options.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	cursor, err := decodeDeploymentListCursor(options.PageToken)
	if err != nil {
		return domain.DeploymentList{}, err
	}

	result := domain.DeploymentList{Deployments: make([]domain.DeploymentSummary, 0, limit)}
	for len(result.Deployments) < limit {
		requestToken := cursor.NextPageToken
		pageSize := limit * 2
		if pageSize < 100 {
			pageSize = 100
		}
		if pageSize > 1000 {
			pageSize = 1000
		}
		response, err := s.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			PageSize:      int32(pageSize),
			NextPageToken: requestToken,
			Query:         "CloseTime IS NULL",
		})
		if err != nil {
			return domain.DeploymentList{}, fmt.Errorf("list deployment actors: %w", err)
		}
		if cursor.Offset < 0 || cursor.Offset > len(response.Executions) {
			return domain.DeploymentList{}, domain.ErrInvalidDeploymentPageToken
		}

		for index := cursor.Offset; index < len(response.Executions); index++ {
			execution := response.Executions[index]
			if execution.GetType().GetName() != "DeploymentActorWorkflow" {
				continue
			}
			workflowID := execution.GetExecution().GetWorkflowId()
			identity, ok := deploymentIdentityFromWorkflowID(workflowID)
			if !ok || !deploymentMatches(identity, options) {
				continue
			}

			summary := domain.DeploymentSummary{
				Identity:   identity,
				WorkflowID: workflowID,
			}
			if execution.GetStartTime() != nil {
				summary.StartedAt = execution.GetStartTime().AsTime()
			}
			result.Deployments = append(result.Deployments, summary)

			if len(result.Deployments) == limit {
				nextCursor := deploymentListCursor{}
				if index+1 < len(response.Executions) {
					nextCursor.NextPageToken = requestToken
					nextCursor.Offset = index + 1
				} else {
					nextCursor.NextPageToken = response.NextPageToken
				}
				result.NextPageToken, err = encodeDeploymentListCursor(nextCursor)
				if err != nil {
					return domain.DeploymentList{}, err
				}
				return result, nil
			}
		}

		if len(response.NextPageToken) == 0 {
			break
		}
		cursor = deploymentListCursor{NextPageToken: response.NextPageToken}
	}

	return result, nil
}

func deploymentIdentityFromWorkflowID(workflowID string) (domain.DeploymentIdentity, bool) {
	const prefix = "flink-deployment/"
	if !strings.HasPrefix(workflowID, prefix) {
		return domain.DeploymentIdentity{}, false
	}
	parts := strings.SplitN(strings.TrimPrefix(workflowID, prefix), "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return domain.DeploymentIdentity{}, false
	}
	return domain.DeploymentIdentity{
		Environment: parts[0],
		Namespace:   parts[1],
		Name:        parts[2],
	}, true
}

func deploymentMatches(identity domain.DeploymentIdentity, options domain.DeploymentListOptions) bool {
	return (options.Environment == "" || identity.Environment == options.Environment) &&
		(options.Namespace == "" || identity.Namespace == options.Namespace)
}

func decodeDeploymentListCursor(value string) (deploymentListCursor, error) {
	if value == "" {
		return deploymentListCursor{}, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return deploymentListCursor{}, domain.ErrInvalidDeploymentPageToken
	}
	var cursor deploymentListCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return deploymentListCursor{}, domain.ErrInvalidDeploymentPageToken
	}
	return cursor, nil
}

func encodeDeploymentListCursor(cursor deploymentListCursor) (string, error) {
	if len(cursor.NextPageToken) == 0 && cursor.Offset == 0 {
		return "", nil
	}
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode deployment list page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func (s *Service) DescribeAll(ctx context.Context) ([]domain.DeploymentCardSummary, error) {
	list, err := s.ListDeployments(ctx, domain.DeploymentListOptions{Limit: 500})
	if err != nil {
		return nil, err
	}
	cards := make([]domain.DeploymentCardSummary, len(list.Deployments))
	type result struct {
		index int
		view  domain.DeploymentActorView
		err   error
	}
	ch := make(chan result, len(list.Deployments))
	for i, dep := range list.Deployments {
		go func(idx int, d domain.DeploymentSummary) {
			view, err := s.Describe(ctx, d.Identity)
			ch <- result{idx, view, err}
		}(i, dep)
	}
	for range list.Deployments {
		r := <-ch
		dep := list.Deployments[r.index]
		card := domain.DeploymentCardSummary{
			Identity:   dep.Identity,
			WorkflowID: dep.WorkflowID,
			StartedAt:  dep.StartedAt,
		}
		if r.err != nil {
			card.Status = domain.ActorStatus("UNREACHABLE")
			card.Error = r.err.Error()
		} else {
			card.Status = r.view.Status
			card.Pending = r.view.PendingOperations
			card.Error = r.view.LastError
			if v := r.view.CurrentVersion; v != nil {
				card.Version = v.VersionID
				card.ImageDigest = v.Spec.ImageDigest
				card.Parallelism = v.Spec.Parallelism
				h := v.HealthSummary.Healthy
				card.Healthy = &h
				card.HealthMessage = v.HealthSummary.Message
			}
		}
		cards[r.index] = card
	}
	return cards, nil
}

func mutatesRuntime(commandType domain.CommandType) bool {
	return commandType != domain.CommandRequestSavepoint && commandType != domain.CommandContinueAsNew
}
