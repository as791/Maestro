package control

import (
	"context"
	"testing"
	"time"

	"github.com/maestro-flink/maestro/domain"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/mocks"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/stretchr/testify/mock"
)

func TestListDeploymentsPaginatesWithinVisibilityPage(t *testing.T) {
	startedAt := time.Date(2026, time.June, 26, 10, 30, 0, 0, time.UTC)
	response := &workflowservice.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			workflowExecution("flink-cluster/prod/streaming", "ClusterActorWorkflow", startedAt),
			workflowExecution("flink-deployment/prod/streaming/orders", "DeploymentActorWorkflow", startedAt),
			workflowExecution("flink-deployment/prod/streaming/payments", "DeploymentActorWorkflow", startedAt.Add(-time.Minute)),
		},
	}
	temporalClient := new(mocks.Client)
	temporalClient.On("ListWorkflow", mock.Anything, mock.Anything).Return(response, nil).Twice()
	service := NewService(temporalClient, "actors", "activities", 500, 1)

	first, err := service.ListDeployments(context.Background(), domain.DeploymentListOptions{
		Environment: "prod",
		Namespace:   "streaming",
		Limit:       1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Deployments) != 1 || first.Deployments[0].Identity.Name != "orders" {
		t.Fatalf("unexpected first page: %+v", first)
	}
	if first.NextPageToken == "" {
		t.Fatal("expected next page token")
	}

	second, err := service.ListDeployments(context.Background(), domain.DeploymentListOptions{
		Environment: "prod",
		Namespace:   "streaming",
		Limit:       1,
		PageToken:   first.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Deployments) != 1 || second.Deployments[0].Identity.Name != "payments" {
		t.Fatalf("unexpected second page: %+v", second)
	}
	if !second.Deployments[0].StartedAt.Equal(startedAt.Add(-time.Minute)) {
		t.Fatalf("unexpected start time: %s", second.Deployments[0].StartedAt)
	}
	if second.NextPageToken != "" {
		t.Fatalf("unexpected next page token: %q", second.NextPageToken)
	}
	temporalClient.AssertExpectations(t)
}

func TestListDeploymentsRejectsInvalidPageToken(t *testing.T) {
	service := NewService(new(mocks.Client), "actors", "activities", 500, 1)

	_, err := service.ListDeployments(context.Background(), domain.DeploymentListOptions{
		PageToken: "not-valid-base64",
	})
	if err == nil {
		t.Fatal("expected invalid page token error")
	}
}

func workflowExecution(workflowID, workflowType string, startedAt time.Time) *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Type:      &commonpb.WorkflowType{Name: workflowType},
		StartTime: timestamppb.New(startedAt),
	}
}
