package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maestro-flink/maestro/domain"
)

type fakeControl struct {
	identity    domain.DeploymentIdentity
	command     domain.DeploymentCommand
	listOptions domain.DeploymentListOptions
	listResult  domain.DeploymentList
	listError   error
}

func (f *fakeControl) EnsureDeploymentActor(_ context.Context, identity domain.DeploymentIdentity, _ *domain.Policy) error {
	f.identity = identity
	return nil
}

func (f *fakeControl) SendCommand(_ context.Context, _ domain.DeploymentIdentity, command domain.DeploymentCommand) error {
	f.command = command
	return nil
}

func (f *fakeControl) ListDeployments(_ context.Context, options domain.DeploymentListOptions) (domain.DeploymentList, error) {
	f.listOptions = options
	return f.listResult, f.listError
}

func (f *fakeControl) Describe(context.Context, domain.DeploymentIdentity) (domain.DeploymentActorView, error) {
	return domain.DeploymentActorView{Status: domain.ActorIdle}, nil
}

func (f *fakeControl) DescribeAll(context.Context) ([]domain.DeploymentCardSummary, error) {
	return nil, nil
}

func (f *fakeControl) Versions(context.Context, domain.DeploymentIdentity) ([]domain.DeploymentVersion, error) {
	return nil, nil
}

func (f *fakeControl) SetClusterFreeze(context.Context, string, string, domain.ClusterCommand) error {
	return nil
}

func (f *fakeControl) DescribeCluster(_ context.Context, environment, namespace string) (domain.ClusterActorState, error) {
	return domain.ClusterActorState{Environment: environment, Namespace: namespace}, nil
}

func TestDeployRequiresIdempotencyKey(t *testing.T) {
	control := &fakeControl{}
	server := New(control).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/deployments/prod/streaming/orders/deploy",
		strings.NewReader(`{"spec":{"imageDigest":"registry/orders@sha256:abc","flinkVersion":"2.2","parallelism":1,"maxParallelism":128}}`))
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", response.Code)
	}
}

func TestDeploySignalsCommand(t *testing.T) {
	control := &fakeControl{}
	server := New(control).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/deployments/prod/streaming/orders/deploy",
		strings.NewReader(`{"requester":"on-call","approved":true,"spec":{"imageDigest":"registry/orders@sha256:abc","flinkVersion":"2.2","parallelism":1,"maxParallelism":128}}`))
	request.Header.Set("Idempotency-Key", "deploy-123")
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", response.Code, response.Body.String())
	}
	if control.command.IdempotencyKey != "deploy-123" || control.command.Type != domain.CommandDeployVersion {
		t.Fatalf("unexpected command: %+v", control.command)
	}
}

func TestServesOperationsConsole(t *testing.T) {
	server := New(&fakeControl{}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
	if contentType := response.Header().Get("Content-Type"); !strings.Contains(contentType, "text/html") {
		t.Fatalf("expected HTML content type, got %q", contentType)
	}
	if !strings.Contains(response.Body.String(), "Maestro Operations Console") {
		t.Fatal("expected operations console HTML")
	}
}

func TestServesOperationsConsoleAssets(t *testing.T) {
	server := New(&fakeControl{}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/ui/app.js", nil)
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "loadActiveTarget") {
		t.Fatal("expected console JavaScript")
	}
}

func TestRegisterAcceptsFlinkDashboardURL(t *testing.T) {
	control := &fakeControl{}
	server := New(control).Handler()
	request := httptest.NewRequest(http.MethodPut, "/api/v1/deployments/prod/streaming/orders",
		strings.NewReader(`{"owner":"streaming","serviceAccount":"flink-orders","nodePool":"arm64","flinkDashboardUrl":"https://flink.example/jobs/orders"}`))
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", response.Code, response.Body.String())
	}
	if control.identity.FlinkDashboardURL != "https://flink.example/jobs/orders" {
		t.Fatalf("unexpected dashboard URL: %+v", control.identity)
	}
}

func TestListsDeployments(t *testing.T) {
	control := &fakeControl{
		listResult: domain.DeploymentList{
			Deployments: []domain.DeploymentSummary{{
				Identity: domain.DeploymentIdentity{
					Environment: "prod",
					Namespace:   "streaming",
					Name:        "orders",
				},
				WorkflowID: "flink-deployment/prod/streaming/orders",
			}},
			NextPageToken: "next",
		},
	}
	server := New(control).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/deployments?environment=prod&namespace=streaming&limit=25&pageToken=cursor", nil)
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if control.listOptions.Environment != "prod" || control.listOptions.Namespace != "streaming" ||
		control.listOptions.Limit != 25 || control.listOptions.PageToken != "cursor" {
		t.Fatalf("unexpected list options: %+v", control.listOptions)
	}
	if !strings.Contains(response.Body.String(), `"workflowId":"flink-deployment/prod/streaming/orders"`) {
		t.Fatalf("unexpected response: %s", response.Body.String())
	}
}

func TestListDeploymentsRejectsInvalidLimit(t *testing.T) {
	server := New(&fakeControl{}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/deployments?limit=501", nil)
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", response.Code)
	}
}

func TestListDeploymentsRejectsInvalidPageToken(t *testing.T) {
	server := New(&fakeControl{listError: domain.ErrInvalidDeploymentPageToken}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/deployments?pageToken=invalid", nil)
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", response.Code)
	}
}
