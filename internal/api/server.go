package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/maestro-flink/maestro/domain"
	"github.com/maestro-flink/maestro/internal/auth"
)

type ControlService interface {
	EnsureDeploymentActor(rctx context.Context, identity domain.DeploymentIdentity, policy *domain.Policy) error
	SendCommand(rctx context.Context, identity domain.DeploymentIdentity, command domain.DeploymentCommand) error
	ListDeployments(rctx context.Context, options domain.DeploymentListOptions) (domain.DeploymentList, error)
	DescribeAll(rctx context.Context) ([]domain.DeploymentCardSummary, error)
	Describe(rctx context.Context, identity domain.DeploymentIdentity) (domain.DeploymentActorView, error)
	Versions(rctx context.Context, identity domain.DeploymentIdentity) ([]domain.DeploymentVersion, error)
	SetClusterFreeze(rctx context.Context, environment, namespace string, command domain.ClusterCommand) error
	DescribeCluster(rctx context.Context, environment, namespace string) (domain.ClusterActorState, error)
}

type Server struct {
	control ControlService
	mux     *http.ServeMux
}

func New(controlService ControlService) *Server {
	server := &Server{control: controlService, mux: http.NewServeMux()}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler {
	return auth.Middleware(requestLogger(s.mux))
}

func (s *Server) routes() {
	registerUI(s.mux)
	s.mux.HandleFunc("GET /swagger", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/docs", http.StatusMovedPermanently)
	})
	s.mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) {
		contents, err := uiFiles.ReadFile("web/login.html")
		if err != nil {
			http.Error(w, "login page unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(contents)
	})
	s.mux.HandleFunc("GET /auth/login", auth.OAuthStartHandler)
	s.mux.HandleFunc("GET /auth/callback", auth.OAuthCallbackHandler)
	s.mux.HandleFunc("GET /auth/logout", auth.LogoutHandler)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /api/v1/deployments/summary", s.deploymentSummary)
	s.mux.HandleFunc("GET /api/v1/deployments", s.listDeployments)
	s.mux.HandleFunc("PUT /api/v1/deployments/{env}/{namespace}/{name}", s.register)
	s.mux.HandleFunc("GET /api/v1/deployments/{env}/{namespace}/{name}/actor", s.describe)
	s.mux.HandleFunc("GET /api/v1/deployments/{env}/{namespace}/{name}/versions", s.versions)
	s.mux.HandleFunc("POST /api/v1/deployments/{env}/{namespace}/{name}/deploy", s.deploy)
	s.mux.HandleFunc("POST /api/v1/deployments/{env}/{namespace}/{name}/savepoint", s.simpleCommand(domain.CommandRequestSavepoint))
	s.mux.HandleFunc("POST /api/v1/deployments/{env}/{namespace}/{name}/suspend", s.simpleCommand(domain.CommandSuspend))
	s.mux.HandleFunc("POST /api/v1/deployments/{env}/{namespace}/{name}/resume", s.simpleCommand(domain.CommandResume))
	s.mux.HandleFunc("POST /api/v1/deployments/{env}/{namespace}/{name}/rollback", s.rollback)
	s.mux.HandleFunc("POST /api/v1/deployments/{env}/{namespace}/{name}/scale", s.scale)
	s.mux.HandleFunc("POST /api/v1/deployments/{env}/{namespace}/{name}/autoscaler/enable", s.simpleCommand(domain.CommandEnableAutoscaler))
	s.mux.HandleFunc("POST /api/v1/deployments/{env}/{namespace}/{name}/autoscaler/freeze", s.simpleCommand(domain.CommandFreezeAutoscaler))
	s.mux.HandleFunc("POST /api/v1/deployments/{env}/{namespace}/{name}/continue-as-new", s.simpleCommand(domain.CommandContinueAsNew))
	s.mux.HandleFunc("GET /api/v1/clusters/{env}/{namespace}/actor", s.describeCluster)
	s.mux.HandleFunc("POST /api/v1/clusters/{env}/{namespace}/freeze", s.clusterCommand(domain.ClusterFreeze))
	s.mux.HandleFunc("POST /api/v1/clusters/{env}/{namespace}/unfreeze", s.clusterCommand(domain.ClusterUnfreeze))
}

func (s *Server) deploymentSummary(w http.ResponseWriter, r *http.Request) {
	cards, err := s.control.DescribeAll(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, cards)
}

func (s *Server) listDeployments(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if value := strings.TrimSpace(r.URL.Query().Get("limit")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 500 {
			writeError(w, http.StatusBadRequest, errors.New("limit must be between 1 and 500"))
			return
		}
		limit = parsed
	}
	result, err := s.control.ListDeployments(r.Context(), domain.DeploymentListOptions{
		Environment: strings.TrimSpace(r.URL.Query().Get("environment")),
		Namespace:   strings.TrimSpace(r.URL.Query().Get("namespace")),
		Limit:       limit,
		PageToken:   strings.TrimSpace(r.URL.Query().Get("pageToken")),
	})
	if err != nil {
		if errors.Is(err, domain.ErrInvalidDeploymentPageToken) {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) describeCluster(w http.ResponseWriter, r *http.Request) {
	state, err := s.control.DescribeCluster(r.Context(), r.PathValue("env"), r.PathValue("namespace"))
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) clusterCommand(commandType domain.ClusterCommandType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request simpleRequest
		if err := decode(r, &request); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if request.Requester == "" {
			request.Requester = "api"
		}
		command := domain.ClusterCommand{
			Type:      commandType,
			Requester: request.Requester,
			Reason:    request.Reason,
			At:        time.Now().UTC(),
		}
		if err := s.control.SetClusterFreeze(r.Context(), r.PathValue("env"), r.PathValue("namespace"), command); err != nil {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		writeJSON(w, http.StatusAccepted, command)
	}
}

type registrationRequest struct {
	Owner             string         `json:"owner"`
	ServiceAccount    string         `json:"serviceAccount"`
	NodePool          string         `json:"nodePool"`
	FlinkDashboardURL string         `json:"flinkDashboardUrl"`
	Policy            *domain.Policy `json:"policy,omitempty"`
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var request registrationRequest
	if err := decodeOptional(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	identity := identityFrom(r, request.Owner, request.ServiceAccount, request.NodePool)
	identity.FlinkDashboardURL = strings.TrimSpace(request.FlinkDashboardURL)
	if err := validateIdentity(identity); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.control.EnsureDeploymentActor(r.Context(), identity, request.Policy); err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"workflowId": identity.WorkflowID()})
}

func (s *Server) describe(w http.ResponseWriter, r *http.Request) {
	view, err := s.control.Describe(r.Context(), identityFrom(r, "", "", "default"))
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) versions(w http.ResponseWriter, r *http.Request) {
	versions, err := s.control.Versions(r.Context(), identityFrom(r, "", "", "default"))
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, versions)
}

type deployRequest struct {
	Requester string                `json:"requester"`
	Approved  bool                  `json:"approved"`
	Incident  bool                  `json:"incident"`
	Reason    string                `json:"reason"`
	Spec      domain.DeploymentSpec `json:"spec"`
}

func (s *Server) deploy(w http.ResponseWriter, r *http.Request) {
	var request deployRequest
	if err := decode(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	command, err := baseCommand(r, domain.CommandDeployVersion, request.Requester)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	command.Approved = request.Approved
	command.Incident = request.Incident
	command.Reason = request.Reason
	command.TargetSpec = &request.Spec
	s.send(w, r, command)
}

type simpleRequest struct {
	Requester string `json:"requester"`
	Approved  bool   `json:"approved"`
	Incident  bool   `json:"incident"`
	Reason    string `json:"reason"`
}

func (s *Server) simpleCommand(commandType domain.CommandType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request simpleRequest
		if err := decodeOptional(r, &request); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		command, err := baseCommand(r, commandType, request.Requester)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		command.Approved = request.Approved
		command.Incident = request.Incident
		command.Reason = request.Reason
		s.send(w, r, command)
	}
}

type rollbackRequest struct {
	Requester     string `json:"requester"`
	TargetVersion int64  `json:"targetVersion"`
	Reason        string `json:"reason"`
	Approved      bool   `json:"approved"`
}

func (s *Server) rollback(w http.ResponseWriter, r *http.Request) {
	var request rollbackRequest
	if err := decode(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	command, err := baseCommand(r, domain.CommandRollback, request.Requester)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	command.TargetVersion = request.TargetVersion
	command.Approved = request.Approved
	command.Reason = request.Reason
	s.send(w, r, command)
}

type scaleRequest struct {
	Requester   string `json:"requester"`
	Parallelism int    `json:"parallelism"`
	Approved    bool   `json:"approved"`
	Reason      string `json:"reason"`
}

func (s *Server) scale(w http.ResponseWriter, r *http.Request) {
	var request scaleRequest
	if err := decode(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if request.Parallelism <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("parallelism must be positive"))
		return
	}
	command, err := baseCommand(r, domain.CommandScaleTo, request.Requester)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	command.Parallelism = request.Parallelism
	command.Approved = request.Approved
	command.Reason = request.Reason
	s.send(w, r, command)
}

func (s *Server) send(w http.ResponseWriter, r *http.Request, command domain.DeploymentCommand) {
	identity := identityFrom(r, "", "", r.URL.Query().Get("nodePool"))
	if identity.NodePool == "" {
		identity.NodePool = "default"
	}
	if err := s.control.SendCommand(r.Context(), identity, command); err != nil {
		var frozen *domain.ClusterFrozenError
		if errors.As(err, &frozen) {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{
		"operationId": command.OperationID,
		"workflowId":  identity.WorkflowID(),
		"status":      string(domain.OperationQueued),
	})
}

func identityFrom(r *http.Request, owner, serviceAccount, nodePool string) domain.DeploymentIdentity {
	return domain.DeploymentIdentity{
		Environment:    r.PathValue("env"),
		Namespace:      r.PathValue("namespace"),
		Name:           r.PathValue("name"),
		Owner:          owner,
		ServiceAccount: serviceAccount,
		NodePool:       nodePool,
	}
}

func validateIdentity(identity domain.DeploymentIdentity) error {
	if strings.TrimSpace(identity.Environment) == "" || strings.TrimSpace(identity.Namespace) == "" || strings.TrimSpace(identity.Name) == "" {
		return errors.New("environment, namespace, and name are required")
	}
	return nil
}

func baseCommand(r *http.Request, commandType domain.CommandType, requester string) (domain.DeploymentCommand, error) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		return domain.DeploymentCommand{}, errors.New("Idempotency-Key header is required")
	}
	if requester == "" {
		requester = "api"
	}
	return domain.DeploymentCommand{
		OperationID:    key,
		IdempotencyKey: key,
		Type:           commandType,
		Requester:      requester,
		RequestedAt:    time.Now().UTC(),
	}, nil
}

func decode(r *http.Request, target any) error {
	if r.Body == nil {
		return errors.New("request body is required")
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func decodeOptional(r *http.Request, target any) error {
	if r.Body == nil || r.ContentLength == 0 {
		return nil
	}
	return decode(r, target)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("http request", "method", r.Method, "path", r.URL.Path, "durationMs", strconv.FormatInt(time.Since(start).Milliseconds(), 10))
	})
}
