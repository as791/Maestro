// Package maestro provides a Go client for the Maestro Flink Control Plane REST API.
package maestro

import "time"

type ResourceShape struct {
	TaskManagerCPU    float64 `json:"taskManagerCpu"`
	TaskManagerMemory int64   `json:"taskManagerMemoryMiB"`
	TaskManagerCount  int     `json:"taskManagerCount"`
	SlotsPerManager   int     `json:"slotsPerManager"`
}

type StateCompatibility struct {
	JobGraphCompatible bool `json:"jobGraphCompatible"`
	OperatorUIDsStable bool `json:"operatorUidsStable"`
	AllowNonRestored   bool `json:"allowNonRestored"`
	FreshStartApproved bool `json:"freshStartApproved"`
}

type DeploymentSpec struct {
	ImageDigest       string             `json:"imageDigest"`
	GitRef            string             `json:"gitRef,omitempty"`
	FlinkVersion      string             `json:"flinkVersion"`
	JobArgs           map[string]string  `json:"jobArgs,omitempty"`
	FlinkConfig       map[string]string  `json:"flinkConfig,omitempty"`
	Parallelism       int                `json:"parallelism"`
	MaxParallelism    int                `json:"maxParallelism"`
	Resources         ResourceShape      `json:"resources"`
	State             StateCompatibility `json:"stateCompatibility"`
	AutoscalerEnabled bool               `json:"autoscalerEnabled"`
}

type DeploymentIdentity struct {
	Environment       string `json:"environment"`
	Namespace         string `json:"namespace"`
	Name              string `json:"name"`
	Owner             string `json:"owner,omitempty"`
	ServiceAccount    string `json:"serviceAccount,omitempty"`
	NodePool          string `json:"nodePool,omitempty"`
	FlinkDashboardURL string `json:"flinkDashboardUrl,omitempty"`
}

type HealthSummary struct {
	Healthy             bool      `json:"healthy"`
	Running             bool      `json:"running"`
	CheckpointCompleted bool      `json:"checkpointCompleted"`
	RestartCount        int       `json:"restartCount"`
	BackpressureRatio   float64   `json:"backpressureRatio"`
	KafkaLag            int64     `json:"kafkaLag"`
	SinkHealthy         bool      `json:"sinkHealthy"`
	Message             string    `json:"message,omitempty"`
	ObservedAt          time.Time `json:"observedAt"`
}

type DeploymentVersion struct {
	VersionID                  int64          `json:"versionId"`
	Spec                       DeploymentSpec `json:"spec"`
	ManifestHash               string         `json:"manifestHash"`
	JobArgsHash                string         `json:"jobArgsHash"`
	FlinkConfigHash            string         `json:"flinkConfigHash"`
	SavepointURI               string         `json:"savepointUri,omitempty"`
	OperatorObservedGeneration int64          `json:"operatorObservedGeneration,omitempty"`
	HealthSummary              HealthSummary  `json:"healthSummary"`
	CreatedAt                  time.Time      `json:"createdAt"`
}

type SavepointRecord struct {
	URI                string    `json:"uri"`
	TriggerID          string    `json:"triggerId"`
	FlinkJobID         string    `json:"flinkJobId"`
	DeploymentVersion  int64     `json:"deploymentVersion"`
	ImageDigest        string    `json:"imageDigest"`
	JobArgsHash        string    `json:"jobArgsHash"`
	Parallelism        int       `json:"parallelism"`
	MaxParallelism     int       `json:"maxParallelism"`
	CompatibilityNotes string    `json:"compatibilityNotes,omitempty"`
	CreatedAt          time.Time `json:"createdAt"`
}

type OperationStatus string

const (
	OperationQueued    OperationStatus = "QUEUED"
	OperationRunning   OperationStatus = "RUNNING"
	OperationSucceeded OperationStatus = "SUCCEEDED"
	OperationFailed    OperationStatus = "FAILED"
	OperationRejected  OperationStatus = "REJECTED"
)

type OperationRecord struct {
	OperationID     string          `json:"operationId"`
	IdempotencyKey  string          `json:"idempotencyKey"`
	Requester       string          `json:"requester"`
	CommandType     string          `json:"commandType"`
	Status          OperationStatus `json:"status"`
	ChildWorkflowID string          `json:"childWorkflowId,omitempty"`
	LeaseID         string          `json:"leaseId,omitempty"`
	Result          string          `json:"result,omitempty"`
	StartedAt       time.Time       `json:"startedAt,omitempty"`
	CompletedAt     time.Time       `json:"completedAt,omitempty"`
}

type ActorStatus string

const (
	ActorIdle      ActorStatus = "IDLE"
	ActorOperating ActorStatus = "OPERATING"
	ActorFailed    ActorStatus = "FAILED"
	ActorSuspended ActorStatus = "SUSPENDED"
)

type DeploymentActorView struct {
	Identity           DeploymentIdentity `json:"identity"`
	Status             ActorStatus        `json:"status"`
	CurrentVersion     *DeploymentVersion `json:"currentVersion,omitempty"`
	LastHealthyVersion *DeploymentVersion `json:"lastHealthyVersion,omitempty"`
	LastSavepoint      *SavepointRecord   `json:"lastSavepoint,omitempty"`
	ActiveOperation    *OperationRecord   `json:"activeOperation,omitempty"`
	PendingOperations  int                `json:"pendingOperations"`
	RecentOperations   []OperationRecord  `json:"recentOperations,omitempty"`
	AutoscalerEnabled  bool               `json:"autoscalerEnabled"`
	AutoscalerFrozen   bool               `json:"autoscalerFrozen"`
	LastError          string             `json:"lastError,omitempty"`
}

type DeploymentSummary struct {
	Identity   DeploymentIdentity `json:"identity"`
	WorkflowID string             `json:"workflowId"`
	StartedAt  time.Time          `json:"startedAt,omitempty"`
}

type DeploymentList struct {
	Deployments   []DeploymentSummary `json:"deployments"`
	NextPageToken string              `json:"nextPageToken,omitempty"`
}

type DeploymentCardSummary struct {
	Identity      DeploymentIdentity `json:"identity"`
	WorkflowID    string             `json:"workflowId"`
	StartedAt     time.Time          `json:"startedAt,omitempty"`
	Status        ActorStatus        `json:"status"`
	Version       int64              `json:"version,omitempty"`
	ImageDigest   string             `json:"imageDigest,omitempty"`
	Parallelism   int                `json:"parallelism,omitempty"`
	Healthy       *bool              `json:"healthy,omitempty"`
	HealthMessage string             `json:"healthMessage,omitempty"`
	Pending       int                `json:"pendingOperations"`
	Error         string             `json:"error,omitempty"`
}

type ClusterActorState struct {
	Environment string    `json:"environment"`
	Namespace   string    `json:"namespace"`
	Frozen      bool      `json:"frozen"`
	Reason      string    `json:"reason,omitempty"`
	Requester   string    `json:"requester,omitempty"`
	UpdatedAt   time.Time `json:"updatedAt,omitempty"`
}

type Policy struct {
	RequireProdApproval      bool    `json:"requireProdApproval"`
	GitOpsOnly               bool    `json:"gitOpsOnly"`
	AllowIncidentDirectPatch bool    `json:"allowIncidentDirectPatch"`
	RequireRiskySavepoint    bool    `json:"requireRiskySavepoint"`
	RequireFirstCheckpoint   bool    `json:"requireFirstCheckpoint"`
	RequireHealthySink       bool    `json:"requireHealthySink"`
	MaxRestartCount          int     `json:"maxRestartCount"`
	MaxBackpressureRatio     float64 `json:"maxBackpressureRatio"`
	MaxKafkaLag              int64   `json:"maxKafkaLag"`
}

type CommandResponse struct {
	OperationID string `json:"operationId"`
	WorkflowID  string `json:"workflowId"`
	Status      string `json:"status"`
}

type RegisterResponse struct {
	WorkflowID string `json:"workflowId"`
}

type ListOptions struct {
	Environment string
	Namespace   string
	Limit       int
	PageToken   string
}

type RegisterRequest struct {
	Owner             string  `json:"owner,omitempty"`
	ServiceAccount    string  `json:"serviceAccount,omitempty"`
	NodePool          string  `json:"nodePool,omitempty"`
	FlinkDashboardURL string  `json:"flinkDashboardUrl,omitempty"`
	Policy            *Policy `json:"policy,omitempty"`
}

type DeployRequest struct {
	Requester string         `json:"requester"`
	Approved  bool           `json:"approved,omitempty"`
	Incident  bool           `json:"incident,omitempty"`
	Reason    string         `json:"reason,omitempty"`
	Spec      DeploymentSpec `json:"spec"`
}

type ScaleRequest struct {
	Requester   string `json:"requester"`
	Parallelism int    `json:"parallelism"`
	Approved    bool   `json:"approved,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type RollbackRequest struct {
	Requester     string `json:"requester"`
	TargetVersion int64  `json:"targetVersion"`
	Approved      bool   `json:"approved,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

type SimpleRequest struct {
	Requester string `json:"requester"`
	Approved  bool   `json:"approved,omitempty"`
	Incident  bool   `json:"incident,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type ClusterCommandRequest struct {
	Requester string `json:"requester"`
	Reason    string `json:"reason,omitempty"`
}

type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return "maestro: " + e.Message
}
