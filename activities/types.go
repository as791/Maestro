package activities

import (
	"context"
	"time"

	"github.com/maestro-flink/maestro/domain"
)

// Backend is the supported integration boundary for Kubernetes, GitOps, Flink,
// metrics, object storage, audit, and resource-capacity implementations.
// Implementations must be idempotent because Temporal may retry activities.
type Backend interface {
	ValidateDeployment(context.Context, ValidateDeploymentInput) error
	AcquireCapacityLease(context.Context, AcquireLeaseInput) (domain.Lease, error)
	ReleaseCapacityLease(context.Context, string) error
	ApplyDeployment(context.Context, ApplyDeploymentInput) (ApplyDeploymentResult, error)
	ObserveDeployment(context.Context, ObserveDeploymentInput) (domain.HealthSummary, error)
	TriggerSavepoint(context.Context, TriggerSavepointInput) (SavepointTrigger, error)
	ObserveSavepoint(context.Context, ObserveSavepointInput) (domain.SavepointRecord, error)
	SetJobState(context.Context, SetJobStateInput) error
	RecordAudit(context.Context, AuditEvent) error
}

const (
	NameValidateDeployment = "ValidateDeployment"
	NameAcquireLease       = "AcquireCapacityLease"
	NameReleaseLease       = "ReleaseCapacityLease"
	NameApplyDeployment    = "ApplyDeployment"
	NameObserveDeployment  = "ObserveDeployment"
	NameTriggerSavepoint   = "TriggerSavepoint"
	NameObserveSavepoint   = "ObserveSavepoint"
	NameSetJobState        = "SetJobState"
	NameRecordAudit        = "RecordAudit"
)

type ValidateDeploymentInput struct {
	Identity       domain.DeploymentIdentity   `json:"identity"`
	CurrentVersion *domain.DeploymentVersion   `json:"currentVersion,omitempty"`
	Target         domain.DeploymentSpec       `json:"target"`
	Classification domain.ChangeClassification `json:"classification"`
	Approved       bool                        `json:"approved"`
	Incident       bool                        `json:"incident"`
	Policy         domain.Policy               `json:"policy"`
}

type AcquireLeaseInput struct {
	Identity      domain.DeploymentIdentity `json:"identity"`
	Resources     domain.ResourceShape      `json:"resources"`
	OwnerWorkflow string                    `json:"ownerWorkflow"`
	TTL           time.Duration             `json:"ttl"`
}

type ApplyDeploymentInput struct {
	Identity     domain.DeploymentIdentity `json:"identity"`
	Version      domain.DeploymentVersion  `json:"version"`
	SavepointURI string                    `json:"savepointUri,omitempty"`
	Previous     *domain.DeploymentVersion `json:"previous,omitempty"`
	Rollback     bool                      `json:"rollback"`
	Incident     bool                      `json:"incident"`
	GitOpsOnly   bool                      `json:"gitOpsOnly"`
}

type ApplyDeploymentResult struct {
	ObservedGeneration int64  `json:"observedGeneration"`
	ApplyReference     string `json:"applyReference"`
}

type ObserveDeploymentInput struct {
	Identity domain.DeploymentIdentity `json:"identity"`
	Version  domain.DeploymentVersion  `json:"version"`
}

type TriggerSavepointInput struct {
	Identity domain.DeploymentIdentity `json:"identity"`
	Version  domain.DeploymentVersion  `json:"version"`
}

type SavepointTrigger struct {
	TriggerID  string `json:"triggerId"`
	FlinkJobID string `json:"flinkJobId"`
}

type ObserveSavepointInput struct {
	Identity domain.DeploymentIdentity `json:"identity"`
	Version  domain.DeploymentVersion  `json:"version"`
	Trigger  SavepointTrigger          `json:"trigger"`
}

type SetJobStateInput struct {
	Identity domain.DeploymentIdentity `json:"identity"`
	State    string                    `json:"state"`
}

type AuditEvent struct {
	Identity    domain.DeploymentIdentity `json:"identity"`
	OperationID string                    `json:"operationId"`
	Type        string                    `json:"type"`
	Message     string                    `json:"message"`
	At          time.Time                 `json:"at"`
}
