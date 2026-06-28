package domain

import (
	"errors"
	"time"
)

var ErrInvalidDeploymentPageToken = errors.New("invalid deployment list page token")

type DeploymentListOptions struct {
	Environment string
	Namespace   string
	Limit       int
	PageToken   string
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
