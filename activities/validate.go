package activities

import (
	"errors"
	"strings"

	"github.com/maestro-flink/maestro/domain"
)

// ValidateSpec performs the deterministic, backend-independent admission checks
// for a deployment command. Every Backend implementation should call it from
// ValidateDeployment so that the simulated and real adapters enforce identical
// safety rules; real adapters may layer additional environment-specific checks
// (for example, image registry allow-lists or cluster capacity) on top.
func ValidateSpec(input ValidateDeploymentInput) error {
	target := input.Target
	if target.ImageDigest == "" {
		return errors.New("imageDigest is required")
	}
	if !strings.Contains(target.ImageDigest, "@sha256:") {
		return errors.New("imageDigest must be digest-pinned")
	}
	if target.Parallelism <= 0 || target.MaxParallelism <= 0 {
		return errors.New("parallelism and maxParallelism must be positive")
	}
	if target.Parallelism > target.MaxParallelism {
		return errors.New("parallelism cannot exceed maxParallelism")
	}
	if input.Classification.Risk == domain.RiskRejected {
		return errors.New(input.Classification.Reason)
	}
	if input.Classification.RequiresApproval && !input.Approved {
		return errors.New("operation requires approval")
	}
	if input.Policy.GitOpsOnly && input.Incident && !input.Policy.AllowIncidentDirectPatch {
		return errors.New("direct incident patching is disabled")
	}
	return nil
}
