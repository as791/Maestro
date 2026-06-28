package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
)

type ChangeRisk string

const (
	RiskNone     ChangeRisk = "NONE"
	RiskSafe     ChangeRisk = "SAFE_LAST_STATE"
	RiskStateful ChangeRisk = "STATEFUL"
	RiskRejected ChangeRisk = "REJECTED"
)

type ChangeClassification struct {
	Risk                  ChangeRisk `json:"risk"`
	RequiresSavepoint     bool       `json:"requiresSavepoint"`
	RequiresApproval      bool       `json:"requiresApproval"`
	RequiresCapacityLease bool       `json:"requiresCapacityLease"`
	Reason                string     `json:"reason"`
}

func ClassifyChange(current *DeploymentVersion, target DeploymentSpec, policy Policy) ChangeClassification {
	if current == nil {
		return ChangeClassification{
			Risk:                  RiskSafe,
			RequiresApproval:      policy.RequireProdApproval,
			RequiresCapacityLease: target.Resources.Slots() > 0,
			Reason:                "initial deployment",
		}
	}

	old := current.Spec
	if target.MaxParallelism < old.MaxParallelism && current.VersionID > 0 && !target.State.FreshStartApproved {
		return ChangeClassification{
			Risk:             RiskRejected,
			RequiresApproval: true,
			Reason:           "max parallelism cannot decrease after state exists without an approved fresh start",
		}
	}
	if !target.State.JobGraphCompatible || !target.State.OperatorUIDsStable || target.State.AllowNonRestored {
		return ChangeClassification{
			Risk:                  RiskStateful,
			RequiresSavepoint:     true,
			RequiresApproval:      true,
			RequiresCapacityLease: old.Resources != target.Resources,
			Reason:                "state compatibility declaration requires guarded rollout",
		}
	}
	if !reflect.DeepEqual(old.JobArgs, target.JobArgs) || old.MaxParallelism != target.MaxParallelism {
		return ChangeClassification{
			Risk:                  RiskStateful,
			RequiresSavepoint:     policy.RequireRiskySavepoint,
			RequiresApproval:      policy.RequireProdApproval,
			RequiresCapacityLease: old.Resources != target.Resources,
			Reason:                "job arguments or max parallelism changed",
		}
	}
	if old.Parallelism != target.Parallelism || old.Resources != target.Resources {
		return ChangeClassification{
			Risk:                  RiskSafe,
			RequiresApproval:      policy.RequireProdApproval,
			RequiresCapacityLease: true,
			Reason:                "parallelism or resource shape changed",
		}
	}
	if old.ImageDigest != target.ImageDigest || !reflect.DeepEqual(old.FlinkConfig, target.FlinkConfig) {
		return ChangeClassification{
			Risk:             RiskSafe,
			RequiresApproval: policy.RequireProdApproval,
			Reason:           "image or Flink configuration changed",
		}
	}
	return ChangeClassification{Risk: RiskNone, Reason: "no material change"}
}

func EvaluateHealth(summary HealthSummary, policy Policy) error {
	if !summary.Running {
		return fmt.Errorf("deployment is not running")
	}
	if policy.RequireFirstCheckpoint && !summary.CheckpointCompleted {
		return fmt.Errorf("no completed checkpoint observed")
	}
	if policy.RequireHealthySink && !summary.SinkHealthy {
		return fmt.Errorf("sink health gate failed")
	}
	if summary.RestartCount > policy.MaxRestartCount {
		return fmt.Errorf("restart count %d exceeds limit %d", summary.RestartCount, policy.MaxRestartCount)
	}
	if summary.BackpressureRatio > policy.MaxBackpressureRatio {
		return fmt.Errorf("backpressure %.2f exceeds limit %.2f", summary.BackpressureRatio, policy.MaxBackpressureRatio)
	}
	if summary.KafkaLag > policy.MaxKafkaLag {
		return fmt.Errorf("Kafka lag %d exceeds limit %d", summary.KafkaLag, policy.MaxKafkaLag)
	}
	return nil
}

func BuildVersion(id int64, spec DeploymentSpec) DeploymentVersion {
	return DeploymentVersion{
		VersionID:       id,
		Spec:            spec,
		ManifestHash:    hash(spec),
		JobArgsHash:     hashMap(spec.JobArgs),
		FlinkConfigHash: hashMap(spec.FlinkConfig),
	}
}

func hash(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hashMap(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([][2]string, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, [2]string{key, values[key]})
	}
	return hash(ordered)
}

