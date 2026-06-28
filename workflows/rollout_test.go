package workflows

import (
	"testing"
	"time"

	"github.com/maestro-flink/maestro/activities"
	"github.com/maestro-flink/maestro/domain"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestInitialRolloutWorkflow(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	simulated := activities.NewSimulated(0)
	registerTestActivities(env, simulated)
	env.RegisterWorkflow(RolloutWorkflow)
	env.RegisterWorkflow(SavepointWorkflow)

	input := RolloutInput{
		Identity: domain.DeploymentIdentity{
			Environment: "integration",
			Namespace:   "streaming",
			Name:        "orders",
			NodePool:    "arm64",
		},
		Target:            workflowSpec(),
		Policy:            domain.DefaultPolicy("integration"),
		OperationID:       "deploy-1",
		Approved:          true,
		ActivityTaskQueue: "test",
	}

	env.ExecuteWorkflow(RolloutWorkflow, input)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result RolloutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, int64(1), result.Version.VersionID)
	require.True(t, result.Version.HealthSummary.Healthy)
	require.NotEmpty(t, result.LeaseID)
}

func TestStatefulRolloutCreatesSavepoint(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	simulated := activities.NewSimulated(0)
	registerTestActivities(env, simulated)
	env.RegisterWorkflow(RolloutWorkflow)
	env.RegisterWorkflow(SavepointWorkflow)

	current := domain.BuildVersion(1, workflowSpec())
	current.CreatedAt = time.Now().UTC()
	target := workflowSpec()
	target.JobArgs = map[string]string{"topic": "orders-v2"}

	env.ExecuteWorkflow(RolloutWorkflow, RolloutInput{
		Identity: domain.DeploymentIdentity{
			Environment: "prod",
			Namespace:   "streaming",
			Name:        "orders",
			NodePool:    "arm64",
		},
		Current:           &current,
		Target:            target,
		Policy:            domain.DefaultPolicy("prod"),
		OperationID:       "deploy-2",
		Approved:          true,
		ActivityTaskQueue: "test",
	})

	require.NoError(t, env.GetWorkflowError())
	var result RolloutResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.NotNil(t, result.Savepoint)
	require.Contains(t, result.Savepoint.URI, "s3://flink-savepoints/prod/orders/")
	require.Equal(t, int64(2), result.Version.VersionID)
}

func registerTestActivities(env *testsuite.TestWorkflowEnvironment, simulated *activities.SimulatedActivities) {
	env.RegisterActivity(simulated.ValidateDeployment)
	env.RegisterActivity(simulated.AcquireCapacityLease)
	env.RegisterActivity(simulated.ReleaseCapacityLease)
	env.RegisterActivity(simulated.ApplyDeployment)
	env.RegisterActivity(simulated.ObserveDeployment)
	env.RegisterActivity(simulated.TriggerSavepoint)
	env.RegisterActivity(simulated.ObserveSavepoint)
	env.RegisterActivity(simulated.SetJobState)
	env.RegisterActivity(simulated.RecordAudit)
}

func workflowSpec() domain.DeploymentSpec {
	return domain.DeploymentSpec{
		ImageDigest:    "registry.example/orders@sha256:abc123",
		FlinkVersion:   "2.2",
		JobArgs:        map[string]string{"topic": "orders"},
		FlinkConfig:    map[string]string{"state.backend.type": "rocksdb"},
		Parallelism:    8,
		MaxParallelism: 128,
		Resources: domain.ResourceShape{
			TaskManagerCPU:    2,
			TaskManagerMemory: 4096,
			TaskManagerCount:  2,
			SlotsPerManager:   4,
		},
		State: domain.StateCompatibility{
			JobGraphCompatible: true,
			OperatorUIDsStable: true,
		},
	}
}
