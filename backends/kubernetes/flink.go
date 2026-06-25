package kubernetes

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/flink-control-plane/fcp/activities"
	"github.com/flink-control-plane/fcp/domain"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Group/version/resources for the Apache Flink Kubernetes Operator (>= 1.15).
var (
	flinkDeploymentGVR = schema.GroupVersionResource{
		Group:    "flink.apache.org",
		Version:  "v1beta1",
		Resource: "flinkdeployments",
	}
	flinkStateSnapshotGVR = schema.GroupVersionResource{
		Group:    "flink.apache.org",
		Version:  "v1beta1",
		Resource: "flinkstatesnapshots",
	}
)

// Reserved JobArgs keys that configure the FlinkDeployment job spec rather than
// being passed through as program arguments.
const (
	argJarURI     = "fcp.jarURI"
	argEntryClass = "fcp.entryClass"
	defaultJarURI = "local:///opt/flink/usrlib/job.jar"
)

// flinkVersionEnum converts a semantic Flink version such as "2.2" into the
// operator's enum form "v2_2".
func flinkVersionEnum(version string) string {
	v := strings.TrimSpace(version)
	if v == "" {
		return "v2_2"
	}
	if strings.HasPrefix(v, "v") {
		return v
	}
	return "v" + strings.ReplaceAll(v, ".", "_")
}

// upgradeMode selects the operator upgrade strategy for an apply.
//   - no previous version              -> stateless (fresh start)
//   - incident direct-patch            -> last-state (fastest, claims local state)
//   - normal stateful redeploy         -> savepoint (take a savepoint, restore from it)
func upgradeMode(input activities.ApplyDeploymentInput) string {
	switch {
	case input.Previous == nil:
		return "stateless"
	case input.Incident && !input.GitOpsOnly:
		return "last-state"
	default:
		return "savepoint"
	}
}

// buildFlinkDeployment renders a FlinkDeployment custom resource from a control
// plane apply request. The result is suitable for a server-side apply. When
// localStoragePath is set, a hostPath volume is mounted into every Flink pod and
// HA / checkpoint / savepoint directories are pointed under it.
func buildFlinkDeployment(input activities.ApplyDeploymentInput, defaults map[string]string, localStoragePath string) *unstructured.Unstructured {
	spec := input.Version.Spec

	flinkConfig := map[string]string{}
	for k, v := range defaults {
		flinkConfig[k] = v
	}
	flinkConfig["pipeline.max-parallelism"] = strconv.Itoa(spec.MaxParallelism)
	flinkConfig["taskmanager.numberOfTaskSlots"] = strconv.Itoa(maxInt(spec.Resources.SlotsPerManager, 1))
	if localStoragePath != "" {
		setIfAbsent(flinkConfig, "high-availability.type", "kubernetes")
		setIfAbsent(flinkConfig, "high-availability.storageDir", "file:///flink-data/ha")
		setIfAbsent(flinkConfig, "state.checkpoints.dir", "file:///flink-data/checkpoints")
		setIfAbsent(flinkConfig, "state.savepoints.dir", "file:///flink-data/savepoints")
	}
	for k, v := range spec.FlinkConfig {
		flinkConfig[k] = v
	}

	job := map[string]interface{}{
		"jarURI":      stringOr(spec.JobArgs[argJarURI], defaultJarURI),
		"parallelism": int64(spec.Parallelism),
		"upgradeMode": upgradeMode(input),
		"state":       "running",
	}
	if entry := spec.JobArgs[argEntryClass]; entry != "" {
		job["entryClass"] = entry
	}
	if args := programArgs(spec.JobArgs); len(args) > 0 {
		job["args"] = args
	}
	if input.SavepointURI != "" {
		job["initialSavepointPath"] = input.SavepointURI
	}

	deployment := map[string]interface{}{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind":       "FlinkDeployment",
		"metadata": map[string]interface{}{
			"name":      input.Identity.Name,
			"namespace": input.Identity.Namespace,
			"labels": map[string]interface{}{
				"app.kubernetes.io/managed-by": "fcp",
				"fcp.flink/environment":        input.Identity.Environment,
				"fcp.flink/node-pool":          stringOr(input.Identity.NodePool, "default"),
			},
			"annotations": map[string]interface{}{
				"fcp.flink/version-id": strconv.FormatInt(input.Version.VersionID, 10),
			},
		},
		"spec": map[string]interface{}{
			"image":              spec.ImageDigest,
			"flinkVersion":       flinkVersionEnum(spec.FlinkVersion),
			"flinkConfiguration": toStringMap(flinkConfig),
			"serviceAccount":     stringOr(input.Identity.ServiceAccount, "flink"),
			"jobManager": map[string]interface{}{
				"resource": map[string]interface{}{
					"memory": "2048m",
					"cpu":    float64(1),
				},
			},
			"taskManager": map[string]interface{}{
				"replicas": int64(maxInt(spec.Resources.TaskManagerCount, 1)),
				"resource": map[string]interface{}{
					"memory": fmt.Sprintf("%dm", maxInt64(spec.Resources.TaskManagerMemory, 1024)),
					"cpu":    spec.Resources.TaskManagerCPU,
				},
			},
			"job": job,
		},
	}

	if localStoragePath != "" {
		mount := map[string]interface{}{"name": "flink-data", "mountPath": "/flink-data"}
		deployment["spec"].(map[string]interface{})["podTemplate"] = map[string]interface{}{
			"spec": map[string]interface{}{
				// kubelet creates the hostPath root-owned; make it writable for the
				// non-root Flink user. The job image is reused so there is no extra pull.
				"initContainers": []interface{}{
					map[string]interface{}{
						"name":         "fix-storage-perms",
						"image":        spec.ImageDigest,
						"command":      []interface{}{"sh", "-c", "mkdir -p /flink-data && chmod -R 777 /flink-data"},
						"volumeMounts": []interface{}{mount},
						"securityContext": map[string]interface{}{
							"runAsUser": int64(0),
						},
					},
				},
				"containers": []interface{}{
					map[string]interface{}{
						"name":         "flink-main-container",
						"volumeMounts": []interface{}{mount},
					},
				},
				"volumes": []interface{}{
					map[string]interface{}{
						"name":     "flink-data",
						"hostPath": map[string]interface{}{"path": localStoragePath, "type": "DirectoryOrCreate"},
					},
				},
			},
		}
	}
	return &unstructured.Unstructured{Object: deployment}
}

// setIfAbsent sets key=value only when the key is not already present.
func setIfAbsent(m map[string]string, key, value string) {
	if _, ok := m[key]; !ok {
		m[key] = value
	}
}

// programArgs renders non-reserved JobArgs as deterministic --key value pairs.
func programArgs(jobArgs map[string]string) []interface{} {
	keys := make([]string, 0, len(jobArgs))
	for k := range jobArgs {
		if k == argJarURI || k == argEntryClass {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	args := make([]interface{}, 0, len(keys)*2)
	for _, k := range keys {
		args = append(args, "--"+k, jobArgs[k])
	}
	return args
}

// buildStateSnapshot renders a FlinkStateSnapshot CR that triggers a savepoint
// of the named FlinkDeployment.
func buildStateSnapshot(name string, identity domain.DeploymentIdentity) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind":       "FlinkStateSnapshot",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": identity.Namespace,
			"labels": map[string]interface{}{
				"app.kubernetes.io/managed-by": "fcp",
			},
		},
		"spec": map[string]interface{}{
			"backoffLimit": int64(2),
			"jobReference": map[string]interface{}{
				"kind": "FlinkDeployment",
				"name": identity.Name,
			},
			"savepoint": map[string]interface{}{
				"disposeOnDelete": false,
				"formatType":      "CANONICAL",
			},
		},
	}}
}

// observedHealth maps a FlinkDeployment status to a control plane health summary.
func observedHealth(obj *unstructured.Unstructured) (domain.HealthSummary, bool) {
	jmStatus, _, _ := unstructured.NestedString(obj.Object, "status", "jobManagerDeploymentStatus")
	jobState, _, _ := unstructured.NestedString(obj.Object, "status", "jobStatus", "state")
	reconcileState, _, _ := unstructured.NestedString(obj.Object, "status", "reconciliationStatus", "state")
	errText, _, _ := unstructured.NestedString(obj.Object, "status", "error")

	running := strings.EqualFold(jobState, "RUNNING")
	jmReady := strings.EqualFold(jmStatus, "READY")
	healthy := running && jmReady && errText == ""

	summary := domain.HealthSummary{
		Healthy:             healthy,
		Running:             running,
		CheckpointCompleted: healthy,
		SinkHealthy:         healthy,
		Message:             errText,
	}

	// "settled" reports whether the deployment has reached a state from which no
	// further polling will change the health verdict.
	failed := strings.EqualFold(jmStatus, "ERROR") ||
		strings.EqualFold(jmStatus, "MISSING") ||
		strings.EqualFold(jobState, "FAILED") ||
		strings.EqualFold(reconcileState, "ROLLED_BACK") ||
		errText != ""
	settled := healthy || failed
	return summary, settled
}

func toStringMap(in map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func stringOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
