package kubernetes

import (
	"context"
	"testing"
	"time"

	"github.com/maestro-flink/maestro/activities"
	"github.com/maestro-flink/maestro/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	corefake "k8s.io/client-go/kubernetes/fake"
)

func testIdentity() domain.DeploymentIdentity {
	return domain.DeploymentIdentity{
		Environment:    "integration",
		Namespace:      "streaming",
		Name:           "orders",
		ServiceAccount: "flink-orders",
		NodePool:       "arm64",
	}
}

func testSpec() domain.DeploymentSpec {
	return domain.DeploymentSpec{
		ImageDigest:    "registry.example/orders@sha256:abc123",
		FlinkVersion:   "2.2",
		Parallelism:    8,
		MaxParallelism: 128,
		JobArgs:        map[string]string{argEntryClass: "com.example.Job", "source.topic": "orders"},
		FlinkConfig:    map[string]string{"state.backend.type": "hashmap"},
		Resources: domain.ResourceShape{
			TaskManagerCPU:    2,
			TaskManagerMemory: 4096,
			TaskManagerCount:  2,
			SlotsPerManager:   4,
		},
	}
}

func TestFlinkVersionEnum(t *testing.T) {
	cases := map[string]string{"2.2": "v2_2", "1.20": "v1_20", "v2_2": "v2_2", "": "v2_2"}
	for in, want := range cases {
		if got := flinkVersionEnum(in); got != want {
			t.Errorf("flinkVersionEnum(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestUpgradeMode(t *testing.T) {
	prev := &domain.DeploymentVersion{}
	cases := []struct {
		name string
		in   activities.ApplyDeploymentInput
		want string
	}{
		{"fresh", activities.ApplyDeploymentInput{}, "stateless"},
		{"stateful", activities.ApplyDeploymentInput{Previous: prev}, "savepoint"},
		{"incident", activities.ApplyDeploymentInput{Previous: prev, Incident: true}, "last-state"},
		{"incident-gitops", activities.ApplyDeploymentInput{Previous: prev, Incident: true, GitOpsOnly: true}, "savepoint"},
	}
	for _, c := range cases {
		if got := upgradeMode(c.in); got != c.want {
			t.Errorf("%s: upgradeMode = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestBuildFlinkDeployment(t *testing.T) {
	version := domain.BuildVersion(3, testSpec())
	obj := buildFlinkDeployment(activities.ApplyDeploymentInput{
		Identity:     testIdentity(),
		Version:      version,
		SavepointURI: "s3://savepoints/orders/v2",
		Previous:     &domain.DeploymentVersion{},
	}, baseFlinkDefaults, "")

	assertNestedString(t, obj, "v2_2", "spec", "flinkVersion")
	assertNestedString(t, obj, "registry.example/orders@sha256:abc123", "spec", "image")
	assertNestedString(t, obj, "flink-orders", "spec", "serviceAccount")
	assertNestedString(t, obj, "savepoint", "spec", "job", "upgradeMode")
	assertNestedString(t, obj, "s3://savepoints/orders/v2", "spec", "job", "initialSavepointPath")
	assertNestedString(t, obj, "com.example.Job", "spec", "job", "entryClass")

	parallelism, _, _ := unstructured.NestedInt64(obj.Object, "spec", "job", "parallelism")
	if parallelism != 8 {
		t.Errorf("parallelism = %d, want 8", parallelism)
	}
	// deployment-supplied flink config wins over defaults
	assertNestedString(t, obj, "hashmap", "spec", "flinkConfiguration", "state.backend.type")
	// slots derived from resource shape
	assertNestedString(t, obj, "4", "spec", "flinkConfiguration", "taskmanager.numberOfTaskSlots")
	// max-parallelism propagated
	assertNestedString(t, obj, "128", "spec", "flinkConfiguration", "pipeline.max-parallelism")

	args, _, _ := unstructured.NestedSlice(obj.Object, "spec", "job", "args")
	if len(args) != 2 || args[0] != "--source.topic" || args[1] != "orders" {
		t.Errorf("program args = %v, want [--source.topic orders]", args)
	}
}

func TestObservedHealth(t *testing.T) {
	ready := newFlinkDeploymentWithStatus("READY", "RUNNING", "DEPLOYED", "")
	summary, settled := observedHealth(ready)
	if !summary.Healthy || !settled {
		t.Errorf("ready deployment: healthy=%v settled=%v, want both true", summary.Healthy, settled)
	}

	deploying := newFlinkDeploymentWithStatus("DEPLOYING", "RECONCILING", "UPGRADING", "")
	summary, settled = observedHealth(deploying)
	if summary.Healthy || settled {
		t.Errorf("deploying: healthy=%v settled=%v, want both false", summary.Healthy, settled)
	}

	failed := newFlinkDeploymentWithStatus("ERROR", "RECONCILING", "UPGRADING", "image pull failed")
	summary, settled = observedHealth(failed)
	if summary.Healthy || !settled {
		t.Errorf("failed: healthy=%v settled=%v, want healthy=false settled=true", summary.Healthy, settled)
	}
	if summary.Message != "image pull failed" {
		t.Errorf("failed message = %q", summary.Message)
	}
}

func TestSetJobState(t *testing.T) {
	identity := testIdentity()
	seed := buildFlinkDeployment(activities.ApplyDeploymentInput{Identity: identity, Version: domain.BuildVersion(1, testSpec())}, baseFlinkDefaults, "")
	backend := newTestBackend(t, []runtime.Object{seed}, nil)

	if err := backend.SetJobState(context.Background(), activities.SetJobStateInput{Identity: identity, State: "SUSPENDED"}); err != nil {
		t.Fatalf("SetJobState: %v", err)
	}
	got, err := backend.dyn.Resource(flinkDeploymentGVR).Namespace(identity.Namespace).Get(context.Background(), identity.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after patch: %v", err)
	}
	assertNestedString(t, got, "suspended", "spec", "job", "state")
}

func TestObserveSavepointCompleted(t *testing.T) {
	identity := testIdentity()
	snap := newStateSnapshot("orders-sp-1", identity.Namespace, "COMPLETED", "s3://savepoints/orders/sp-1", "")
	backend := newTestBackend(t, []runtime.Object{snap}, nil)

	record, err := backend.ObserveSavepoint(context.Background(), activities.ObserveSavepointInput{
		Identity: identity,
		Version:  domain.BuildVersion(2, testSpec()),
		Trigger:  activities.SavepointTrigger{TriggerID: "orders-sp-1", FlinkJobID: "job-orders"},
	})
	if err != nil {
		t.Fatalf("ObserveSavepoint: %v", err)
	}
	if record.URI != "s3://savepoints/orders/sp-1" {
		t.Errorf("savepoint URI = %q", record.URI)
	}
}

func TestLeaseStoreBudget(t *testing.T) {
	core := corefake.NewSimpleClientset()
	store := newLeaseStore(core, "maestro-system", 10)
	ctx := context.Background()
	input := activities.AcquireLeaseInput{
		Identity:  testIdentity(),
		Resources: domain.ResourceShape{TaskManagerCount: 2, SlotsPerManager: 3}, // 6 slots
		TTL:       time.Minute,
	}

	first, err := store.acquire(ctx, input, "lease-a")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := store.acquire(ctx, input, "lease-b"); err == nil {
		t.Fatal("second acquire should exceed the 10-slot budget")
	}
	if err := store.release(ctx, first.ID); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := store.acquire(ctx, input, "lease-b"); err != nil {
		t.Fatalf("acquire after release should succeed: %v", err)
	}
}

// --- test helpers ---

func newTestBackend(t *testing.T, dynObjs []runtime.Object, _ []runtime.Object) *Backend {
	t.Helper()
	scheme := runtime.NewScheme()
	gvrToListKind := map[schema.GroupVersionResource]string{
		flinkDeploymentGVR:    "FlinkDeploymentList",
		flinkStateSnapshotGVR: "FlinkStateSnapshotList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, dynObjs...)
	core := corefake.NewSimpleClientset()
	return newWithClients(dyn, core, Config{PollInterval: time.Millisecond, ObserveTimeout: time.Second, SavepointTimeout: time.Second})
}

func newFlinkDeploymentWithStatus(jmStatus, jobState, reconcile, errText string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind":       "FlinkDeployment",
		"metadata":   map[string]interface{}{"name": "orders", "namespace": "streaming"},
		"status": map[string]interface{}{
			"jobManagerDeploymentStatus": jmStatus,
			"jobStatus":                  map[string]interface{}{"state": jobState},
			"reconciliationStatus":       map[string]interface{}{"state": reconcile},
			"error":                      errText,
		},
	}}
}

func newStateSnapshot(name, namespace, state, path, errText string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind":       "FlinkStateSnapshot",
		"metadata":   map[string]interface{}{"name": name, "namespace": namespace},
		"status": map[string]interface{}{
			"state": state,
			"path":  path,
			"error": errText,
		},
	}}
}

func assertNestedString(t *testing.T, obj *unstructured.Unstructured, want string, fields ...string) {
	t.Helper()
	got, found, err := unstructured.NestedString(obj.Object, fields...)
	if err != nil || !found {
		t.Fatalf("field %v not found (err=%v)", fields, err)
	}
	if got != want {
		t.Errorf("field %v = %q, want %q", fields, got, want)
	}
}
