// Package kubernetes implements the Maestro activities.Backend contract against a
// real Apache Flink Kubernetes Operator (>= 1.15) by reconciling FlinkDeployment
// and FlinkStateSnapshot custom resources. It is a separate Go module so that
// the public Maestro core library does not take a client-go dependency.
package kubernetes

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/maestro-flink/maestro/activities"
	"github.com/maestro-flink/maestro/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const fieldManager = "maestro"

// Config tunes the Kubernetes backend.
type Config struct {
	// LeaseNamespace is where capacity-lease ConfigMaps are stored. It should be
	// a namespace the worker can write to (typically the Maestro system namespace).
	LeaseNamespace string
	// SlotBudget caps total reserved task slots per node pool.
	SlotBudget int
	// WatchNamespaces restricts informer caches (and therefore the RBAC the worker
	// needs) to these namespaces. Empty means cluster-wide, which requires
	// cluster-scoped RBAC.
	WatchNamespaces []string
	// LocalStoragePath, when set, mounts a hostPath at that location into every
	// Flink pod and points HA / checkpoint / savepoint directories under it. This
	// gives a single-node cluster (e.g. kind) shared, restart-surviving storage so
	// savepoint-based upgrades work without an object store. Leave empty in
	// production and supply s3:// (etc.) directories via the deployment FlinkConfig.
	LocalStoragePath string
	// DefaultFlinkConfig is merged under every FlinkDeployment's flinkConfiguration
	// (deployment-specified config wins on conflict).
	DefaultFlinkConfig map[string]string
	// ObserveTimeout bounds how long ObserveDeployment waits for a deployment to
	// reach a settled (healthy or failed) state before returning its last reading.
	ObserveTimeout time.Duration
	// SavepointTimeout bounds how long ObserveSavepoint waits for completion.
	SavepointTimeout time.Duration
	// PollInterval is the wait between status reads while polling.
	PollInterval time.Duration
	// ResyncPeriod is the informer cache resync period.
	ResyncPeriod time.Duration
}

func (c *Config) withDefaults() {
	if c.LeaseNamespace == "" {
		c.LeaseNamespace = "maestro-system"
	}
	if c.SlotBudget <= 0 {
		c.SlotBudget = 4096
	}
	if c.ObserveTimeout <= 0 {
		c.ObserveTimeout = 5 * time.Minute
	}
	if c.SavepointTimeout <= 0 {
		c.SavepointTimeout = 10 * time.Minute
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 3 * time.Second
	}
	if c.ResyncPeriod <= 0 {
		c.ResyncPeriod = 30 * time.Second
	}
}

// baseFlinkDefaults are infrastructure-free: they do not assume any persistent
// volume or object store, so a vanilla deployment runs out of the box.
// Durable checkpoint/savepoint/HA directories must be supplied either through
// the deployment's FlinkConfig (e.g. an s3:// path on EKS) or via
// Config.LocalStoragePath (a hostPath for single-node clusters).
var baseFlinkDefaults = map[string]string{
	"state.backend.type":               "hashmap",
	"execution.checkpointing.interval": "10s",
}

// Backend implements activities.Backend against the Flink Kubernetes Operator.
type Backend struct {
	dyn       dynamic.Interface
	core      kubernetes.Interface
	factories []dynamicinformer.DynamicSharedInformerFactory
	// listers maps a watched namespace to its FlinkDeployment lister. The key ""
	// holds a cluster-wide lister when no namespaces are configured.
	listers  map[string]cache.GenericLister
	leases   *leaseStore
	cfg      Config
	defaults map[string]string
}

var _ activities.Backend = (*Backend)(nil)

// New builds a Backend from a Kubernetes REST config.
func New(restConfig *rest.Config, cfg Config) (*Backend, error) {
	cfg.withDefaults()
	dyn, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	core, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("core client: %w", err)
	}
	return newWithClients(dyn, core, cfg), nil
}

func newWithClients(dyn dynamic.Interface, core kubernetes.Interface, cfg Config) *Backend {
	cfg.withDefaults()
	defaults := map[string]string{}
	for k, v := range baseFlinkDefaults {
		defaults[k] = v
	}
	for k, v := range cfg.DefaultFlinkConfig {
		defaults[k] = v
	}
	listers := map[string]cache.GenericLister{}
	var factories []dynamicinformer.DynamicSharedInformerFactory
	if len(cfg.WatchNamespaces) == 0 {
		factory := dynamicinformer.NewDynamicSharedInformerFactory(dyn, cfg.ResyncPeriod)
		listers[""] = factory.ForResource(flinkDeploymentGVR).Lister()
		factories = append(factories, factory)
	} else {
		for _, ns := range cfg.WatchNamespaces {
			factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, cfg.ResyncPeriod, ns, nil)
			listers[ns] = factory.ForResource(flinkDeploymentGVR).Lister()
			factories = append(factories, factory)
		}
	}
	return &Backend{
		dyn:       dyn,
		core:      core,
		factories: factories,
		listers:   listers,
		leases:    newLeaseStore(core, cfg.LeaseNamespace, cfg.SlotBudget),
		cfg:       cfg,
		defaults:  defaults,
	}
}

// Start begins informer caching and blocks until the FlinkDeployment cache is
// synced. Call it once before serving activities. The informer-backed reads keep
// ObserveDeployment off the API server hot path, which matters at 10k jobs.
func (b *Backend) Start(ctx context.Context) error {
	for _, factory := range b.factories {
		factory.Start(ctx.Done())
		for gvr, synced := range factory.WaitForCacheSync(ctx.Done()) {
			if !synced {
				return fmt.Errorf("informer cache for %s did not sync", gvr)
			}
		}
	}
	return nil
}

func (b *Backend) ValidateDeployment(_ context.Context, input activities.ValidateDeploymentInput) error {
	return activities.ValidateSpec(input)
}

func (b *Backend) AcquireCapacityLease(ctx context.Context, input activities.AcquireLeaseInput) (domain.Lease, error) {
	leaseID := "lease-" + stringOr(input.Identity.NodePool, "default") + "-" + randID()
	return b.leases.acquire(ctx, input, leaseID)
}

func (b *Backend) ReleaseCapacityLease(ctx context.Context, leaseID string) error {
	return b.leases.release(ctx, leaseID)
}

func (b *Backend) ApplyDeployment(ctx context.Context, input activities.ApplyDeploymentInput) (activities.ApplyDeploymentResult, error) {
	desired := buildFlinkDeployment(input, b.defaults, b.cfg.LocalStoragePath)
	applied, err := b.dyn.Resource(flinkDeploymentGVR).
		Namespace(input.Identity.Namespace).
		Apply(ctx, input.Identity.Name, desired, metav1.ApplyOptions{FieldManager: fieldManager, Force: true})
	if err != nil {
		return activities.ApplyDeploymentResult{}, fmt.Errorf("apply FlinkDeployment %s/%s: %w", input.Identity.Namespace, input.Identity.Name, err)
	}
	generation, _, _ := unstructured.NestedInt64(applied.Object, "metadata", "generation")
	mode := "gitops"
	if input.Incident && !input.GitOpsOnly {
		mode = "direct-patch"
	}
	return activities.ApplyDeploymentResult{
		ObservedGeneration: generation,
		ApplyReference:     fmt.Sprintf("%s://%s/%s@v%d", mode, input.Identity.Namespace, input.Identity.Name, input.Version.VersionID),
	}, nil
}

func (b *Backend) ObserveDeployment(ctx context.Context, input activities.ObserveDeploymentInput) (domain.HealthSummary, error) {
	deadline := time.Now().Add(b.cfg.ObserveTimeout)
	ticker := time.NewTicker(b.cfg.PollInterval)
	defer ticker.Stop()

	var last domain.HealthSummary
	for {
		obj, err := b.getFlinkDeployment(ctx, input.Identity.Namespace, input.Identity.Name)
		if err == nil {
			summary, settled := observedHealth(obj)
			summary.ObservedAt = time.Now().UTC()
			last = summary
			if settled {
				return summary, nil
			}
		} else if !apierrors.IsNotFound(err) {
			return domain.HealthSummary{}, err
		}

		if time.Now().After(deadline) {
			if last.ObservedAt.IsZero() {
				last.ObservedAt = time.Now().UTC()
				last.Message = "deployment did not report status before observe timeout"
			}
			return last, nil
		}
		select {
		case <-ctx.Done():
			return domain.HealthSummary{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *Backend) TriggerSavepoint(ctx context.Context, input activities.TriggerSavepointInput) (activities.SavepointTrigger, error) {
	name := input.Identity.Name + "-sp-" + randID()
	snapshot := buildStateSnapshot(name, input.Identity)
	if _, err := b.dyn.Resource(flinkStateSnapshotGVR).
		Namespace(input.Identity.Namespace).
		Create(ctx, snapshot, metav1.CreateOptions{FieldManager: fieldManager}); err != nil && !apierrors.IsAlreadyExists(err) {
		return activities.SavepointTrigger{}, fmt.Errorf("create FlinkStateSnapshot %s: %w", name, err)
	}
	flinkJobID := "job-" + input.Identity.Name
	if obj, err := b.getFlinkDeployment(ctx, input.Identity.Namespace, input.Identity.Name); err == nil {
		if id, ok, _ := unstructured.NestedString(obj.Object, "status", "jobStatus", "jobId"); ok && id != "" {
			flinkJobID = id
		}
	}
	return activities.SavepointTrigger{TriggerID: name, FlinkJobID: flinkJobID}, nil
}

func (b *Backend) ObserveSavepoint(ctx context.Context, input activities.ObserveSavepointInput) (domain.SavepointRecord, error) {
	deadline := time.Now().Add(b.cfg.SavepointTimeout)
	ticker := time.NewTicker(b.cfg.PollInterval)
	defer ticker.Stop()

	for {
		obj, err := b.dyn.Resource(flinkStateSnapshotGVR).
			Namespace(input.Identity.Namespace).
			Get(ctx, input.Trigger.TriggerID, metav1.GetOptions{})
		if err == nil {
			state, _, _ := unstructured.NestedString(obj.Object, "status", "state")
			switch strings.ToUpper(state) {
			case "COMPLETED":
				path, _, _ := unstructured.NestedString(obj.Object, "status", "path")
				return b.savepointRecord(input, path), nil
			case "FAILED", "ABANDONED":
				msg, _, _ := unstructured.NestedString(obj.Object, "status", "error")
				return domain.SavepointRecord{}, fmt.Errorf("savepoint %s %s: %s", input.Trigger.TriggerID, state, msg)
			}
		} else if !apierrors.IsNotFound(err) {
			return domain.SavepointRecord{}, err
		}

		if time.Now().After(deadline) {
			return domain.SavepointRecord{}, fmt.Errorf("savepoint %s did not complete before timeout", input.Trigger.TriggerID)
		}
		select {
		case <-ctx.Done():
			return domain.SavepointRecord{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *Backend) savepointRecord(input activities.ObserveSavepointInput, path string) domain.SavepointRecord {
	if path == "" {
		path = fmt.Sprintf("unknown://%s/%s", input.Identity.Name, input.Trigger.TriggerID)
	}
	return domain.SavepointRecord{
		URI:               path,
		TriggerID:         input.Trigger.TriggerID,
		FlinkJobID:        input.Trigger.FlinkJobID,
		DeploymentVersion: input.Version.VersionID,
		ImageDigest:       input.Version.Spec.ImageDigest,
		JobArgsHash:       input.Version.JobArgsHash,
		Parallelism:       input.Version.Spec.Parallelism,
		MaxParallelism:    input.Version.Spec.MaxParallelism,
		CreatedAt:         time.Now().UTC(),
	}
}

func (b *Backend) SetJobState(ctx context.Context, input activities.SetJobStateInput) error {
	desired := "running"
	if strings.EqualFold(input.State, "SUSPENDED") {
		desired = "suspended"
	}
	patch := []byte(fmt.Sprintf(`{"spec":{"job":{"state":%q}}}`, desired))
	_, err := b.dyn.Resource(flinkDeploymentGVR).
		Namespace(input.Identity.Namespace).
		Patch(ctx, input.Identity.Name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: fieldManager})
	if err != nil {
		return fmt.Errorf("patch job state %s/%s: %w", input.Identity.Namespace, input.Identity.Name, err)
	}
	return nil
}

func (b *Backend) RecordAudit(ctx context.Context, event activities.AuditEvent) error {
	slog.InfoContext(ctx, "control-plane audit",
		"operationId", event.OperationID,
		"type", event.Type,
		"deployment", event.Identity.WorkflowID(),
		"message", event.Message,
	)
	// Best-effort Kubernetes Event so operators can correlate from kubectl.
	now := metav1.NewTime(event.At)
	k8sEvent := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "maestro-",
			Namespace:    event.Identity.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:       "FlinkDeployment",
			Namespace:  event.Identity.Namespace,
			Name:       event.Identity.Name,
			APIVersion: "flink.apache.org/v1beta1",
		},
		Reason:         event.Type,
		Message:        event.Message,
		Type:           corev1.EventTypeNormal,
		Source:         corev1.EventSource{Component: "maestro"},
		FirstTimestamp: now,
		LastTimestamp:  now,
	}
	if _, err := b.core.CoreV1().Events(event.Identity.Namespace).Create(ctx, k8sEvent, metav1.CreateOptions{}); err != nil {
		slog.WarnContext(ctx, "failed to record kubernetes event", "error", err)
	}
	return nil
}

// getFlinkDeployment reads from the informer cache, falling back to a live read
// when the object is not yet cached.
func (b *Backend) getFlinkDeployment(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	lister := b.listers[namespace]
	if lister == nil {
		lister = b.listers[""]
	}
	if lister != nil {
		if obj, err := lister.ByNamespace(namespace).Get(name); err == nil {
			if u, ok := obj.(*unstructured.Unstructured); ok {
				return u, nil
			}
		}
	}
	return b.dyn.Resource(flinkDeploymentGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
}


func randID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
