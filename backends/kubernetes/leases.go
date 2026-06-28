package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/maestro-flink/maestro/activities"
	"github.com/maestro-flink/maestro/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// leaseStore is a durable capacity-lease reservation store backed by a
// ConfigMap per node pool. Using the API server as the backing store keeps
// reservations consistent across multiple worker replicas, which an in-process
// map cannot do once the worker tier is horizontally scaled.
type leaseStore struct {
	core       kubernetes.Interface
	namespace  string
	slotBudget int
}

func newLeaseStore(core kubernetes.Interface, namespace string, slotBudget int) *leaseStore {
	if slotBudget <= 0 {
		slotBudget = 4096
	}
	return &leaseStore{core: core, namespace: namespace, slotBudget: slotBudget}
}

func leaseConfigMapName(nodePool string) string {
	return "maestro-leases-" + stringOr(nodePool, "default")
}

func (s *leaseStore) acquire(ctx context.Context, input activities.AcquireLeaseInput, leaseID string) (domain.Lease, error) {
	nodePool := stringOr(input.Identity.NodePool, "default")
	lease := domain.Lease{
		ID:            leaseID,
		NodePool:      nodePool,
		CPU:           input.Resources.TaskManagerCPU * float64(input.Resources.TaskManagerCount),
		MemoryMiB:     input.Resources.TaskManagerMemory * int64(input.Resources.TaskManagerCount),
		Slots:         input.Resources.Slots(),
		OwnerWorkflow: input.OwnerWorkflow,
		ExpiresAt:     time.Now().UTC().Add(input.TTL),
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm, leases, err := s.load(ctx, nodePool)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		reserved := 0
		for id, existing := range leases {
			if existing.ExpiresAt.Before(now) {
				delete(leases, id)
				continue
			}
			reserved += existing.Slots
		}
		if _, exists := leases[leaseID]; !exists && reserved+lease.Slots > s.slotBudget {
			return fmt.Errorf("nodepool %s slot budget exhausted (%d/%d reserved)", nodePool, reserved, s.slotBudget)
		}
		leases[leaseID] = lease
		return s.save(ctx, cm, leases)
	})
	if err != nil {
		return domain.Lease{}, err
	}
	return lease, nil
}

func (s *leaseStore) release(ctx context.Context, leaseID string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// The lease ID encodes the node pool as the prefix-free suffix is unknown
		// here, so we scan the known pools' ConfigMaps lazily by listing.
		cms, err := s.core.CoreV1().ConfigMaps(s.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/managed-by=maestro,maestro.flink/store=leases",
		})
		if err != nil {
			return err
		}
		for i := range cms.Items {
			cm := &cms.Items[i]
			if _, ok := cm.Data[leaseID]; !ok {
				continue
			}
			delete(cm.Data, leaseID)
			_, err := s.core.CoreV1().ConfigMaps(s.namespace).Update(ctx, cm, metav1.UpdateOptions{})
			return err
		}
		return nil
	})
}

func (s *leaseStore) load(ctx context.Context, nodePool string) (*corev1.ConfigMap, map[string]domain.Lease, error) {
	name := leaseConfigMapName(nodePool)
	cm, err := s.core.CoreV1().ConfigMaps(s.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: s.namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "maestro",
					"maestro.flink/store":          "leases",
					"maestro.flink/node-pool":      nodePool,
				},
			},
			Data: map[string]string{},
		}
		created, createErr := s.core.CoreV1().ConfigMaps(s.namespace).Create(ctx, cm, metav1.CreateOptions{})
		if createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return nil, nil, createErr
		}
		if createErr == nil {
			cm = created
		} else {
			cm, err = s.core.CoreV1().ConfigMaps(s.namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return nil, nil, err
			}
		}
	} else if err != nil {
		return nil, nil, err
	}

	leases := make(map[string]domain.Lease, len(cm.Data))
	for id, raw := range cm.Data {
		var lease domain.Lease
		if json.Unmarshal([]byte(raw), &lease) == nil {
			leases[id] = lease
		}
	}
	return cm, leases, nil
}

func (s *leaseStore) save(ctx context.Context, cm *corev1.ConfigMap, leases map[string]domain.Lease) error {
	data := make(map[string]string, len(leases))
	for id, lease := range leases {
		raw, err := json.Marshal(lease)
		if err != nil {
			return err
		}
		data[id] = string(raw)
	}
	cm.Data = data
	_, err := s.core.CoreV1().ConfigMaps(s.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	return err
}
