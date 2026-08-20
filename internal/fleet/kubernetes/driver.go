// Package kubernetes manages load-test workers through a preexisting Deployment.
package kubernetes

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/marcuslin123/load-tester/internal/fleet"
)

const (
	managedLabel          = "io.github.marcuslin123.load-tester.managed"
	pollInterval          = 100 * time.Millisecond
	conflictRetryAttempts = 4
	maxReplicas           = int64(1<<31 - 1)
)

// Options identifies the preexisting worker Deployment the driver may operate.
type Options struct {
	Namespace  string
	Deployment string
}

// Driver reconciles Deployment replicas without creating or deleting infrastructure.
type Driver struct {
	mu       sync.Mutex
	client   kubernetesClient
	options  Options
	selector string
	closed   bool
	sleep    func(context.Context, time.Duration) error
}

// kubernetesClient is the narrow API surface required by the fleet driver.
type kubernetesClient interface {
	GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error)
	GetScale(ctx context.Context, namespace, name string) (*autoscalingv1.Scale, error)
	UpdateScale(ctx context.Context, namespace, name string, scale *autoscalingv1.Scale) (*autoscalingv1.Scale, error)
	ListPods(ctx context.Context, namespace, selector string) ([]corev1.Pod, error)
	DeletePod(ctx context.Context, namespace, name string, uid types.UID) error
}

func newDriver(ctx context.Context, options Options, client kubernetesClient) (*Driver, error) {
	options.Namespace = strings.TrimSpace(options.Namespace)
	options.Deployment = strings.TrimSpace(options.Deployment)
	if options.Namespace == "" {
		return nil, fmt.Errorf("Kubernetes namespace is required")
	}
	if options.Deployment == "" {
		return nil, fmt.Errorf("Kubernetes worker Deployment is required")
	}
	if client == nil {
		return nil, fmt.Errorf("Kubernetes client is required")
	}

	deployment, err := client.GetDeployment(ctx, options.Namespace, options.Deployment)
	if err != nil {
		return nil, fmt.Errorf("get worker Deployment %q: %w", options.Deployment, err)
	}
	if deployment.Labels[managedLabel] != "true" {
		return nil, fmt.Errorf("worker Deployment %q is missing ownership label %q", options.Deployment, managedLabel)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0 {
		return nil, fmt.Errorf("worker Deployment %q must start with zero replicas", options.Deployment)
	}
	selector, err := deploymentSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("worker Deployment %q has invalid selector: %w", options.Deployment, err)
	}

	return &Driver{
		client:   client,
		options:  options,
		selector: selector,
		sleep:    sleepContext,
	}, nil
}

// List reports all Deployment-selected pods, including stopped and terminating pods.
func (d *Driver) List(ctx context.Context) ([]fleet.WorkerInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	pods, err := d.listPods(ctx)
	if err != nil {
		return nil, err
	}
	workers := make([]fleet.WorkerInfo, 0, len(pods))
	for _, pod := range pods {
		workers = append(workers, fleet.WorkerInfo{
			WorkerID:   pod.Name,
			ProviderID: string(pod.UID),
			State:      normalizeState(pod),
		})
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].WorkerID < workers[j].WorkerID })
	return workers, nil
}

// Scale sets the Deployment scale subresource and waits for exactly desired running pods.
func (d *Driver) Scale(ctx context.Context, desired int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fleet.ErrClosed
	}
	if desired < 0 {
		return fmt.Errorf("desired worker count must not be negative")
	}
	if int64(desired) > maxReplicas {
		return fmt.Errorf("desired worker count exceeds Kubernetes replica range")
	}
	if err := d.setReplicas(ctx, int32(desired)); err != nil {
		return err
	}
	return d.waitRunning(ctx, desired)
}

// Replace removes one pod by its UID and waits for the Deployment to restore capacity.
func (d *Driver) Replace(ctx context.Context, workerID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fleet.ErrClosed
	}

	pods, err := d.listPods(ctx)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if pod.Name != workerID || pod.DeletionTimestamp != nil {
			continue
		}
		if err := d.client.DeletePod(ctx, d.options.Namespace, pod.Name, pod.UID); err != nil {
			if apierrors.IsNotFound(err) {
				break
			}
			return fmt.Errorf("delete worker pod %q: %w", workerID, err)
		}
		break
	}

	scale, err := d.client.GetScale(ctx, d.options.Namespace, d.options.Deployment)
	if err != nil {
		return fmt.Errorf("get worker Deployment scale: %w", err)
	}
	return d.waitRunning(ctx, int(scale.Spec.Replicas))
}

// Teardown permanently closes scaling, returns the Deployment to zero, and waits for cleanup.
func (d *Driver) Teardown(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	if err := d.setReplicas(ctx, 0); err != nil {
		return err
	}
	return d.waitRunning(ctx, 0)
}

func (d *Driver) setReplicas(ctx context.Context, desired int32) error {
	var lastErr error
	for attempt := 0; attempt < conflictRetryAttempts; attempt++ {
		scale, err := d.client.GetScale(ctx, d.options.Namespace, d.options.Deployment)
		if err != nil {
			return fmt.Errorf("get worker Deployment scale: %w", err)
		}
		if scale.Spec.Replicas == desired {
			return nil
		}
		scale.Spec.Replicas = desired
		if _, err := d.client.UpdateScale(ctx, d.options.Namespace, d.options.Deployment, scale); err == nil {
			return nil
		} else if !apierrors.IsConflict(err) {
			return fmt.Errorf("update worker Deployment scale: %w", err)
		} else {
			lastErr = err
		}
		if attempt+1 < conflictRetryAttempts {
			if err := d.sleep(ctx, time.Duration(attempt+1)*pollInterval); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("update worker Deployment scale after %d conflicts: %w", conflictRetryAttempts, lastErr)
}

func (d *Driver) waitRunning(ctx context.Context, desired int) error {
	for {
		pods, err := d.listPods(ctx)
		if err != nil {
			return err
		}
		if podsConverged(pods, desired) {
			return nil
		}
		if err := d.sleep(ctx, pollInterval); err != nil {
			return err
		}
	}
}

func (d *Driver) listPods(ctx context.Context) ([]corev1.Pod, error) {
	pods, err := d.client.ListPods(ctx, d.options.Namespace, d.selector)
	if err != nil {
		return nil, fmt.Errorf("list worker pods: %w", err)
	}
	return pods, nil
}

func deploymentSelector(selector *metav1.LabelSelector) (string, error) {
	if selector == nil {
		return "", fmt.Errorf("selector is required")
	}
	parsed, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return "", err
	}
	if parsed.Empty() {
		return "", fmt.Errorf("selector must not match every pod")
	}
	return parsed.String(), nil
}

func normalizeState(pod corev1.Pod) fleet.WorkerState {
	if pod.DeletionTimestamp != nil {
		return fleet.WorkerStateStopped
	}
	switch pod.Status.Phase {
	case corev1.PodPending:
		return fleet.WorkerStateStarting
	case corev1.PodRunning:
		return fleet.WorkerStateRunning
	case corev1.PodSucceeded, corev1.PodFailed:
		return fleet.WorkerStateStopped
	default:
		return fleet.WorkerStateUnknown
	}
}

// podsConverged ignores retained terminal history but waits for active and terminating pods to settle exactly.
func podsConverged(pods []corev1.Pod, desired int) bool {
	active := 0
	running := 0
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			return false
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		active++
		if pod.Status.Phase == corev1.PodRunning {
			running++
		}
	}
	return active == desired && running == desired
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ fleet.FleetManager = (*Driver)(nil)
