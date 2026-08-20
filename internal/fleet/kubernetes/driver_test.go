package kubernetes

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/marcuslin123/load-tester/internal/fleet"
)

const managedDeploymentLabel = "io.github.marcuslin123.load-tester.managed"

func TestNewDriverValidatesExistingWorkerDeployment(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		mutate  func(*fakeClient)
		want    string
	}{
		{name: "namespace", options: Options{Deployment: "workers"}, want: "namespace"},
		{name: "deployment", options: Options{Namespace: "load-test"}, want: "Deployment"},
		{
			name:    "get deployment failure",
			options: validOptions(),
			mutate:  func(client *fakeClient) { client.getDeploymentErr = errors.New("forbidden") },
			want:    "get worker Deployment",
		},
		{
			name:    "missing ownership label",
			options: validOptions(),
			mutate: func(client *fakeClient) {
				client.deployment.Labels = nil
			},
			want: "ownership label",
		},
		{
			name:    "nonzero initial replicas",
			options: validOptions(),
			mutate: func(client *fakeClient) {
				client.deployment.Spec.Replicas = int32ptr(1)
			},
			want: "zero replicas",
		},
		{
			name:    "missing selector",
			options: validOptions(),
			mutate: func(client *fakeClient) {
				client.deployment.Spec.Selector = nil
			},
			want: "selector",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			if test.mutate != nil {
				test.mutate(client)
			}
			_, err := newDriver(context.Background(), test.options, client)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("newDriver() error = %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestListMapsPodStatesAndSortsByWorkerID(t *testing.T) {
	client := newFakeClient()
	client.pods = []corev1.Pod{
		pod("worker-z", "uid-z", corev1.PodPending),
		pod("worker-a", "uid-a", corev1.PodRunning),
		pod("worker-c", "uid-c", corev1.PodSucceeded),
		pod("worker-b", "uid-b", corev1.PodFailed),
		pod("worker-d", "uid-d", corev1.PodUnknown),
		terminatingPod("worker-e", "uid-e", corev1.PodRunning),
	}
	driver := mustNewDriver(t, client)

	workers, err := driver.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	want := []fleet.WorkerInfo{
		{WorkerID: "worker-a", ProviderID: "uid-a", State: fleet.WorkerStateRunning},
		{WorkerID: "worker-b", ProviderID: "uid-b", State: fleet.WorkerStateStopped},
		{WorkerID: "worker-c", ProviderID: "uid-c", State: fleet.WorkerStateStopped},
		{WorkerID: "worker-d", ProviderID: "uid-d", State: fleet.WorkerStateUnknown},
		{WorkerID: "worker-e", ProviderID: "uid-e", State: fleet.WorkerStateStopped},
		{WorkerID: "worker-z", ProviderID: "uid-z", State: fleet.WorkerStateStarting},
	}
	if !reflect.DeepEqual(workers, want) {
		t.Fatalf("List() = %#v, want %#v", workers, want)
	}
	if got := client.lastSelector(); got != "app=worker" {
		t.Fatalf("pod selector = %q, want deployment selector", got)
	}
}

func TestScaleUpdatesOnlyTheSubresourceAndConvergesExactly(t *testing.T) {
	client := newFakeClient()
	driver := mustNewDriver(t, client)

	if err := driver.Scale(context.Background(), 2); err != nil {
		t.Fatalf("Scale(2) error = %v", err)
	}
	if got := client.updatedReplicas(); !reflect.DeepEqual(got, []int32{2}) {
		t.Fatalf("updated replicas = %v, want [2]", got)
	}
	if got := runningPods(client.podSnapshot()); got != 2 {
		t.Fatalf("running pods = %d, want 2", got)
	}

	if err := driver.Scale(context.Background(), 2); err != nil {
		t.Fatalf("repeated Scale(2) error = %v", err)
	}
	if got := client.updatedReplicas(); !reflect.DeepEqual(got, []int32{2}) {
		t.Fatalf("updates after repeated Scale(2) = %v, want no further update", got)
	}
}

func TestScaleRejectsNegativeDesiredCount(t *testing.T) {
	driver := mustNewDriver(t, newFakeClient())
	if err := driver.Scale(context.Background(), -1); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Scale(-1) error = %v, want negative desired count error", err)
	}
}

func TestScaleRejectsReplicaCountAboveKubernetesRange(t *testing.T) {
	driver := mustNewDriver(t, newFakeClient())
	err := driver.Scale(context.Background(), int64ToInt(t, int64(math.MaxInt32)+1))
	if err == nil || !strings.Contains(err.Error(), "Kubernetes replica range") {
		t.Fatalf("Scale() error = %v, want replica range error", err)
	}
}

func TestScaleRetriesScaleConflicts(t *testing.T) {
	client := newFakeClient()
	client.conflictsRemaining = 2
	driver := mustNewDriver(t, client)
	driver.sleep = func(context.Context, time.Duration) error { return nil }

	if err := driver.Scale(context.Background(), 1); err != nil {
		t.Fatalf("Scale(1) error = %v", err)
	}
	if got := client.updateAttempts(); got != 3 {
		t.Fatalf("scale update attempts = %d, want 3 after two conflicts", got)
	}
}

func TestScaleHonorsContextWhileWaitingForExactRunningCount(t *testing.T) {
	client := newFakeClient()
	client.onUpdate = func(*fakeClient, int32) {}
	driver := mustNewDriver(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	driver.sleep = func(context.Context, time.Duration) error {
		cancel()
		return ctx.Err()
	}

	err := driver.Scale(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Scale() error = %v, want context cancellation", err)
	}
}

func TestScaleRequiresExactRunningCount(t *testing.T) {
	client := newFakeClient()
	client.onUpdate = func(client *fakeClient, replicas int32) {
		client.pods = []corev1.Pod{
			pod("worker-1", "uid-1", corev1.PodRunning),
			pod("worker-2", "uid-2", corev1.PodRunning),
		}
	}
	driver := mustNewDriver(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	driver.sleep = func(context.Context, time.Duration) error {
		cancel()
		return ctx.Err()
	}

	err := driver.Scale(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Scale() error = %v, want cancellation instead of accepting excess pods", err)
	}
}

func TestScaleWaitsForTerminatingPodsToDisappear(t *testing.T) {
	client := newFakeClient()
	client.onUpdate = func(client *fakeClient, replicas int32) {
		client.pods = []corev1.Pod{
			pod("worker-1", "uid-1", corev1.PodRunning),
			terminatingPod("worker-old", "uid-old", corev1.PodRunning),
		}
	}
	driver := mustNewDriver(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	driver.sleep = func(context.Context, time.Duration) error {
		cancel()
		return ctx.Err()
	}

	err := driver.Scale(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Scale() error = %v, want cancellation while terminating pod remains", err)
	}
}

func TestScaleIgnoresRetainedTerminalPodsAfterReplacement(t *testing.T) {
	client := newFakeClient()
	client.onUpdate = func(client *fakeClient, replicas int32) {
		client.pods = []corev1.Pod{
			pod("worker-running", "uid-running", corev1.PodRunning),
			pod("worker-failed", "uid-failed", corev1.PodFailed),
		}
	}
	driver := mustNewDriver(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	if err := driver.Scale(ctx, 1); err != nil {
		t.Fatalf("Scale(1) error = %v, want retained terminal pod to be ignored", err)
	}
}

func TestReplaceUsesPodUIDPreconditionAndWaitsForReplacement(t *testing.T) {
	client := newFakeClient()
	client.scale.Spec.Replicas = 1
	client.pods = []corev1.Pod{pod("worker-old", "uid-old", corev1.PodRunning)}
	driver := mustNewDriver(t, client)

	if err := driver.Replace(context.Background(), "worker-old"); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	if got := client.deletedPods(); !reflect.DeepEqual(got, []deleteCall{{name: "worker-old", uid: "uid-old"}}) {
		t.Fatalf("delete calls = %#v, want UID-protected old pod deletion", got)
	}
	workers, err := driver.List(context.Background())
	if err != nil {
		t.Fatalf("List() after replacement error = %v", err)
	}
	if !reflect.DeepEqual(workers, []fleet.WorkerInfo{{WorkerID: "worker-replacement", ProviderID: "uid-replacement", State: fleet.WorkerStateRunning}}) {
		t.Fatalf("workers after replacement = %#v, want replacement pod", workers)
	}
}

func TestReplaceReportsUIDConflictWithoutDeletingNewIncarnation(t *testing.T) {
	client := newFakeClient()
	client.scale.Spec.Replicas = 1
	client.pods = []corev1.Pod{pod("worker-old", "uid-old", corev1.PodRunning)}
	client.deleteErr = apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, "worker-old", errors.New("UID changed"))
	driver := mustNewDriver(t, client)

	if err := driver.Replace(context.Background(), "worker-old"); err == nil || !apierrors.IsConflict(err) {
		t.Fatalf("Replace() error = %v, want UID conflict", err)
	}
	if got := client.podSnapshot(); len(got) != 1 || got[0].Name != "worker-old" || got[0].UID != "uid-old" {
		t.Fatalf("pods after UID conflict = %#v, want original pod unchanged", got)
	}
}

func TestReplaceTreatsMissingPodAsReplacementAlreadyUnderway(t *testing.T) {
	client := newFakeClient()
	client.scale.Spec.Replicas = 1
	client.pods = []corev1.Pod{pod("worker-new", "uid-new", corev1.PodRunning)}
	driver := mustNewDriver(t, client)

	if err := driver.Replace(context.Background(), "worker-old"); err != nil {
		t.Fatalf("Replace() error = %v, want missing old pod to converge", err)
	}
	if got := client.deletedPods(); len(got) != 0 {
		t.Fatalf("delete calls = %#v, want none for already absent pod", got)
	}
}

func TestTeardownScalesToZeroIsIdempotentAndClosesScaling(t *testing.T) {
	client := newFakeClient()
	client.scale.Spec.Replicas = 1
	client.pods = []corev1.Pod{pod("worker-1", "uid-1", corev1.PodRunning)}
	driver := mustNewDriver(t, client)

	if err := driver.Teardown(context.Background()); err != nil {
		t.Fatalf("Teardown() error = %v", err)
	}
	if err := driver.Teardown(context.Background()); err != nil {
		t.Fatalf("repeated Teardown() error = %v", err)
	}
	if got := client.updatedReplicas(); !reflect.DeepEqual(got, []int32{0}) {
		t.Fatalf("updated replicas = %v, want one scale-to-zero update", got)
	}
	if err := driver.Scale(context.Background(), 1); !errors.Is(err, fleet.ErrClosed) {
		t.Fatalf("Scale() after teardown error = %v, want ErrClosed", err)
	}
	if err := driver.Replace(context.Background(), "worker-1"); !errors.Is(err, fleet.ErrClosed) {
		t.Fatalf("Replace() after teardown error = %v, want ErrClosed", err)
	}
	if workers, err := driver.List(context.Background()); err != nil || len(workers) != 0 {
		t.Fatalf("List() after teardown = %#v, %v; want empty list available for cleanup verification", workers, err)
	}
}

func mustNewDriver(t *testing.T, client *fakeClient) *Driver {
	t.Helper()
	driver, err := newDriver(context.Background(), validOptions(), client)
	if err != nil {
		t.Fatalf("newDriver() error = %v", err)
	}
	return driver
}

func validOptions() Options {
	return Options{Namespace: "load-test", Deployment: "workers"}
}

type deleteCall struct {
	name string
	uid  types.UID
}

type fakeClient struct {
	mu                 sync.Mutex
	deployment         *appsv1.Deployment
	scale              *autoscalingv1.Scale
	pods               []corev1.Pod
	getDeploymentErr   error
	conflictsRemaining int
	deleteErr          error
	onUpdate           func(*fakeClient, int32)
	onDelete           func(*fakeClient, string, types.UID)
	selectors          []string
	updates            []int32
	deleteCalls        []deleteCall
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		deployment: workerDeployment(0),
		scale:      &autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: 0}},
		onUpdate: func(client *fakeClient, replicas int32) {
			client.pods = runningPodSet(replicas)
		},
		onDelete: func(client *fakeClient, name string, _ types.UID) {
			for index, candidate := range client.pods {
				if candidate.Name == name {
					client.pods = append(client.pods[:index], client.pods[index+1:]...)
					break
				}
			}
			client.pods = append(client.pods, pod("worker-replacement", "uid-replacement", corev1.PodRunning))
		},
	}
}

func (f *fakeClient) GetDeployment(context.Context, string, string) (*appsv1.Deployment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getDeploymentErr != nil {
		return nil, f.getDeploymentErr
	}
	return f.deployment.DeepCopy(), nil
}

func (f *fakeClient) GetScale(context.Context, string, string) (*autoscalingv1.Scale, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scale.DeepCopy(), nil
}

func (f *fakeClient) UpdateScale(_ context.Context, _ string, _ string, scale *autoscalingv1.Scale) (*autoscalingv1.Scale, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, scale.Spec.Replicas)
	if f.conflictsRemaining > 0 {
		f.conflictsRemaining--
		return nil, apierrors.NewConflict(schema.GroupResource{Resource: "deployments/scale"}, "workers", errors.New("conflict"))
	}
	f.scale = scale.DeepCopy()
	if f.onUpdate != nil {
		f.onUpdate(f, scale.Spec.Replicas)
	}
	return f.scale.DeepCopy(), nil
}

func (f *fakeClient) ListPods(_ context.Context, _ string, selector string) ([]corev1.Pod, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.selectors = append(f.selectors, selector)
	pods := make([]corev1.Pod, len(f.pods))
	for index := range f.pods {
		pods[index] = *f.pods[index].DeepCopy()
	}
	return pods, nil
}

func (f *fakeClient) DeletePod(_ context.Context, _ string, name string, uid types.UID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, deleteCall{name: name, uid: uid})
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if f.onDelete != nil {
		f.onDelete(f, name, uid)
	}
	return nil
}

func (f *fakeClient) lastSelector() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.selectors) == 0 {
		return ""
	}
	return f.selectors[len(f.selectors)-1]
}

func (f *fakeClient) updatedReplicas() []int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int32(nil), f.updates...)
}

func (f *fakeClient) updateAttempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.updates)
}

func (f *fakeClient) deletedPods() []deleteCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]deleteCall(nil), f.deleteCalls...)
}

func (f *fakeClient) podSnapshot() []corev1.Pod {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]corev1.Pod(nil), f.pods...)
}

func workerDeployment(replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "load-test", Labels: map[string]string{managedDeploymentLabel: "true"}},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}},
		},
	}
}

func runningPodSet(replicas int32) []corev1.Pod {
	pods := make([]corev1.Pod, 0, replicas)
	for index := int32(1); index <= replicas; index++ {
		pods = append(pods, pod("worker-"+strconv.Itoa(int(index)), types.UID("uid-"+strconv.Itoa(int(index))), corev1.PodRunning))
	}
	return pods
}

func pod(name string, uid types.UID, phase corev1.PodPhase) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid}, Status: corev1.PodStatus{Phase: phase}}
}

func terminatingPod(name string, uid types.UID, phase corev1.PodPhase) corev1.Pod {
	value := metav1.Now()
	result := pod(name, uid, phase)
	result.DeletionTimestamp = &value
	return result
}

func int32ptr(value int32) *int32 { return &value }

func int64ToInt(t *testing.T, value int64) int {
	t.Helper()
	converted := int(value)
	if int64(converted) != value {
		t.Skip("platform int cannot represent the test value")
	}
	return converted
}

func runningPods(pods []corev1.Pod) int {
	count := 0
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
			count++
		}
	}
	return count
}
