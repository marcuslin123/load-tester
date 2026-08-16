package docker

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/fleet"
	containertypes "github.com/moby/moby/api/types/container"
)

func TestNewDriverValidatesOptions(t *testing.T) {
	valid := Options{
		RunID:               "run-16",
		Image:               "load-tester-worker:test",
		Network:             "piece-16",
		OrchestratorAddress: "host.docker.internal:9090",
	}
	tests := []struct {
		name   string
		change func(*Options)
		want   string
	}{
		{name: "run ID", change: func(options *Options) { options.RunID = "" }, want: "run ID"},
		{name: "image", change: func(options *Options) { options.Image = "" }, want: "image"},
		{name: "network", change: func(options *Options) { options.Network = "" }, want: "network"},
		{name: "orchestrator address", change: func(options *Options) { options.OrchestratorAddress = "" }, want: "orchestrator address"},
		{name: "unsafe run ID", change: func(options *Options) { options.RunID = "run/id" }, want: "run ID"},
		{name: "negative worker CPUs", change: func(options *Options) { options.WorkerCPUs = -0.25 }, want: "worker CPU limit"},
		{name: "NaN worker CPUs", change: func(options *Options) { options.WorkerCPUs = math.NaN() }, want: "worker CPU limit"},
		{name: "positive infinite worker CPUs", change: func(options *Options) { options.WorkerCPUs = math.Inf(1) }, want: "worker CPU limit"},
		{name: "negative infinite worker CPUs", change: func(options *Options) { options.WorkerCPUs = math.Inf(-1) }, want: "worker CPU limit"},
		{name: "sub NanoCPU worker CPUs", change: func(options *Options) { options.WorkerCPUs = 0.5e-9 }, want: "worker CPU limit"},
		{name: "overflowing worker CPUs", change: func(options *Options) { options.WorkerCPUs = 10_000_000_000 }, want: "worker CPU limit"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.change(&options)
			_, err := newDriver(context.Background(), options, &fakeEngine{imageExists: true})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("newDriver() error = %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestNewDriverRequiresLocalImage(t *testing.T) {
	engine := &fakeEngine{}
	_, err := newDriver(context.Background(), validOptions(), engine)
	if err == nil || !strings.Contains(err.Error(), "not available locally") {
		t.Fatalf("newDriver() error = %v, want missing local image error", err)
	}
	if engine.imageChecked != "load-tester-worker:test" {
		t.Fatalf("checked image = %q, want configured worker image", engine.imageChecked)
	}
}

func TestNewDriverReportsImageInspectionFailure(t *testing.T) {
	inspectErr := errors.New("docker unavailable")
	_, err := newDriver(context.Background(), validOptions(), &fakeEngine{imageErr: inspectErr})
	if !errors.Is(err, inspectErr) {
		t.Fatalf("newDriver() error = %v, want wrapped inspection error", err)
	}
}

func TestContainerSpecConfiguresWorkerOwnershipAndConnectivity(t *testing.T) {
	driver, err := newDriver(context.Background(), Options{
		RunID:               "run-16",
		Image:               "load-tester-worker:test",
		Network:             "piece-16",
		OrchestratorAddress: "host.docker.internal:9090",
		AddHostGateway:      true,
		WorkerCPUs:          0.25,
	}, &fakeEngine{imageExists: true})
	if err != nil {
		t.Fatalf("newDriver() error = %v", err)
	}

	got := driver.containerSpec(2)
	want := containerSpec{
		Name:          "run-16-worker-2",
		Image:         "load-tester-worker:test",
		WorkerCPUs:    0.25,
		Env:           []string{"ORCHESTRATOR_ADDR=host.docker.internal:9090", "WORKER_ID=run-16-worker-2"},
		Labels:        ownershipLabels("run-16", "run-16-worker-2", 2),
		Network:       "piece-16",
		ExtraHosts:    []string{"host.docker.internal:host-gateway"},
		RestartPolicy: "no",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("containerSpec(2) = %#v, want %#v", got, want)
	}
}

func TestContainerSpecOmitsHostGatewayByDefault(t *testing.T) {
	driver, err := newDriver(context.Background(), validOptions(), &fakeEngine{imageExists: true})
	if err != nil {
		t.Fatalf("newDriver() error = %v", err)
	}
	if got := driver.containerSpec(1).ExtraHosts; got != nil {
		t.Fatalf("extra hosts = %v, want nil", got)
	}
}

func TestMobyCreateOptionsTranslateContainerSpec(t *testing.T) {
	spec := containerSpec{
		Name:          "run-16-worker-1",
		Image:         "load-tester-worker:test",
		Env:           []string{"ORCHESTRATOR_ADDR=orchestrator:9090", "WORKER_ID=run-16-worker-1"},
		Labels:        ownershipLabels("run-16", "run-16-worker-1", 1),
		Network:       "piece-16",
		ExtraHosts:    []string{"host.docker.internal:host-gateway"},
		RestartPolicy: "no",
	}

	got := toMobyCreateOptions(spec)
	if got.Name != spec.Name || got.Config.Image != spec.Image {
		t.Fatalf("create identity = name %q image %q, want %q %q", got.Name, got.Config.Image, spec.Name, spec.Image)
	}
	if !reflect.DeepEqual(got.Config.Env, spec.Env) || !reflect.DeepEqual(got.Config.Labels, spec.Labels) {
		t.Fatalf("create config = env %v labels %v, want %v %v", got.Config.Env, got.Config.Labels, spec.Env, spec.Labels)
	}
	if got.HostConfig.NetworkMode != containertypes.NetworkMode(spec.Network) {
		t.Fatalf("network mode = %q, want %q", got.HostConfig.NetworkMode, spec.Network)
	}
	if !reflect.DeepEqual(got.HostConfig.ExtraHosts, spec.ExtraHosts) {
		t.Fatalf("extra hosts = %v, want %v", got.HostConfig.ExtraHosts, spec.ExtraHosts)
	}
	if got.HostConfig.RestartPolicy.Name != containertypes.RestartPolicyDisabled {
		t.Fatalf("restart policy = %q, want disabled", got.HostConfig.RestartPolicy.Name)
	}
}

func TestMobyCreateOptionsOmitsWorkerCPULimitWhenZero(t *testing.T) {
	got := toMobyCreateOptions(containerSpec{WorkerCPUs: 0})
	if got.HostConfig.Resources.NanoCPUs != 0 {
		t.Fatalf("NanoCPUs = %d, want unlimited zero value", got.HostConfig.Resources.NanoCPUs)
	}
}

func TestMobyCreateOptionsConvertsWorkerCPUsToNanoCPUs(t *testing.T) {
	got := toMobyCreateOptions(containerSpec{WorkerCPUs: 0.25})
	if got.HostConfig.Resources.NanoCPUs != 250000000 {
		t.Fatalf("NanoCPUs = %d, want 250000000", got.HostConfig.Resources.NanoCPUs)
	}
}

func TestMobyCreateOptionsRoundsFractionalNanoCPUs(t *testing.T) {
	got := toMobyCreateOptions(containerSpec{WorkerCPUs: 1.6e-9})
	if got.HostConfig.Resources.NanoCPUs != 2 {
		t.Fatalf("NanoCPUs = %d, want rounded value 2", got.HostConfig.Resources.NanoCPUs)
	}
}

func TestListReturnsWorkersSortedBySlotWithNormalizedStates(t *testing.T) {
	engine := &fakeEngine{imageExists: true, containers: []engineContainer{
		managedContainer("container-2", "run-16", 2, "exited", false),
		managedContainer("container-3", "run-16", 3, "created", false),
		managedContainer("container-1", "run-16", 1, "running", true),
	}}
	driver := mustNewDriver(t, engine)

	workers, err := driver.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	want := []fleet.WorkerInfo{
		{WorkerID: "run-16-worker-1", ProviderID: "container-1", State: fleet.WorkerStateRunning},
		{WorkerID: "run-16-worker-2", ProviderID: "container-2", State: fleet.WorkerStateStopped},
		{WorkerID: "run-16-worker-3", ProviderID: "container-3", State: fleet.WorkerStateStarting},
	}
	if !reflect.DeepEqual(workers, want) {
		t.Fatalf("List() = %#v, want %#v", workers, want)
	}
	if !reflect.DeepEqual(engine.listLabels, map[string]string{managedLabel: "true", runIDLabel: "run-16"}) {
		t.Fatalf("list labels = %v, want run ownership labels", engine.listLabels)
	}
}

func TestListRejectsMalformedOrDuplicateManagedSlots(t *testing.T) {
	tests := []struct {
		name       string
		containers []engineContainer
	}{
		{
			name: "missing slot",
			containers: []engineContainer{{
				ID:     "bad",
				Labels: map[string]string{workerLabel: "run-16-worker-1"},
			}},
		},
		{name: "invalid slot", containers: []engineContainer{managedContainer("bad", "run-16", 0, "running", true)}},
		{name: "wrong worker ID", containers: []engineContainer{withWorkerID(managedContainer("bad", "run-16", 1, "running", true), "another-worker")}},
		{name: "duplicate slot", containers: []engineContainer{managedContainer("one", "run-16", 1, "running", true), managedContainer("two", "run-16", 1, "running", true)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := mustNewDriver(t, &fakeEngine{imageExists: true, containers: test.containers})
			if _, err := driver.List(context.Background()); err == nil {
				t.Fatal("List() error = nil, want malformed managed container error")
			}
		})
	}
}

func TestScaleCreatesMissingSlotsInAscendingOrder(t *testing.T) {
	engine := &fakeEngine{imageExists: true, runningAfterStart: true}
	driver := mustNewDriver(t, engine)

	if err := driver.Scale(context.Background(), 3); err != nil {
		t.Fatalf("Scale(3) error = %v", err)
	}
	if got, want := specNames(engine.created), []string{"run-16-worker-1", "run-16-worker-2", "run-16-worker-3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("created containers = %v, want %v", got, want)
	}
	if got, want := engine.started, []string{"created-1", "created-2", "created-3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("started containers = %v, want %v", got, want)
	}
	if err := driver.Scale(context.Background(), 3); err != nil {
		t.Fatalf("repeated Scale(3) error = %v", err)
	}
	if len(engine.created) != 3 {
		t.Fatalf("created containers after repeated Scale(3) = %d, want 3", len(engine.created))
	}
}

func TestScaleIsIdempotentWhenDesiredWorkersAreRunning(t *testing.T) {
	engine := &fakeEngine{imageExists: true, containers: []engineContainer{
		managedContainer("one", "run-16", 1, "running", true),
		managedContainer("two", "run-16", 2, "running", true),
	}}
	driver := mustNewDriver(t, engine)

	if err := driver.Scale(context.Background(), 2); err != nil {
		t.Fatalf("Scale(2) error = %v", err)
	}
	if len(engine.created) != 0 || len(engine.removed) != 0 {
		t.Fatalf("same-size scale created %d and removed %v, want no changes", len(engine.created), engine.removed)
	}
}

func TestScaleDownRemovesHighestSlotsFirst(t *testing.T) {
	engine := &fakeEngine{imageExists: true, containers: []engineContainer{
		managedContainer("one", "run-16", 1, "running", true),
		managedContainer("two", "run-16", 2, "running", true),
		managedContainer("three", "run-16", 3, "running", true),
	}}
	driver := mustNewDriver(t, engine)

	if err := driver.Scale(context.Background(), 1); err != nil {
		t.Fatalf("Scale(1) error = %v", err)
	}
	if want := []string{"three", "two"}; !reflect.DeepEqual(engine.removed, want) {
		t.Fatalf("removed containers = %v, want %v", engine.removed, want)
	}
}

func TestScaleReplacesStoppedContainerInSameSlot(t *testing.T) {
	engine := &fakeEngine{
		imageExists:       true,
		runningAfterStart: true,
		containers:        []engineContainer{managedContainer("stopped", "run-16", 1, "exited", false)},
	}
	driver := mustNewDriver(t, engine)

	if err := driver.Scale(context.Background(), 1); err != nil {
		t.Fatalf("Scale(1) error = %v", err)
	}
	if !reflect.DeepEqual(engine.removed, []string{"stopped"}) || len(engine.created) != 1 || engine.created[0].Name != "run-16-worker-1" {
		t.Fatalf("replacement removed=%v created=%v, want stopped removed and slot 1 recreated", engine.removed, specNames(engine.created))
	}
}

func TestReplaceRecreatesRunningContainerInExactStableSlot(t *testing.T) {
	engine := &fakeEngine{
		imageExists:       true,
		runningAfterStart: true,
		containers: []engineContainer{
			managedContainer("one", "run-16", 1, "running", true),
			managedContainer("two", "run-16", 2, "running", true),
		},
	}
	driver := mustNewDriver(t, engine)

	if err := driver.Replace(context.Background(), "run-16-worker-2"); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	if want := []string{"two"}; !reflect.DeepEqual(engine.removed, want) {
		t.Fatalf("removed containers = %v, want %v", engine.removed, want)
	}
	if got, want := specNames(engine.created), []string{"run-16-worker-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("created containers = %v, want %v", got, want)
	}
	if len(engine.containers) != 2 {
		t.Fatalf("containers after replacement = %v, want two slots", engine.containers)
	}
}

func TestScaleRollsBackOnlyContainersCreatedByFailedScaleUp(t *testing.T) {
	createErr := errors.New("create slot 4")
	engine := &fakeEngine{
		imageExists:       true,
		runningAfterStart: true,
		containers:        []engineContainer{managedContainer("existing", "run-16", 1, "running", true)},
		createErrors:      map[int]error{4: createErr},
	}
	driver := mustNewDriver(t, engine)

	err := driver.Scale(context.Background(), 4)
	if !errors.Is(err, createErr) {
		t.Fatalf("Scale(4) error = %v, want create failure", err)
	}
	if want := []string{"created-3", "created-2"}; !reflect.DeepEqual(engine.removed, want) {
		t.Fatalf("rollback removals = %v, want %v", engine.removed, want)
	}
	for _, removed := range engine.removed {
		if removed == "existing" {
			t.Fatal("rollback removed a container that existed before Scale")
		}
	}
}

func TestScaleRecoversContainerFromAmbiguousCreateFailure(t *testing.T) {
	createErr := errors.New("create response lost")
	engine := &fakeEngine{
		imageExists:           true,
		runningAfterStart:     true,
		containers:            []engineContainer{managedContainer("existing", "run-16", 1, "running", true)},
		ambiguousCreateErrors: map[int]error{4: createErr},
	}
	driver := mustNewDriver(t, engine)

	err := driver.Scale(context.Background(), 4)
	if !errors.Is(err, createErr) {
		t.Fatalf("Scale(4) error = %v, want ambiguous create failure", err)
	}
	if want := []string{"created-4", "created-3", "created-2"}; !reflect.DeepEqual(engine.removed, want) {
		t.Fatalf("rollback removals = %v, want %v", engine.removed, want)
	}
	if len(engine.containers) != 1 || engine.containers[0].ID != "existing" {
		t.Fatalf("containers after rollback = %v, want only pre-existing container", engine.containers)
	}
}

func TestScaleDownAttemptsEveryRemovalAndJoinsErrors(t *testing.T) {
	removeTwo := errors.New("remove two")
	removeThree := errors.New("remove three")
	engine := &fakeEngine{
		imageExists: true,
		containers: []engineContainer{
			managedContainer("one", "run-16", 1, "running", true),
			managedContainer("two", "run-16", 2, "running", true),
			managedContainer("three", "run-16", 3, "running", true),
		},
		removeErrors: map[string]error{"two": removeTwo, "three": removeThree},
	}
	driver := mustNewDriver(t, engine)

	err := driver.Scale(context.Background(), 1)
	if !errors.Is(err, removeTwo) || !errors.Is(err, removeThree) {
		t.Fatalf("Scale(1) error = %v, want both removal failures", err)
	}
	if want := []string{"three", "two"}; !reflect.DeepEqual(engine.removed, want) {
		t.Fatalf("removal attempts = %v, want %v", engine.removed, want)
	}
}

func TestScaleRejectsNegativeDesiredCount(t *testing.T) {
	driver := mustNewDriver(t, &fakeEngine{imageExists: true})
	if err := driver.Scale(context.Background(), -1); err == nil {
		t.Fatal("Scale(-1) error = nil, want validation error")
	}
}

func TestScaleStopsWaitingWhenContextIsCanceled(t *testing.T) {
	engine := &fakeEngine{imageExists: true}
	driver := mustNewDriver(t, engine)
	ctx, cancel := context.WithCancel(context.Background())
	driver.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}

	err := driver.Scale(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Scale() error = %v, want context cancellation", err)
	}
	if !reflect.DeepEqual(engine.removed, []string{"created-1"}) {
		t.Fatalf("rollback removals = %v, want interrupted container removed", engine.removed)
	}
	if !reflect.DeepEqual(engine.removeContextErrors, []error{nil}) {
		t.Fatalf("rollback removal context errors = %v, want active cleanup context", engine.removeContextErrors)
	}
}

func TestTeardownIsIdempotentTerminalAndLeavesListAvailable(t *testing.T) {
	engine := &fakeEngine{imageExists: true, containers: []engineContainer{
		managedContainer("one", "run-16", 1, "running", true),
		managedContainer("two", "run-16", 2, "running", true),
	}}
	driver := mustNewDriver(t, engine)

	if err := driver.Teardown(context.Background()); err != nil {
		t.Fatalf("first Teardown() error = %v", err)
	}
	if err := driver.Teardown(context.Background()); err != nil {
		t.Fatalf("second Teardown() error = %v", err)
	}
	if got := engine.removed; !reflect.DeepEqual(got, []string{"two", "one"}) {
		t.Fatalf("teardown removals = %v, want each managed container once", got)
	}
	if err := driver.Scale(context.Background(), 1); !errors.Is(err, fleet.ErrClosed) {
		t.Fatalf("Scale() after teardown error = %v, want fleet.ErrClosed", err)
	}
	workers, err := driver.List(context.Background())
	if err != nil {
		t.Fatalf("List() after teardown error = %v", err)
	}
	if len(workers) != 0 || len(engine.created) != 0 {
		t.Fatalf("post-teardown workers=%v created=%v, want empty fleet", workers, specNames(engine.created))
	}
}

func TestTeardownAttemptsEveryRemovalAndJoinsErrors(t *testing.T) {
	removeOne := errors.New("remove one")
	removeTwo := errors.New("remove two")
	engine := &fakeEngine{
		imageExists: true,
		containers: []engineContainer{
			managedContainer("one", "run-16", 1, "running", true),
			managedContainer("two", "run-16", 2, "running", true),
		},
		removeErrors: map[string]error{"one": removeOne, "two": removeTwo},
	}
	driver := mustNewDriver(t, engine)

	err := driver.Teardown(context.Background())
	if !errors.Is(err, removeOne) || !errors.Is(err, removeTwo) {
		t.Fatalf("Teardown() error = %v, want both removal failures", err)
	}
	if got := engine.removed; !reflect.DeepEqual(got, []string{"two", "one"}) {
		t.Fatalf("teardown attempts = %v, want both containers", got)
	}
	if err := driver.Scale(context.Background(), 1); !errors.Is(err, fleet.ErrClosed) {
		t.Fatalf("Scale() after failed teardown error = %v, want fleet.ErrClosed", err)
	}
}

func TestDriverSerializesListAndTeardown(t *testing.T) {
	engine := &blockingListEngine{
		fakeEngine:   fakeEngine{imageExists: true},
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	driver, err := newDriver(context.Background(), validOptions(), engine)
	if err != nil {
		t.Fatalf("newDriver() error = %v", err)
	}

	listDone := make(chan error, 1)
	go func() {
		_, err := driver.List(context.Background())
		listDone <- err
	}()
	<-engine.firstEntered
	teardownDone := make(chan error, 1)
	go func() { teardownDone <- driver.Teardown(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	if got := engine.listCalls.Load(); got != 1 {
		t.Fatalf("concurrent list calls = %d, want teardown blocked behind List", got)
	}
	close(engine.releaseFirst)
	if err := <-listDone; err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if err := <-teardownDone; err != nil {
		t.Fatalf("Teardown() error = %v", err)
	}
	if err := driver.Scale(context.Background(), 1); !errors.Is(err, fleet.ErrClosed) {
		t.Fatalf("Scale() after serialized teardown error = %v, want fleet.ErrClosed", err)
	}
}

func TestCloseReleasesEngineClientOnce(t *testing.T) {
	engine := &fakeEngine{imageExists: true}
	driver := mustNewDriver(t, engine)
	if err := driver.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := driver.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if engine.closeCalls != 1 {
		t.Fatalf("engine Close() calls = %d, want 1", engine.closeCalls)
	}
}

func validOptions() Options {
	return Options{
		RunID:               "run-16",
		Image:               "load-tester-worker:test",
		Network:             "piece-16",
		OrchestratorAddress: "host.docker.internal:9090",
	}
}

type fakeEngine struct {
	imageExists           bool
	imageErr              error
	imageChecked          string
	containers            []engineContainer
	listLabels            map[string]string
	listErr               error
	created               []containerSpec
	createErrors          map[int]error
	ambiguousCreateErrors map[int]error
	started               []string
	startErrors           map[string]error
	runningAfterStart     bool
	inspectErr            error
	removed               []string
	removeErrors          map[string]error
	removeContextErrors   []error
	closeCalls            int
}

func (f *fakeEngine) ImageExists(_ context.Context, image string) (bool, error) {
	f.imageChecked = image
	return f.imageExists, f.imageErr
}

func (f *fakeEngine) List(_ context.Context, labels map[string]string) ([]engineContainer, error) {
	f.listLabels = cloneMap(labels)
	return append([]engineContainer(nil), f.containers...), f.listErr
}

func (f *fakeEngine) Create(_ context.Context, spec containerSpec) (string, error) {
	f.created = append(f.created, spec)
	slot, _ := strconv.Atoi(spec.Labels[slotLabel])
	if err := f.createErrors[slot]; err != nil {
		return "", err
	}
	id := "created-" + spec.Labels[slotLabel]
	f.containers = append(f.containers, engineContainer{ID: id, Labels: cloneMap(spec.Labels), State: "created"})
	if err := f.ambiguousCreateErrors[slot]; err != nil {
		return "", err
	}
	return id, nil
}

func (f *fakeEngine) Close() error {
	f.closeCalls++
	return nil
}

type blockingListEngine struct {
	fakeEngine
	firstEntered chan struct{}
	releaseFirst chan struct{}
	listCalls    atomic.Int32
}

func (f *blockingListEngine) List(ctx context.Context, labels map[string]string) ([]engineContainer, error) {
	if f.listCalls.Add(1) == 1 {
		close(f.firstEntered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.releaseFirst:
		}
	}
	return f.fakeEngine.List(ctx, labels)
}

func (f *fakeEngine) Start(_ context.Context, id string) error {
	f.started = append(f.started, id)
	if err := f.startErrors[id]; err != nil {
		return err
	}
	if f.runningAfterStart {
		for index := range f.containers {
			if f.containers[index].ID == id {
				f.containers[index].State = "running"
				f.containers[index].Running = true
			}
		}
	}
	return nil
}

func (f *fakeEngine) Inspect(_ context.Context, id string) (engineContainer, error) {
	if f.inspectErr != nil {
		return engineContainer{}, f.inspectErr
	}
	for _, container := range f.containers {
		if container.ID == id {
			return container, nil
		}
	}
	return engineContainer{}, errors.New("container not found")
}

func (f *fakeEngine) Remove(ctx context.Context, id string) error {
	f.removed = append(f.removed, id)
	f.removeContextErrors = append(f.removeContextErrors, ctx.Err())
	if err := f.removeErrors[id]; err != nil {
		return err
	}
	kept := f.containers[:0]
	for _, container := range f.containers {
		if container.ID != id {
			kept = append(kept, container)
		}
	}
	f.containers = kept
	return nil
}

func mustNewDriver(t *testing.T, engine *fakeEngine) *Driver {
	t.Helper()
	driver, err := newDriver(context.Background(), validOptions(), engine)
	if err != nil {
		t.Fatalf("newDriver() error = %v", err)
	}
	driver.pollInterval = time.Millisecond
	driver.startTimeout = 50 * time.Millisecond
	return driver
}

func managedContainer(id, runID string, slot int, state string, running bool) engineContainer {
	workerID := runID + "-worker-" + strconv.Itoa(slot)
	return engineContainer{ID: id, Labels: ownershipLabels(runID, workerID, slot), State: state, Running: running}
}

func withWorkerID(container engineContainer, workerID string) engineContainer {
	container.Labels[workerLabel] = workerID
	return container
}

func specNames(specs []containerSpec) []string {
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		names = append(names, spec.Name)
	}
	return names
}

func cloneMap(values map[string]string) map[string]string {
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}
