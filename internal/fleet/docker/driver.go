// Package docker manages load-test workers through the Docker Engine API.
package docker

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcuslin123/load-tester/internal/fleet"
)

const (
	managedLabel = "io.github.marcuslin123.load-tester.managed"
	runIDLabel   = "io.github.marcuslin123.load-tester.run-id"
	workerLabel  = "io.github.marcuslin123.load-tester.worker-id"
	slotLabel    = "io.github.marcuslin123.load-tester.worker-slot"
)

var safeRunID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

type Options struct {
	RunID               string
	Image               string
	Network             string
	OrchestratorAddress string
	AddHostGateway      bool
}

type Driver struct {
	mu           sync.Mutex
	client       engineClient
	options      Options
	closed       bool
	clientClosed bool
	pollInterval time.Duration
	startTimeout time.Duration
	sleep        func(context.Context, time.Duration) error
}

type engineClient interface {
	ImageExists(ctx context.Context, image string) (bool, error)
	List(ctx context.Context, labels map[string]string) ([]engineContainer, error)
	Create(ctx context.Context, spec containerSpec) (string, error)
	Start(ctx context.Context, id string) error
	Inspect(ctx context.Context, id string) (engineContainer, error)
	Remove(ctx context.Context, id string) error
	Close() error
}

type engineContainer struct {
	ID      string
	Labels  map[string]string
	State   string
	Running bool
}

type containerSpec struct {
	Name          string
	Image         string
	Env           []string
	Labels        map[string]string
	Network       string
	ExtraHosts    []string
	RestartPolicy string
}

func newDriver(ctx context.Context, options Options, client engineClient) (*Driver, error) {
	options.RunID = strings.TrimSpace(options.RunID)
	options.Image = strings.TrimSpace(options.Image)
	options.Network = strings.TrimSpace(options.Network)
	options.OrchestratorAddress = strings.TrimSpace(options.OrchestratorAddress)
	if options.RunID == "" || !safeRunID.MatchString(options.RunID) {
		return nil, fmt.Errorf("run ID must be a Docker-safe identifier")
	}
	if options.Image == "" {
		return nil, fmt.Errorf("worker image is required")
	}
	if options.Network == "" {
		return nil, fmt.Errorf("Docker network is required")
	}
	if options.OrchestratorAddress == "" {
		return nil, fmt.Errorf("orchestrator address is required")
	}

	exists, err := client.ImageExists(ctx, options.Image)
	if err != nil {
		return nil, fmt.Errorf("inspect worker image %q: %w", options.Image, err)
	}
	if !exists {
		return nil, fmt.Errorf("worker image %q is not available locally", options.Image)
	}
	return &Driver{
		client:       client,
		options:      options,
		pollInterval: 25 * time.Millisecond,
		startTimeout: 10 * time.Second,
		sleep:        sleepContext,
	}, nil
}

// List returns every container owned by this run, including stopped workers.
func (d *Driver) List(ctx context.Context) ([]fleet.WorkerInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	workers, err := d.listManaged(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]fleet.WorkerInfo, 0, len(workers))
	for _, worker := range workers {
		result = append(result, fleet.WorkerInfo{
			WorkerID:   worker.workerID,
			ProviderID: worker.container.ID,
			State:      normalizeState(worker.container),
		})
	}
	return result, nil
}

// Scale reconciles this run to exactly desired running containers.
func (d *Driver) Scale(ctx context.Context, desired int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fleet.ErrClosed
	}
	if desired < 0 {
		return fmt.Errorf("desired worker count must not be negative")
	}
	workers, err := d.listManaged(ctx)
	if err != nil {
		return fmt.Errorf("list managed containers: %w", err)
	}
	preexisting := make(map[string]struct{}, len(workers))
	for _, worker := range workers {
		preexisting[worker.container.ID] = struct{}{}
	}

	remove := make([]managedWorker, 0)
	running := make(map[int]bool, desired)
	for _, worker := range workers {
		if worker.slot > desired || !worker.container.Running {
			remove = append(remove, worker)
			continue
		}
		running[worker.slot] = true
	}
	sort.Slice(remove, func(i, j int) bool { return remove[i].slot > remove[j].slot })
	var removeErrors []error
	for _, worker := range remove {
		if err := d.client.Remove(ctx, worker.container.ID); err != nil {
			removeErrors = append(removeErrors, fmt.Errorf("remove worker slot %d: %w", worker.slot, err))
		}
	}
	if err := errors.Join(removeErrors...); err != nil {
		return err
	}

	created := make([]string, 0, desired-len(running))
	for slot := 1; slot <= desired; slot++ {
		if running[slot] {
			continue
		}
		id, err := d.client.Create(ctx, d.containerSpec(slot))
		if err != nil {
			return d.rollbackAmbiguousCreate(ctx, preexisting, created, fmt.Errorf("create worker slot %d: %w", slot, err))
		}
		created = append(created, id)
		if err := d.client.Start(ctx, id); err != nil {
			return d.rollback(ctx, created, fmt.Errorf("start worker slot %d: %w", slot, err))
		}
		if err := d.waitRunning(ctx, id); err != nil {
			return d.rollback(ctx, created, fmt.Errorf("wait for worker slot %d: %w", slot, err))
		}
	}
	return nil
}

// Close releases Docker client resources after the driver is no longer needed.
func (d *Driver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.clientClosed {
		return nil
	}
	d.clientClosed = true
	d.closed = true
	return d.client.Close()
}

// Teardown permanently closes scaling and attempts to remove every owned container.
func (d *Driver) Teardown(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.closed = true
	workers, err := d.listManaged(ctx)
	if err != nil {
		return fmt.Errorf("list managed containers for teardown: %w", err)
	}
	var removeErrors []error
	for index := len(workers) - 1; index >= 0; index-- {
		worker := workers[index]
		if err := d.client.Remove(ctx, worker.container.ID); err != nil {
			removeErrors = append(removeErrors, fmt.Errorf("remove worker slot %d: %w", worker.slot, err))
		}
	}
	return errors.Join(removeErrors...)
}

type managedWorker struct {
	slot      int
	workerID  string
	container engineContainer
}

func (d *Driver) listManaged(ctx context.Context) ([]managedWorker, error) {
	containers, err := d.client.List(ctx, d.selectorLabels())
	if err != nil {
		return nil, err
	}
	workers := make([]managedWorker, 0, len(containers))
	seen := make(map[int]struct{}, len(containers))
	for _, container := range containers {
		if container.Labels[managedLabel] != "true" || container.Labels[runIDLabel] != d.options.RunID {
			return nil, fmt.Errorf("container %q does not match driver ownership labels", container.ID)
		}
		slot, err := strconv.Atoi(container.Labels[slotLabel])
		if err != nil || slot < 1 {
			return nil, fmt.Errorf("container %q has invalid worker slot %q", container.ID, container.Labels[slotLabel])
		}
		if _, exists := seen[slot]; exists {
			return nil, fmt.Errorf("multiple managed containers claim worker slot %d", slot)
		}
		seen[slot] = struct{}{}
		workerID := container.Labels[workerLabel]
		if want := d.workerID(slot); workerID != want {
			return nil, fmt.Errorf("container %q worker ID is %q, want %q", container.ID, workerID, want)
		}
		workers = append(workers, managedWorker{slot: slot, workerID: workerID, container: container})
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].slot < workers[j].slot })
	return workers, nil
}

func (d *Driver) selectorLabels() map[string]string {
	return map[string]string{managedLabel: "true", runIDLabel: d.options.RunID}
}

func (d *Driver) waitRunning(ctx context.Context, id string) error {
	waitCtx, cancel := context.WithTimeout(ctx, d.startTimeout)
	defer cancel()
	for {
		container, err := d.client.Inspect(waitCtx, id)
		if err != nil {
			return err
		}
		if container.Running {
			return nil
		}
		if container.State == "exited" || container.State == "dead" {
			return fmt.Errorf("container entered %s state", container.State)
		}
		if err := d.sleep(waitCtx, d.pollInterval); err != nil {
			return err
		}
	}
}

func (d *Driver) rollback(ctx context.Context, created []string, operationErr error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.startTimeout)
	defer cancel()
	return d.removeCreated(cleanupCtx, created, []error{operationErr})
}

func (d *Driver) rollbackAmbiguousCreate(
	ctx context.Context,
	preexisting map[string]struct{},
	created []string,
	operationErr error,
) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.startTimeout)
	defer cancel()
	errorsFound := []error{operationErr}
	known := make(map[string]struct{}, len(created))
	for _, id := range created {
		known[id] = struct{}{}
	}
	workers, err := d.listManaged(cleanupCtx)
	if err != nil {
		errorsFound = append(errorsFound, fmt.Errorf("discover containers after create failure: %w", err))
	} else {
		for _, worker := range workers {
			id := worker.container.ID
			if _, existed := preexisting[id]; existed {
				continue
			}
			if _, tracked := known[id]; tracked {
				continue
			}
			created = append(created, id)
			known[id] = struct{}{}
		}
	}
	return d.removeCreated(cleanupCtx, created, errorsFound)
}

func (d *Driver) removeCreated(ctx context.Context, created []string, errorsFound []error) error {
	for index := len(created) - 1; index >= 0; index-- {
		if err := d.client.Remove(ctx, created[index]); err != nil {
			errorsFound = append(errorsFound, fmt.Errorf("roll back container %q: %w", created[index], err))
		}
	}
	return errors.Join(errorsFound...)
}

func normalizeState(container engineContainer) fleet.WorkerState {
	if container.Running {
		return fleet.WorkerStateRunning
	}
	switch container.State {
	case "created", "restarting":
		return fleet.WorkerStateStarting
	case "exited", "dead", "removing":
		return fleet.WorkerStateStopped
	default:
		return fleet.WorkerStateUnknown
	}
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

func (d *Driver) containerSpec(slot int) containerSpec {
	workerID := d.workerID(slot)
	spec := containerSpec{
		Name:          workerID,
		Image:         d.options.Image,
		Env:           []string{"ORCHESTRATOR_ADDR=" + d.options.OrchestratorAddress, "WORKER_ID=" + workerID},
		Labels:        ownershipLabels(d.options.RunID, workerID, slot),
		Network:       d.options.Network,
		RestartPolicy: "no",
	}
	if d.options.AddHostGateway {
		spec.ExtraHosts = []string{"host.docker.internal:host-gateway"}
	}
	return spec
}

func (d *Driver) workerID(slot int) string {
	return fmt.Sprintf("%s-worker-%d", d.options.RunID, slot)
}

func ownershipLabels(runID, workerID string, slot int) map[string]string {
	return map[string]string{
		managedLabel: "true",
		runIDLabel:   runID,
		workerLabel:  workerID,
		slotLabel:    strconv.Itoa(slot),
	}
}
