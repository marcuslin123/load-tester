package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	"github.com/marcuslin123/load-tester/internal/autoscale"
	loadconfig "github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/fleet"
	"github.com/marcuslin123/load-tester/internal/fleet/docker"
	"github.com/marcuslin123/load-tester/internal/fleet/kubernetes"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/orchestrator"
	"github.com/marcuslin123/load-tester/internal/report"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

const (
	heartbeatInterval    = 3 * time.Second
	assignmentLeadTime   = time.Second
	metricsGracePeriod   = 4 * time.Second
	metricsDeliveryDelay = 3 * time.Second
	httpShutdownPeriod   = 5 * time.Second
	workerStartupTimeout = 30 * time.Second
	fleetCleanupPeriod   = 5 * time.Second
)

type exitCode int

const (
	exitPass         exitCode = 0
	exitTestFailure  exitCode = 1
	exitSetupFailure exitCode = 2
	exitInterrupted  exitCode = 130
)

type cliOptions struct {
	address                    string
	metricsAddress             string
	configPath                 string
	fleetDriver                fleetDriver
	workerStartupTimeout       time.Duration
	workerImage                string
	dockerNetwork              string
	workerOrchestratorAddress  string
	dockerAddHostGateway       bool
	dockerWorkerCPUs           float64
	kubernetesNamespace        string
	kubernetesWorkerDeployment string
}

type fleetDriver string

const (
	fleetDriverManual     fleetDriver = "manual"
	fleetDriverDocker     fleetDriver = "docker"
	fleetDriverKubernetes fleetDriver = "kubernetes"
)

type runResult struct {
	summary report.Summary
	err     error
}

type managedController interface {
	Prepare(context.Context) error
	Run(context.Context, autoscale.RunWindow) error
	AutoscalerMetrics() metrics.AutoscalerView
}

type managedControllerFactory func(
	fleet.FleetManager,
	*orchestrator.Server,
	loadconfig.Config,
	cliOptions,
	*log.Logger,
) (managedController, error)

type runWindowSource interface {
	WaitForRunWindow(context.Context) (orchestrator.RunWindow, error)
	RecordInfrastructureViolation(string)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(int(execute(ctx, os.Args[1:], os.Stdout, os.Stderr, log.Default())))
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer, logger *log.Logger) exitCode {
	options, err := parseOptions(args)
	if err != nil {
		fmt.Fprintf(stderr, "orchestrator: %v\n", err)
		return exitSetupFailure
	}
	cfg, err := loadTestConfig(options.configPath)
	if err != nil {
		fmt.Fprintf(stderr, "orchestrator: %v\n", err)
		return exitSetupFailure
	}
	if err := validateFleetSelection(options, cfg); err != nil {
		fmt.Fprintf(stderr, "orchestrator: %v\n", err)
		return exitSetupFailure
	}
	runID, err := newRunID()
	if err != nil {
		fmt.Fprintf(stderr, "orchestrator: generate run ID: %v\n", err)
		return exitSetupFailure
	}
	summary, err := runWithOptions(ctx, options, cfg, runID, logger, defaultFleetFactories(), newManagedController)
	if err != nil {
		fmt.Fprintf(stderr, "orchestrator: %v\n", err)
		return exitSetupFailure
	}
	if err := report.WriteText(stdout, summary); err != nil {
		fmt.Fprintf(stderr, "orchestrator: %v\n", err)
		return exitSetupFailure
	}
	return exitCodeForSummary(summary)
}

type dockerFleetFactory func(context.Context, docker.Options) (fleet.FleetManager, error)
type kubernetesFleetFactory func(context.Context, kubernetes.Options) (fleet.FleetManager, error)

type fleetFactories struct {
	docker     dockerFleetFactory
	kubernetes kubernetesFleetFactory
}

func validateFleetSelection(options cliOptions, cfg loadconfig.Config) error {
	if options.fleetDriver == fleetDriverManual && cfg.Fleet.Autoscale {
		return errors.New("manual fleet driver cannot be used with fleet.autoscale: true")
	}
	return nil
}

func newFleetManager(
	ctx context.Context,
	options cliOptions,
	runID string,
	factories fleetFactories,
) (fleet.FleetManager, error) {
	switch options.fleetDriver {
	case fleetDriverManual:
		return nil, nil
	case fleetDriverDocker:
		manager, err := factories.docker(ctx, docker.Options{
			RunID:               runID,
			Image:               options.workerImage,
			Network:             options.dockerNetwork,
			OrchestratorAddress: options.workerOrchestratorAddress,
			AddHostGateway:      options.dockerAddHostGateway,
			WorkerCPUs:          options.dockerWorkerCPUs,
		})
		if err != nil {
			return nil, fmt.Errorf("create Docker fleet driver: %w", err)
		}
		return manager, nil
	case fleetDriverKubernetes:
		manager, err := factories.kubernetes(ctx, kubernetes.Options{
			Namespace:  options.kubernetesNamespace,
			Deployment: options.kubernetesWorkerDeployment,
		})
		if err != nil {
			return nil, fmt.Errorf("create Kubernetes fleet driver: %w", err)
		}
		return manager, nil
	default:
		return nil, fmt.Errorf("unknown fleet driver %q", options.fleetDriver)
	}
}

type fleetCloser interface {
	Close() error
}

// cleanupManagedFleet releases managed workers even when an earlier cleanup step fails.
func cleanupManagedFleet(manager fleet.FleetManager, timeout time.Duration) error {
	var cleanupErrors []error
	scaleCtx, cancelScale := context.WithTimeout(context.Background(), timeout)
	if err := manager.Scale(scaleCtx, 0); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("scale managed workers to zero: %w", err))
	}
	cancelScale()

	teardownCtx, cancelTeardown := context.WithTimeout(context.Background(), timeout)
	if err := manager.Teardown(teardownCtx); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("teardown managed workers: %w", err))
	}
	cancelTeardown()
	if closer, ok := manager.(fleetCloser); ok {
		if err := closer.Close(); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("close managed fleet driver: %w", err))
		}
	}
	return errors.Join(cleanupErrors...)
}

func parseOptions(args []string) (cliOptions, error) {
	flags := flag.NewFlagSet("orchestrator", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("addr", ":9090", "gRPC listen address")
	metricsAddress := flags.String("metrics-addr", ":9091", "metrics and health listen address")
	configPath := flags.String("config", "", "path to the load-test YAML file")
	fleetDriverName := flags.String("fleet-driver", string(fleetDriverManual), "worker fleet driver (manual, docker, or kubernetes)")
	startupTimeout := flags.Duration("worker-startup-timeout", workerStartupTimeout, "maximum time for managed workers to register")
	workerImage := flags.String("worker-image", "load-tester-worker:latest", "managed worker container image")
	dockerNetwork := flags.String("docker-network", "bridge", "Docker network for managed workers")
	workerOrchestratorAddress := flags.String("worker-orchestrator-addr", "host.docker.internal:9090", "orchestrator address used by managed workers")
	dockerAddHostGateway := flags.Bool("docker-add-host-gateway", false, "add host.docker.internal host-gateway mapping to managed workers")
	dockerWorkerCPUs := flags.Float64("docker-worker-cpus", 0, "per-worker Docker CPU limit; zero is unlimited")
	kubernetesNamespace := flags.String("kubernetes-namespace", "load-tester", "namespace containing the managed worker Deployment")
	kubernetesWorkerDeployment := flags.String("kubernetes-worker-deployment", "load-tester-worker", "managed worker Deployment name")
	if err := flags.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if flags.NArg() != 0 {
		return cliOptions{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *configPath == "" {
		return cliOptions{}, errors.New("-config is required")
	}
	driver := fleetDriver(*fleetDriverName)
	if driver != fleetDriverManual && driver != fleetDriverDocker && driver != fleetDriverKubernetes {
		return cliOptions{}, fmt.Errorf("unknown fleet driver %q", *fleetDriverName)
	}
	if *startupTimeout <= 0 {
		return cliOptions{}, errors.New("-worker-startup-timeout must be greater than zero")
	}
	if err := docker.ValidateWorkerCPUs(*dockerWorkerCPUs); err != nil {
		return cliOptions{}, fmt.Errorf("-docker-worker-cpus: %w", err)
	}
	namespace := strings.TrimSpace(*kubernetesNamespace)
	deployment := strings.TrimSpace(*kubernetesWorkerDeployment)
	if driver == fleetDriverKubernetes && namespace == "" {
		return cliOptions{}, errors.New("-kubernetes-namespace is required for the Kubernetes fleet driver")
	}
	if driver == fleetDriverKubernetes && deployment == "" {
		return cliOptions{}, errors.New("-kubernetes-worker-deployment is required for the Kubernetes fleet driver")
	}
	return cliOptions{
		address:                    *address,
		metricsAddress:             *metricsAddress,
		configPath:                 *configPath,
		fleetDriver:                driver,
		workerStartupTimeout:       *startupTimeout,
		workerImage:                *workerImage,
		dockerNetwork:              *dockerNetwork,
		workerOrchestratorAddress:  *workerOrchestratorAddress,
		dockerAddHostGateway:       *dockerAddHostGateway,
		dockerWorkerCPUs:           *dockerWorkerCPUs,
		kubernetesNamespace:        namespace,
		kubernetesWorkerDeployment: deployment,
	}, nil
}

func newHTTPHandler(source metrics.PrometheusSource, info metrics.RunInfo) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.NewPrometheusCollector(source, info))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	return mux
}

func loadTestConfig(path string) (loadconfig.Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return loadconfig.Config{}, fmt.Errorf("open config: %w", err)
	}
	cfg, parseErr := loadconfig.Parse(file)
	closeErr := file.Close()
	if parseErr != nil {
		return loadconfig.Config{}, parseErr
	}
	if closeErr != nil {
		return loadconfig.Config{}, fmt.Errorf("close config: %w", closeErr)
	}
	return cfg, nil
}

func newRunID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func run(
	ctx context.Context,
	address string,
	metricsAddress string,
	cfg loadconfig.Config,
	runID string,
	logger *log.Logger,
) (report.Summary, error) {
	return runWithOptions(ctx, cliOptions{
		address:              address,
		metricsAddress:       metricsAddress,
		fleetDriver:          fleetDriverManual,
		workerStartupTimeout: workerStartupTimeout,
	}, cfg, runID, logger, defaultFleetFactories(), newManagedController)
}

func runWithOptions(
	ctx context.Context,
	options cliOptions,
	cfg loadconfig.Config,
	runID string,
	logger *log.Logger,
	factories fleetFactories,
	newController managedControllerFactory,
) (report.Summary, error) {
	grpcListener, err := net.Listen("tcp", options.address)
	if err != nil {
		return report.Summary{}, fmt.Errorf("listen on %s: %w", options.address, err)
	}
	defer grpcListener.Close()
	metricsListener, err := net.Listen("tcp", options.metricsAddress)
	if err != nil {
		return report.Summary{}, fmt.Errorf("listen metrics on %s: %w", options.metricsAddress, err)
	}
	defer metricsListener.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	control, err := orchestrator.NewServer(orchestrator.Options{
		HeartbeatInterval: heartbeatInterval,
		Logger:            logger,
		Assignment: &orchestrator.AssignmentOptions{
			Context:  runCtx,
			Config:   cfg,
			RunID:    runID,
			LeadTime: assignmentLeadTime,
		},
	})
	if err != nil {
		return report.Summary{}, fmt.Errorf("create worker control server: %w", err)
	}
	grpcServer := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(grpcServer, control)
	httpServer := &http.Server{
		Handler: newHTTPHandler(control, metrics.RunInfo{
			RunID: runID,
			Name:  cfg.Name,
			Model: cfg.Load.Model,
		}),
	}

	logger.Printf(
		"orchestrator listening: address=%s metrics_address=%s run_id=%s min_workers=%d",
		grpcListener.Addr(), metricsListener.Addr(), runID, cfg.Fleet.MinWorkers,
	)
	events := make(chan lifecycleEvent, 4)
	if options.fleetDriver == fleetDriverManual {
		go waitForResult(runCtx, control, events)
		go waitForCancellation(runCtx, events)
		return serveRun(cancel, grpcServer, httpServer, grpcListener, metricsListener, events, httpShutdownPeriod)
	}

	grpcReadyListener, grpcReady := newReadinessListener(grpcListener)
	metricsReadyListener, metricsReady := newReadinessListener(metricsListener)
	startServers(grpcServer, httpServer, grpcReadyListener, metricsReadyListener, events)
	if err := waitForListeners(runCtx, grpcReady, metricsReady, events); err != nil {
		return report.Summary{}, finishManagedSetupFailure(err, cancel, grpcServer, httpServer, nil)
	}

	manager, err := newFleetManager(runCtx, options, runID, factories)
	if err != nil {
		return report.Summary{}, finishManagedSetupFailure(err, cancel, grpcServer, httpServer, nil)
	}
	controller, err := newController(manager, control, cfg, options, logger)
	if err != nil {
		return report.Summary{}, finishManagedSetupFailure(err, cancel, grpcServer, httpServer, manager)
	}
	control.SetAutoscalerMetricsSource(controller)
	if err := controller.Prepare(runCtx); err != nil {
		return report.Summary{}, finishManagedSetupFailure(fmt.Errorf("prepare managed worker fleet: %w", err), cancel, grpcServer, httpServer, manager)
	}

	go func() {
		waitForResult(runCtx, control, events)
	}()
	go waitForCancellation(runCtx, events)
	controllerDone := make(chan struct{})
	go func() {
		defer close(controllerDone)
		runManagedController(runCtx, control, controller, logger)
	}()

	summary, lifecycleErr := coordinateLifecycle(events, cancel, grpcServer, httpServer, httpShutdownPeriod)
	<-controllerDone
	cleanupErr := cleanupManagedFleet(manager, fleetCleanupPeriod)
	return summary, errors.Join(lifecycleErr, cleanupErr)
}

func newDockerFleetManager(ctx context.Context, options docker.Options) (fleet.FleetManager, error) {
	return docker.New(ctx, options)
}

func newKubernetesFleetManager(ctx context.Context, options kubernetes.Options) (fleet.FleetManager, error) {
	return kubernetes.New(ctx, options)
}

func defaultFleetFactories() fleetFactories {
	return fleetFactories{docker: newDockerFleetManager, kubernetes: newKubernetesFleetManager}
}

func newManagedController(
	manager fleet.FleetManager,
	control *orchestrator.Server,
	cfg loadconfig.Config,
	options cliOptions,
	logger *log.Logger,
) (managedController, error) {
	return autoscale.NewController(manager, control, autoscale.ControllerOptions{
		Autoscale:        cfg.Fleet.Autoscale,
		MinWorkers:       cfg.Fleet.MinWorkers,
		MaxWorkers:       cfg.Fleet.MaxWorkers,
		TargetRate:       cfg.Load.Rate,
		RampUp:           cfg.Load.RampUp,
		StartupTimeout:   options.workerStartupTimeout,
		EvaluationPeriod: 5 * time.Second,
		ObservationDelay: metricsDeliveryDelay,
		Policy: autoscale.PolicyConfig{
			Threshold:           0.9,
			RequiredDeficits:    2,
			Cooldown:            15 * time.Second,
			MaximumGrowthFactor: 2,
			MaxWorkers:          cfg.Fleet.MaxWorkers,
		},
	}, logger)
}

func waitForResult(ctx context.Context, control *orchestrator.Server, events chan<- lifecycleEvent) {
	summary, err := control.WaitForResult(ctx, metricsGracePeriod)
	events <- lifecycleEvent{
		kind:   lifecycleResult,
		result: runResult{summary: summary, err: err},
	}
}

func waitForCancellation(ctx context.Context, events chan<- lifecycleEvent) {
	<-ctx.Done()
	events <- lifecycleEvent{kind: lifecycleCanceled}
}

func runManagedController(ctx context.Context, control runWindowSource, controller managedController, logger *log.Logger) {
	window, err := control.WaitForRunWindow(ctx)
	if err != nil {
		if ctx.Err() == nil {
			control.RecordInfrastructureViolation(fmt.Sprintf("wait for autoscale run window: %v", err))
		}
		return
	}
	err = controller.Run(ctx, autoscale.RunWindow{StartsAt: window.StartsAt, Deadline: window.Deadline})
	if err != nil && ctx.Err() == nil {
		control.RecordInfrastructureViolation(fmt.Sprintf("run autoscale controller: %v", err))
		logger.Printf("autoscale controller error: %v", err)
	}
}

func finishManagedSetupFailure(
	setupErr error,
	cancel context.CancelFunc,
	grpcServer grpcServeStopper,
	httpServer httpServeShutdowner,
	manager fleet.FleetManager,
) error {
	serverErr := stopServers(cancel, grpcServer, httpServer, httpShutdownPeriod)
	if manager == nil {
		return errors.Join(setupErr, serverErr)
	}
	return errors.Join(setupErr, serverErr, cleanupManagedFleet(manager, fleetCleanupPeriod))
}

type grpcServeStopper interface {
	Serve(net.Listener) error
	Stop()
}

type httpServeShutdowner interface {
	Serve(net.Listener) error
	Shutdown(context.Context) error
	Close() error
}

type lifecycleEventKind uint8

const (
	lifecycleResult lifecycleEventKind = iota + 1
	lifecycleCanceled
	lifecycleGRPCStopped
	lifecycleHTTPStopped
)

type lifecycleEvent struct {
	kind   lifecycleEventKind
	result runResult
	err    error
}

type serveResult struct {
	name string
	err  error
}

func serveRun(
	cancel context.CancelFunc,
	grpcServer grpcServeStopper,
	httpServer httpServeShutdowner,
	grpcListener net.Listener,
	httpListener net.Listener,
	events chan lifecycleEvent,
	shutdownPeriod time.Duration,
) (report.Summary, error) {
	startServers(grpcServer, httpServer, grpcListener, httpListener, events)
	return coordinateLifecycle(events, cancel, grpcServer, httpServer, shutdownPeriod)
}

func startServers(
	grpcServer grpcServeStopper,
	httpServer httpServeShutdowner,
	grpcListener net.Listener,
	httpListener net.Listener,
	events chan<- lifecycleEvent,
) {
	go func() {
		events <- lifecycleEvent{kind: lifecycleGRPCStopped, err: grpcServer.Serve(grpcListener)}
	}()
	go func() {
		events <- lifecycleEvent{kind: lifecycleHTTPStopped, err: httpServer.Serve(httpListener)}
	}()
}

type readinessListener struct {
	net.Listener
	ready chan struct{}
	once  sync.Once
}

func newReadinessListener(listener net.Listener) (*readinessListener, <-chan struct{}) {
	ready := make(chan struct{})
	return &readinessListener{Listener: listener, ready: ready}, ready
}

func (l *readinessListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.ready) })
	return l.Listener.Accept()
}

func waitForListeners(
	ctx context.Context,
	grpcReady, httpReady <-chan struct{},
	events <-chan lifecycleEvent,
) error {
	for _, ready := range []<-chan struct{}{grpcReady, httpReady} {
		select {
		case <-ready:
		case event := <-events:
			switch event.kind {
			case lifecycleGRPCStopped, lifecycleHTTPStopped:
				return earlyServeError(serveResultFromEvent(event))
			default:
				return fmt.Errorf("unexpected lifecycle event %d before listener readiness", event.kind)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func coordinateLifecycle(
	events <-chan lifecycleEvent,
	cancel context.CancelFunc,
	grpcServer grpcServeStopper,
	httpServer httpServeShutdowner,
	shutdownPeriod time.Duration,
) (report.Summary, error) {
	first := <-events
	shutdownErr := stopServers(cancel, grpcServer, httpServer, shutdownPeriod)

	var (
		completed                runResult
		hasResult                bool
		grpcStopped, httpStopped bool
		stopped                  = make([]serveResult, 0, 2)
	)
	record := func(event lifecycleEvent) {
		switch event.kind {
		case lifecycleResult:
			if !hasResult {
				completed = event.result
				hasResult = true
			}
		case lifecycleGRPCStopped:
			if !grpcStopped {
				grpcStopped = true
				stopped = append(stopped, serveResult{name: "gRPC", err: event.err})
			}
		case lifecycleHTTPStopped:
			if !httpStopped {
				httpStopped = true
				stopped = append(stopped, serveResult{name: "HTTP", err: event.err})
			}
		}
	}
	record(first)

	waitForResult := first.kind == lifecycleCanceled
	for !grpcStopped || !httpStopped || (waitForResult && !hasResult) {
		record(<-events)
	}

	switch first.kind {
	case lifecycleGRPCStopped, lifecycleHTTPStopped:
		return report.Summary{}, earlyServeError(serveResultFromEvent(first))
	case lifecycleResult, lifecycleCanceled:
		return finishRun(completed, shutdownErr, stopped...)
	default:
		return report.Summary{}, fmt.Errorf("unknown lifecycle event kind %d", first.kind)
	}
}

func stopServers(
	cancel context.CancelFunc,
	grpcServer grpcServeStopper,
	httpServer httpServeShutdowner,
	shutdownPeriod time.Duration,
) error {
	cancel()
	grpcServer.Stop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownPeriod)
	defer shutdownCancel()
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	if shutdownErr == nil {
		return nil
	}
	if closeErr := httpServer.Close(); closeErr != nil {
		return errors.Join(shutdownErr, fmt.Errorf("force close HTTP: %w", closeErr))
	}
	return shutdownErr
}

func serveResultFromEvent(event lifecycleEvent) serveResult {
	switch event.kind {
	case lifecycleGRPCStopped:
		return serveResult{name: "gRPC", err: event.err}
	case lifecycleHTTPStopped:
		return serveResult{name: "HTTP", err: event.err}
	default:
		return serveResult{err: event.err}
	}
}

func finishRun(completed runResult, shutdownErr error, stopped ...serveResult) (report.Summary, error) {
	if completed.summary.Status != report.StatusInterrupted {
		for _, result := range stopped {
			if !expectedServeStop(result) {
				return report.Summary{}, fmt.Errorf("serve %s: %w", result.name, result.err)
			}
		}
		if shutdownErr != nil {
			return report.Summary{}, fmt.Errorf("shutdown HTTP: %w", shutdownErr)
		}
	}
	if completed.err != nil {
		return report.Summary{}, completed.err
	}
	return completed.summary, nil
}

func earlyServeError(result serveResult) error {
	if expectedServeStop(result) {
		return fmt.Errorf("%s server stopped before the run completed", result.name)
	}
	return fmt.Errorf("serve %s: %w", result.name, result.err)
}

func expectedServeStop(result serveResult) bool {
	if result.err == nil {
		return true
	}
	switch result.name {
	case "gRPC":
		return errors.Is(result.err, grpc.ErrServerStopped)
	case "HTTP":
		return errors.Is(result.err, http.ErrServerClosed)
	default:
		return false
	}
}

func exitCodeForSummary(summary report.Summary) exitCode {
	switch summary.Status {
	case report.StatusPass:
		return exitPass
	case report.StatusFail:
		return exitTestFailure
	case report.StatusInterrupted:
		return exitInterrupted
	default:
		return exitSetupFailure
	}
}
