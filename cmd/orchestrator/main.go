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
	"syscall"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	loadconfig "github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/metrics"
	"github.com/marcuslin123/load-tester/internal/orchestrator"
	"github.com/marcuslin123/load-tester/internal/report"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

const (
	heartbeatInterval  = 3 * time.Second
	assignmentLeadTime = time.Second
	metricsGracePeriod = 2 * time.Second
	httpShutdownPeriod = 5 * time.Second
)

type exitCode int

const (
	exitPass         exitCode = 0
	exitTestFailure  exitCode = 1
	exitSetupFailure exitCode = 2
	exitInterrupted  exitCode = 130
)

type cliOptions struct {
	address        string
	metricsAddress string
	configPath     string
}

type runResult struct {
	summary report.Summary
	err     error
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
	runID, err := newRunID()
	if err != nil {
		fmt.Fprintf(stderr, "orchestrator: generate run ID: %v\n", err)
		return exitSetupFailure
	}
	summary, err := run(ctx, options.address, options.metricsAddress, cfg, runID, logger)
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

func parseOptions(args []string) (cliOptions, error) {
	flags := flag.NewFlagSet("orchestrator", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("addr", ":9090", "gRPC listen address")
	metricsAddress := flags.String("metrics-addr", ":9091", "metrics and health listen address")
	configPath := flags.String("config", "", "path to the load-test YAML file")
	if err := flags.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if flags.NArg() != 0 {
		return cliOptions{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *configPath == "" {
		return cliOptions{}, errors.New("-config is required")
	}
	return cliOptions{address: *address, metricsAddress: *metricsAddress, configPath: *configPath}, nil
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
	grpcListener, err := net.Listen("tcp", address)
	if err != nil {
		return report.Summary{}, fmt.Errorf("listen on %s: %w", address, err)
	}
	defer grpcListener.Close()
	metricsListener, err := net.Listen("tcp", metricsAddress)
	if err != nil {
		return report.Summary{}, fmt.Errorf("listen metrics on %s: %w", metricsAddress, err)
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
	go func() {
		summary, err := control.WaitForResult(runCtx, metricsGracePeriod)
		events <- lifecycleEvent{
			kind:   lifecycleResult,
			result: runResult{summary: summary, err: err},
		}
	}()
	go func() {
		<-runCtx.Done()
		events <- lifecycleEvent{kind: lifecycleCanceled}
	}()
	return serveRun(cancel, grpcServer, httpServer, grpcListener, metricsListener, events, httpShutdownPeriod)
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
	go func() {
		events <- lifecycleEvent{kind: lifecycleGRPCStopped, err: grpcServer.Serve(grpcListener)}
	}()
	go func() {
		events <- lifecycleEvent{kind: lifecycleHTTPStopped, err: httpServer.Serve(httpListener)}
	}()
	return coordinateLifecycle(events, cancel, grpcServer, httpServer, shutdownPeriod)
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
