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
	"os"
	"os/signal"
	"syscall"
	"time"

	loadtestv1 "github.com/marcuslin123/load-tester/gen/loadtest/v1"
	loadconfig "github.com/marcuslin123/load-tester/internal/config"
	"github.com/marcuslin123/load-tester/internal/orchestrator"
	"github.com/marcuslin123/load-tester/internal/report"
	"google.golang.org/grpc"
)

const (
	heartbeatInterval  = 3 * time.Second
	assignmentLeadTime = time.Second
	metricsGracePeriod = 2 * time.Second
)

type exitCode int

const (
	exitPass         exitCode = 0
	exitTestFailure  exitCode = 1
	exitSetupFailure exitCode = 2
	exitInterrupted  exitCode = 130
)

type cliOptions struct {
	address    string
	configPath string
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
	summary, err := run(ctx, options.address, cfg, runID, logger)
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
	return cliOptions{address: *address, configPath: *configPath}, nil
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
	cfg loadconfig.Config,
	runID string,
	logger *log.Logger,
) (report.Summary, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return report.Summary{}, fmt.Errorf("listen on %s: %w", address, err)
	}
	defer listener.Close()
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
	server := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(server, control)
	go func() {
		<-runCtx.Done()
		server.Stop()
	}()

	logger.Printf("orchestrator listening: address=%s run_id=%s min_workers=%d", listener.Addr(), runID, cfg.Fleet.MinWorkers)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	results := make(chan runResult, 1)
	go func() {
		summary, err := control.WaitForResult(runCtx, metricsGracePeriod)
		results <- runResult{summary: summary, err: err}
	}()
	return awaitRunCompletion(runCtx, results, serveErrors, func() {
		cancel()
		server.Stop()
	})
}

func awaitRunCompletion(
	ctx context.Context,
	results <-chan runResult,
	serveErrors <-chan error,
	stopServer func(),
) (report.Summary, error) {
	select {
	case completed := <-results:
		stopServer()
		serveErr := <-serveErrors
		if serveErr != nil &&
			!errors.Is(serveErr, grpc.ErrServerStopped) &&
			completed.summary.Status != report.StatusInterrupted {
			return report.Summary{}, fmt.Errorf("serve gRPC: %w", serveErr)
		}
		if completed.err != nil {
			return report.Summary{}, completed.err
		}
		return completed.summary, nil
	case serveErr := <-serveErrors:
		if ctx.Err() != nil {
			completed := <-results
			if completed.err != nil {
				return report.Summary{}, completed.err
			}
			return completed.summary, nil
		}
		stopServer()
		if serveErr == nil || errors.Is(serveErr, grpc.ErrServerStopped) {
			return report.Summary{}, errors.New("gRPC server stopped before the run completed")
		}
		return report.Summary{}, fmt.Errorf("serve gRPC: %w", serveErr)
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
