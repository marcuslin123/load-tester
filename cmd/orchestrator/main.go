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
	"google.golang.org/grpc"
)

const (
	heartbeatInterval  = 3 * time.Second
	assignmentLeadTime = time.Second
)

type cliOptions struct {
	address    string
	configPath string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	options, err := parseOptions(os.Args[1:])
	if err != nil {
		log.Printf("orchestrator: %v", err)
		os.Exit(2)
	}
	cfg, err := loadTestConfig(options.configPath)
	if err != nil {
		log.Printf("orchestrator: %v", err)
		os.Exit(2)
	}
	runID, err := newRunID()
	if err != nil {
		log.Printf("orchestrator: generate run ID: %v", err)
		os.Exit(1)
	}
	if err := run(ctx, options.address, cfg, runID, log.Default()); err != nil {
		log.Printf("orchestrator: %v", err)
		os.Exit(1)
	}
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

func run(ctx context.Context, address string, cfg loadconfig.Config, runID string, logger *log.Logger) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	defer listener.Close()

	control, err := orchestrator.NewServer(orchestrator.Options{
		HeartbeatInterval: heartbeatInterval,
		Logger:            logger,
		Assignment: &orchestrator.AssignmentOptions{
			Context:  ctx,
			Config:   cfg,
			RunID:    runID,
			LeadTime: assignmentLeadTime,
		},
	})
	if err != nil {
		return fmt.Errorf("create worker control server: %w", err)
	}
	server := grpc.NewServer()
	loadtestv1.RegisterWorkerControlServer(server, control)
	go func() {
		<-ctx.Done()
		server.Stop()
	}()

	logger.Printf("orchestrator listening: address=%s run_id=%s min_workers=%d", listener.Addr(), runID, cfg.Fleet.MinWorkers)
	err = server.Serve(listener)
	if ctx.Err() != nil || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return fmt.Errorf("serve gRPC: %w", err)
}
