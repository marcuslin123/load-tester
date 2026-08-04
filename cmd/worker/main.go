package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	workerclient "github.com/marcuslin123/load-tester/internal/worker"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config, err := loadConfig(os.Getenv, os.Hostname)
	if err != nil {
		log.Printf("worker: %v", err)
		os.Exit(2)
	}
	log.Printf("worker starting: id=%s orchestrator=%s", config.WorkerID, config.Address)
	if err := workerclient.Run(ctx, config, workerclient.Options{Logger: log.Default()}); err != nil {
		log.Printf("worker: %v", err)
		os.Exit(1)
	}
}

func loadConfig(getenv func(string) string, hostname func() (string, error)) (workerclient.Config, error) {
	address := strings.TrimSpace(getenv("ORCHESTRATOR_ADDR"))
	if address == "" {
		return workerclient.Config{}, errors.New("ORCHESTRATOR_ADDR is required")
	}
	host, err := hostname()
	if err != nil {
		return workerclient.Config{}, fmt.Errorf("read hostname: %w", err)
	}
	workerID := strings.TrimSpace(getenv("WORKER_ID"))
	if workerID == "" {
		workerID = host
	}
	return workerclient.Config{
		Address:         address,
		WorkerID:        workerID,
		Hostname:        host,
		SoftwareVersion: version,
	}, nil
}
