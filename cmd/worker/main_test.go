package main

import (
	"strings"
	"testing"
)

func TestLoadConfigUsesEnvironmentAndHostname(t *testing.T) {
	t.Parallel()

	environment := map[string]string{
		"ORCHESTRATOR_ADDR": "orchestrator:9090",
		"WORKER_ID":         "worker-override",
	}
	config, err := loadConfig(
		func(name string) string { return environment[name] },
		func() (string, error) { return "pod-hostname", nil },
	)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if config.Address != "orchestrator:9090" {
		t.Errorf("Address = %q, want orchestrator:9090", config.Address)
	}
	if config.WorkerID != "worker-override" {
		t.Errorf("WorkerID = %q, want worker-override", config.WorkerID)
	}
	if config.Hostname != "pod-hostname" {
		t.Errorf("Hostname = %q, want pod-hostname", config.Hostname)
	}
	if config.SoftwareVersion != "dev" {
		t.Errorf("SoftwareVersion = %q, want dev", config.SoftwareVersion)
	}
}

func TestLoadConfigDefaultsWorkerIDToHostname(t *testing.T) {
	t.Parallel()

	config, err := loadConfig(
		func(name string) string {
			if name == "ORCHESTRATOR_ADDR" {
				return "orchestrator:9090"
			}
			return ""
		},
		func() (string, error) { return "pod-hostname", nil },
	)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if config.WorkerID != "pod-hostname" {
		t.Fatalf("WorkerID = %q, want pod-hostname", config.WorkerID)
	}
}

func TestLoadConfigRequiresOrchestratorAddress(t *testing.T) {
	t.Parallel()

	_, err := loadConfig(
		func(string) string { return "" },
		func() (string, error) { return "pod-hostname", nil },
	)
	if err == nil || !strings.Contains(err.Error(), "ORCHESTRATOR_ADDR") {
		t.Fatalf("loadConfig() error = %v, want ORCHESTRATOR_ADDR error", err)
	}
}
