package eks_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
)

func TestEKSWorkerScenarios(t *testing.T) {
	baseline := parseScenario(t, "scenarios/worker-baseline.yaml", "")
	assertScenario(t, baseline, "/echo", 100, 1000, 90*time.Second, 20*time.Second, 1, 1, false)
	proof := parseScenario(t, "scenarios/worker-autoscale.yaml.tmpl", "150")
	assertScenario(t, proof, "/echo", 150, 1000, 3*time.Minute, 30*time.Second, 1, 4, true)
}

func TestEKSTargetHPAScenarios(t *testing.T) {
	baseline := parseScenario(t, "scenarios/target-baseline.yaml", "")
	assertScenario(t, baseline, "/burn?ms=20", 30, 500, 2*time.Minute, 10*time.Second, 2, 2, false)
	if baseline.Target.Headers["Connection"] != "close" {
		t.Fatalf("Connection = %q, want close", baseline.Target.Headers["Connection"])
	}
	proof := parseScenario(t, "scenarios/target-hpa.yaml.tmpl", "45")
	assertScenario(t, proof, "/burn?ms=20", 45, 500, 2*time.Minute, 10*time.Second, 2, 2, false)
	if proof.Target.Headers["Connection"] != "close" {
		t.Fatalf("Connection = %q, want close", proof.Target.Headers["Connection"])
	}
}

func parseScenario(t *testing.T, path, rate string) config.Config {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if rate != "" {
		raw = []byte(strings.ReplaceAll(string(raw), "__RATE__", rate))
	}
	cfg, err := config.Parse(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return cfg
}

func assertScenario(t *testing.T, cfg config.Config, endpoint string, rate, maxInFlight int, duration, ramp time.Duration, minWorkers, maxWorkers int, autoscale bool) {
	t.Helper()
	if cfg.Target.Protocol != config.ProtocolHTTP || !strings.HasSuffix(cfg.Target.URL, endpoint) {
		t.Errorf("target = %+v, want HTTP %s", cfg.Target, endpoint)
	}
	if cfg.Load.Model != config.LoadConstantRate || cfg.Load.Rate != rate || cfg.Load.MaxInFlight != maxInFlight || cfg.Load.Duration != duration || cfg.Load.RampUp != ramp {
		t.Errorf("load = %+v", cfg.Load)
	}
	if cfg.Fleet.MinWorkers != minWorkers || cfg.Fleet.MaxWorkers != maxWorkers || cfg.Fleet.Autoscale != autoscale {
		t.Errorf("fleet = %+v", cfg.Fleet)
	}
}
