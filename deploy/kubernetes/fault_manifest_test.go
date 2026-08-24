package kubernetes_test

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
)

func TestFaultInjectionScenario(t *testing.T) {
	cfg := parseScenarioFile(t)

	if cfg.Target.URL != "http://autoscale-target:8080/delay?ms=20" {
		t.Fatalf("target URL = %q, want controlled delay endpoint", cfg.Target.URL)
	}
	if cfg.Load.Model != config.LoadConstantRate || cfg.Load.Rate != 90 ||
		cfg.Load.MaxInFlight != 300 || cfg.Load.Duration != 2*time.Minute ||
		cfg.Load.RampUp != 10*time.Second {
		t.Fatalf("load = %+v, want 90 RPS for 2m with 10s ramp and max_in_flight 300", cfg.Load)
	}
	if cfg.Fleet.MinWorkers != 3 || cfg.Fleet.MaxWorkers != 3 || cfg.Fleet.Autoscale {
		t.Fatalf("fleet = %+v, want exactly three fixed workers", cfg.Fleet)
	}
}

func TestKindFaultOverlayRendersFixedExperiment(t *testing.T) {
	manifests := renderOverlay(t, "overlays/kind-fault")

	orchestrator := resource(t, manifests, "Job", "load-tester-orchestrator")
	assertBool(t, orchestrator, true, "spec", "suspend")

	worker := resource(t, manifests, "Deployment", "load-tester-worker")
	assertInt(t, worker, 0, "spec", "replicas")
	assertContainerImage(t, worker, "worker", "load-tester-worker:kind")

	target := resource(t, manifests, "Deployment", "autoscale-target")
	assertInt(t, target, 1, "spec", "replicas")
	assertContainerImage(t, target, "target", "load-tester-target:kind")

	configMap := resource(t, manifests, "ConfigMap", "load-test-config")
	assertString(t, configMap, "metadata", "namespace", "load-tester")
	rawScenario, ok := nested(configMap, "data", "demo.yaml").(string)
	if !ok {
		t.Fatal("load-test-config.data[demo.yaml] is missing")
	}
	got, err := config.Parse(strings.NewReader(rawScenario))
	if err != nil {
		t.Fatalf("parse rendered fault scenario: %v", err)
	}
	expected := parseScenarioFile(t)
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("rendered scenario = %+v, want %+v", got, expected)
	}
}

func parseScenarioFile(t *testing.T) config.Config {
	t.Helper()
	file, err := os.Open("fault-injection/demo.yaml")
	if err != nil {
		t.Fatalf("open fault scenario: %v", err)
	}
	defer file.Close()

	cfg, err := config.Parse(file)
	if err != nil {
		t.Fatalf("parse fault scenario: %v", err)
	}
	return cfg
}
