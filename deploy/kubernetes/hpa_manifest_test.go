package kubernetes_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/marcuslin123/load-tester/internal/config"
)

func TestKindHPAOverlayRendersTargetAutoscalingStack(t *testing.T) {
	manifests := renderOverlay(t, "overlays/kind-hpa")

	hpa := resource(t, manifests, "HorizontalPodAutoscaler", "autoscale-target")
	assertString(t, hpa, "apiVersion", "autoscaling/v2")
	assertString(t, hpa, "spec", "scaleTargetRef", "apiVersion", "apps/v1")
	assertString(t, hpa, "spec", "scaleTargetRef", "kind", "Deployment")
	assertString(t, hpa, "spec", "scaleTargetRef", "name", "autoscale-target")
	assertInt(t, hpa, 1, "spec", "minReplicas")
	assertInt(t, hpa, 5, "spec", "maxReplicas")
	assertString(t, hpa, "spec", "metrics", 0, "type", "Resource")
	assertString(t, hpa, "spec", "metrics", 0, "resource", "name", "cpu")
	assertString(t, hpa, "spec", "metrics", 0, "resource", "target", "type", "Utilization")
	assertInt(t, hpa, 60, "spec", "metrics", 0, "resource", "target", "averageUtilization")
	assertInt(t, hpa, 0, "spec", "behavior", "scaleUp", "stabilizationWindowSeconds")
	assertInt(t, hpa, 60, "spec", "behavior", "scaleDown", "stabilizationWindowSeconds")
	assertHPAPolicy(t, hpa, "scaleUp", "Percent", 100, 15)
	assertHPAPolicy(t, hpa, "scaleUp", "Pods", 2, 15)
	assertHPAPolicy(t, hpa, "scaleDown", "Pods", 1, 60)

	metricsServer := resource(t, manifests, "Deployment", "metrics-server")
	assertString(t, metricsServer, "metadata", "namespace", "kube-system")
	assertContainerImage(t, metricsServer, "metrics-server", "registry.k8s.io/metrics-server/metrics-server:v0.9.0")
	assertContains(t, stringList(t, nested(metricsServer, "spec", "template", "spec", "containers", 0, "args")), "--kubelet-insecure-tls", "kind Metrics Server arguments")

	kubeStateMetrics := resource(t, manifests, "Deployment", "kube-state-metrics")
	assertString(t, kubeStateMetrics, "metadata", "namespace", "load-tester")
	assertContainerImage(t, kubeStateMetrics, "kube-state-metrics", "registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.19.1")
	args := stringList(t, nested(kubeStateMetrics, "spec", "template", "spec", "containers", 0, "args"))
	assertContains(t, args, "--namespaces=load-tester", "kube-state-metrics arguments")
	assertContains(t, args, "--resources=horizontalpodautoscalers", "kube-state-metrics arguments")
	assertInt(t, kubeStateMetrics, 65534, "spec", "template", "spec", "containers", 0, "securityContext", "runAsUser")
	assertInt(t, kubeStateMetrics, 8081, "spec", "template", "spec", "containers", 0, "ports", 1, "containerPort")
	assertString(t, kubeStateMetrics, "spec", "template", "spec", "containers", 0, "ports", 1, "name", "telemetry")
	assertString(t, kubeStateMetrics, "spec", "template", "spec", "containers", 0, "readinessProbe", "httpGet", "port", "telemetry")
	assertString(t, kubeStateMetrics, "spec", "template", "spec", "containers", 0, "livenessProbe", "httpGet", "port", "metrics")

	role := resource(t, manifests, "Role", "kube-state-metrics")
	assertRoleRule(t, role, "autoscaling", "horizontalpodautoscalers", "list", "watch")
	if len(nested(role, "rules").([]any)) != 1 {
		t.Fatalf("kube-state-metrics Role must contain exactly one rule")
	}
	assertNoResource(t, manifests, "ClusterRole", "kube-state-metrics")

	prometheusConfig := resource(t, manifests, "ConfigMap", "prometheus-config")
	assertStringContains(t, prometheusConfig, "data", "prometheus.yml", "kube-state-metrics:8080")

	dashboards := resource(t, manifests, "ConfigMap", "grafana-dashboards")
	assertDashboardMatchesCompose(t, dashboards)
	assertTargetAutoscalingDashboard(t, dashboards)
}

func TestReusableHPAUsesConservativeScaleDown(t *testing.T) {
	manifests := renderOverlay(t, "target-hpa")
	hpa := resource(t, manifests, "HorizontalPodAutoscaler", "autoscale-target")
	assertInt(t, hpa, 300, "spec", "behavior", "scaleDown", "stabilizationWindowSeconds")
}

func TestMetricsServerBaseDoesNotDisableKubeletTLSVerification(t *testing.T) {
	contents, err := os.ReadFile("addons/metrics-server/components.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "--kubelet-insecure-tls") {
		t.Fatal("reusable Metrics Server manifest must verify kubelet TLS")
	}
	if !strings.Contains(string(contents), "Upstream artifact SHA-256 (before local YAML formatting): 1cec29a5267809306a2c6ec74a3e449abbb705b4a8beed0c8a1963910f72c79b") {
		t.Fatal("Metrics Server manifest must record its pinned upstream artifact checksum")
	}
}

func TestTargetHPAScenario(t *testing.T) {
	file, err := os.Open("target-hpa/demo.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	cfg, err := config.Parse(file)
	if err != nil {
		t.Fatalf("parse target HPA scenario: %v", err)
	}
	if cfg.Target.URL != "http://autoscale-target:8080/burn?ms=20" || cfg.Target.Headers["Connection"] != "close" {
		t.Fatalf("target = %+v, want CPU-burn endpoint with Connection: close", cfg.Target)
	}
	if cfg.Load.Model != config.LoadConstantRate || cfg.Load.Rate != 30 || cfg.Load.MaxInFlight != 500 || cfg.Load.Duration != 2*time.Minute || cfg.Load.RampUp != 10*time.Second {
		t.Fatalf("load = %+v, want 30 RPS for 2m with 10s ramp and max_in_flight 500", cfg.Load)
	}
	if cfg.Fleet.MinWorkers != 2 || cfg.Fleet.MaxWorkers != 2 || cfg.Fleet.Autoscale {
		t.Fatalf("fleet = %+v, want exactly two fixed workers", cfg.Fleet)
	}
}

func assertHPAPolicy(t *testing.T, hpa manifest, direction, policyType string, value, period int) {
	t.Helper()
	policies, _ := nested(hpa, "spec", "behavior", direction, "policies").([]any)
	for _, policy := range policies {
		if nested(policy, "type") == policyType && nested(policy, "value") == value && nested(policy, "periodSeconds") == period {
			return
		}
	}
	t.Errorf("missing %s HPA policy %s=%d per %ds", direction, policyType, value, period)
}

func assertNoResource(t *testing.T, manifests []manifest, kind, name string) {
	t.Helper()
	for _, value := range manifests {
		if nested(value, "kind") == kind && nested(value, "metadata", "name") == name {
			t.Errorf("unexpected %s %q", kind, name)
		}
	}
}

func assertTargetAutoscalingDashboard(t *testing.T, dashboardConfig manifest) {
	t.Helper()
	raw, _ := nested(dashboardConfig, "data", "target-autoscaling.json").(string)
	var dashboard struct {
		Title  string `json:"title"`
		UID    string `json:"uid"`
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Expression string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal([]byte(raw), &dashboard); err != nil {
		t.Fatalf("decode target autoscaling dashboard: %v", err)
	}
	if dashboard.Title != "Target Autoscaling" || dashboard.UID != "target-autoscaling" {
		t.Fatalf("dashboard title/uid = %q/%q", dashboard.Title, dashboard.UID)
	}
	joined := raw
	for _, want := range []string{
		"Current / Desired / Bounds",
		"CPU Utilization vs Target",
		"HPA Conditions",
		"Request Rate",
		"Latency Percentiles",
		"Error Rate",
		"kube_horizontalpodautoscaler_status_current_replicas",
		"kube_horizontalpodautoscaler_status_desired_replicas",
		"kube_horizontalpodautoscaler_spec_target_metric",
		"loadtester_request_duration_seconds",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("target autoscaling dashboard does not contain %q", want)
		}
	}
}
