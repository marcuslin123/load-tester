package scripts_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestKindHPAScriptHasValidShellSyntax(t *testing.T) {
	command := exec.Command("bash", "-n", "test-kind-hpa.sh")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("bash -n test-kind-hpa.sh: %v\n%s", err, output)
	}
}

func TestKindHPAScriptCoversAcceptanceContract(t *testing.T) {
	contents, err := os.ReadFile("test-kind-hpa.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(contents)

	for _, want := range []string{
		"KEEP_KIND_CLUSTER",
		"kubectl top pods",
		"apis/metrics.k8s.io/v1beta1",
		"MIN_TARGET_REPLICAS=3",
		"MAX_TARGET_REPLICAS=5",
		"MIN_THROUGHPUT_RATIO=0.90",
		"MIN_P95_IMPROVEMENT=0.20",
		"loadtester_requests_total",
		"loadtester_request_duration_seconds_bucket",
		"Result: PASS",
		"Unmet demand: 0",
		"Dropped samples: 0",
		"Target Autoscaling",
		"target-autoscaling",
		"deployment/autoscale-target",
		"deployment/load-tester-worker",
		"horizontalpodautoscaler/autoscale-target",
		"kubectl_kind get events",
		"kubectl_kind logs",
		"registry.k8s.io/metrics-server/metrics-server:v0.9.0",
		"registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.19.1",
		"crictl pull",
		": >\"$SAMPLES_FILE\"",
		"capacity_limited_since",
		"current >= MAX_TARGET_REPLICAS",
		"max_capacity_limited_seconds >= MAX_CAPACITY_LIMITED_SECONDS",
		"max_worker_desired",
		"max_worker_ready",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("test-kind-hpa.sh does not contain %q", want)
		}
	}
}

func TestKindHPAScriptKeepsLifecycleProofSeparate(t *testing.T) {
	contents, err := os.ReadFile("test-kind-hpa.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(contents)
	for _, want := range []string{
		"deploy/kubernetes/target-hpa/demo.yaml",
		"deploy/kubernetes/overlays/kind-hpa",
		"workers=0",
		"target=1",
		"cluster=deleted",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("test-kind-hpa.sh does not contain %q", want)
		}
	}
}
