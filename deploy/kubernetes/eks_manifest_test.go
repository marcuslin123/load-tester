package kubernetes_test

import (
	"strings"
	"testing"
)

func TestEKSWorkerOverlay(t *testing.T) {
	manifests := renderOverlay(t, "overlays/eks-worker")
	assertEKSWorkload(t, resource(t, manifests, "Deployment", "load-tester-worker"), "worker", "loadgen", "500m", "512Mi", "1", "1Gi")
	assertEKSWorkload(t, resource(t, manifests, "Job", "load-tester-orchestrator"), "orchestrator", "platform", "500m", "512Mi", "1", "1Gi")
	assertEKSWorkload(t, resource(t, manifests, "Deployment", "autoscale-target"), "target", "platform", "250m", "256Mi", "500m", "512Mi")
	assertEKSWorkload(t, resource(t, manifests, "Deployment", "prometheus"), "prometheus", "platform", "500m", "1Gi", "1", "2Gi")
	assertEKSWorkload(t, resource(t, manifests, "Deployment", "grafana"), "grafana", "platform", "250m", "256Mi", "500m", "512Mi")
	assertBool(t, resource(t, manifests, "Job", "load-tester-orchestrator"), true, "spec", "suspend")
	assertInt(t, resource(t, manifests, "Deployment", "load-tester-worker"), 0, "spec", "replicas")
	for _, image := range []string{"example.invalid/load-tester-orchestrator:sha-required", "example.invalid/load-tester-worker:sha-required", "example.invalid/load-tester-target:sha-required"} {
		assertContainsImage(t, manifests, image)
	}
	assertHorizontalPodAutoscalers(t, manifests)
	assertNoPublicEndpoints(t, manifests)
}

func TestEKSHPAOverlay(t *testing.T) {
	manifests := renderOverlay(t, "overlays/eks-hpa")
	assertHorizontalPodAutoscalers(t, manifests, "autoscale-target")
	hpa := resource(t, manifests, "HorizontalPodAutoscaler", "autoscale-target")
	assertString(t, hpa, "spec", "scaleTargetRef", "kind", "Deployment")
	assertString(t, hpa, "spec", "scaleTargetRef", "name", "autoscale-target")
	assertInt(t, hpa, 1, "spec", "minReplicas")
	assertInt(t, hpa, 5, "spec", "maxReplicas")
	assertString(t, hpa, "spec", "metrics", 0, "type", "Resource")
	assertString(t, hpa, "spec", "metrics", 0, "resource", "name", "cpu")
	assertInt(t, hpa, 60, "spec", "metrics", 0, "resource", "target", "averageUtilization")
	metrics := resource(t, manifests, "Deployment", "metrics-server")
	assertNodePool(t, metrics, "platform")
	if strings.Contains(strings.Join(stringList(t, nested(metrics, "spec", "template", "spec", "containers", 0, "args")), " "), "--kubelet-insecure-tls") {
		t.Fatal("EKS Metrics Server must not disable kubelet TLS verification")
	}
	kubeState := resource(t, manifests, "Deployment", "kube-state-metrics")
	assertNodePool(t, kubeState, "platform")
	assertString(t, kubeState, "spec", "template", "spec", "containers", 0, "resources", "requests", "cpu", "10m")
	assertString(t, kubeState, "spec", "template", "spec", "containers", 0, "resources", "requests", "memory", "32Mi")
	assertString(t, kubeState, "spec", "template", "spec", "containers", 0, "resources", "limits", "cpu", "100m")
	assertString(t, kubeState, "spec", "template", "spec", "containers", 0, "resources", "limits", "memory", "64Mi")
	assertTargetAutoscalingDashboard(t, resource(t, manifests, "ConfigMap", "grafana-dashboards"))
	assertNoPublicEndpoints(t, manifests)
}

func assertEKSWorkload(t *testing.T, workload manifest, container, pool, requestCPU, requestMemory, limitCPU, limitMemory string) {
	t.Helper()
	assertNodePool(t, workload, pool)
	containers, _ := nested(workload, "spec", "template", "spec", "containers").([]any)
	for _, raw := range containers {
		if nested(raw, "name") != container {
			continue
		}
		assertString(t, raw, "resources", "requests", "cpu", requestCPU)
		assertString(t, raw, "resources", "requests", "memory", requestMemory)
		assertString(t, raw, "resources", "limits", "cpu", limitCPU)
		assertString(t, raw, "resources", "limits", "memory", limitMemory)
		return
	}
	t.Fatalf("missing container %q", container)
}

func assertNodePool(t *testing.T, workload manifest, want string) {
	t.Helper()
	assertString(t, workload, "spec", "template", "spec", "nodeSelector", "load-tester.io/pool", want)
}

func assertContainsImage(t *testing.T, manifests []manifest, want string) {
	t.Helper()
	for _, value := range manifests {
		for _, key := range []string{"containers", "initContainers"} {
			containers, _ := nested(value, "spec", "template", "spec", key).([]any)
			for _, container := range containers {
				if nested(container, "image") == want {
					return
				}
			}
		}
	}
	t.Errorf("missing image %q", want)
}

func assertNoPublicEndpoints(t *testing.T, manifests []manifest) {
	t.Helper()
	for _, value := range manifests {
		if nested(value, "kind") == "Ingress" {
			t.Errorf("Ingress %q is not allowed", nested(value, "metadata", "name"))
		}
		if nested(value, "kind") == "Service" && nested(value, "spec", "type") == "LoadBalancer" {
			t.Errorf("LoadBalancer Service %q is not allowed", nested(value, "metadata", "name"))
		}
	}
}

func assertHorizontalPodAutoscalers(t *testing.T, manifests []manifest, want ...string) {
	t.Helper()
	got := make(map[string]bool)
	for _, value := range manifests {
		if nested(value, "kind") == "HorizontalPodAutoscaler" {
			got[nested(value, "metadata", "name").(string)] = true
		}
	}
	if len(got) != len(want) {
		t.Errorf("HorizontalPodAutoscalers = %v, want %v", got, want)
		return
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("HorizontalPodAutoscalers = %v, want %q", got, name)
		}
	}
}
