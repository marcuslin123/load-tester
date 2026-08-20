package kubernetes_test

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type manifest map[string]any

func TestKindOverlayRendersFleetStack(t *testing.T) {
	manifests := renderKindOverlay(t)

	assertString(t, resource(t, manifests, "Namespace", "load-tester"), "metadata", "name", "load-tester")

	orchestrator := resource(t, manifests, "Job", "load-tester-orchestrator")
	assertInt(t, orchestrator, 0, "spec", "backoffLimit")
	assertBool(t, orchestrator, true, "spec", "suspend")
	assertString(t, orchestrator, "spec", "template", "spec", "serviceAccountName", "load-tester-orchestrator")
	assertBool(t, orchestrator, false, "spec", "template", "spec", "automountServiceAccountToken")
	assertContainerImage(t, orchestrator, "orchestrator", "load-tester-orchestrator:kind")
	assertContainerImage(t, orchestrator, "wait-for-target", "busybox:1.37.0")
	assertContains(t, stringList(t, nested(orchestrator, "spec", "template", "spec", "containers", 0, "args")), "-fleet-driver", "orchestrator arguments")
	assertContains(t, stringList(t, nested(orchestrator, "spec", "template", "spec", "containers", 0, "args")), "kubernetes", "orchestrator arguments")
	assertContains(t, stringList(t, nested(orchestrator, "spec", "template", "spec", "containers", 0, "args")), "-kubernetes-namespace", "orchestrator arguments")
	assertContains(t, stringList(t, nested(orchestrator, "spec", "template", "spec", "containers", 0, "args")), "load-tester", "orchestrator arguments")
	assertContains(t, stringList(t, nested(orchestrator, "spec", "template", "spec", "containers", 0, "args")), "-kubernetes-worker-deployment", "orchestrator arguments")
	assertContains(t, stringList(t, nested(orchestrator, "spec", "template", "spec", "containers", 0, "args")), "load-tester-worker", "orchestrator arguments")
	assertConfigMapVolume(t, orchestrator, "load-test-config")
	assertVolumeMount(t, orchestrator, "orchestrator", "load-test-config", "/app/config")
	assertProjectedServiceAccountToken(t, orchestrator, "kube-api-access")
	assertVolumeMount(t, orchestrator, "orchestrator", "kube-api-access", "/var/run/secrets/kubernetes.io/serviceaccount")
	assertContainerDoesNotMountVolume(t, orchestrator, "wait-for-target", "kube-api-access")

	serviceAccount := resource(t, manifests, "ServiceAccount", "load-tester-orchestrator")
	assertBool(t, serviceAccount, false, "automountServiceAccountToken")

	worker := resource(t, manifests, "Deployment", "load-tester-worker")
	assertString(t, worker, "metadata", "labels", "io.github.marcuslin123.load-tester.managed", "true")
	assertInt(t, worker, 0, "spec", "replicas")
	assertBool(t, worker, false, "spec", "template", "spec", "automountServiceAccountToken")
	assertContainerImage(t, worker, "worker", "load-tester-worker:kind")
	assertString(t, worker, "spec", "template", "spec", "containers", 0, "resources", "limits", "cpu", "250m")
	assertWorkerIdentityEnvironment(t, worker)

	target := resource(t, manifests, "Deployment", "autoscale-target")
	assertBool(t, target, false, "spec", "template", "spec", "automountServiceAccountToken")
	assertContainerImage(t, target, "target", "load-tester-target:kind")
	assertServicePort(t, resource(t, manifests, "Service", "autoscale-target"), "http", 8080)

	orchestratorService := resource(t, manifests, "Service", "load-tester-orchestrator")
	assertServicePort(t, orchestratorService, "grpc", 9090)
	assertServicePort(t, orchestratorService, "metrics", 9091)

	role := resource(t, manifests, "Role", "load-tester-orchestrator")
	assertRoleRule(t, role, "apps", "deployments", "get")
	assertRoleRule(t, role, "apps", "deployments/scale", "get", "update")
	assertRoleRule(t, role, "", "pods", "get", "list", "delete")
	assertRoleResourceNames(t, role, "apps", "deployments", "load-tester-worker")
	assertRoleResourceNames(t, role, "apps", "deployments/scale", "load-tester-worker")
	roleBinding := resource(t, manifests, "RoleBinding", "load-tester-orchestrator")
	assertString(t, roleBinding, "roleRef", "kind", "Role")
	assertString(t, roleBinding, "roleRef", "name", "load-tester-orchestrator")
	assertString(t, roleBinding, "subjects", 0, "kind", "ServiceAccount")
	assertString(t, roleBinding, "subjects", 0, "name", "load-tester-orchestrator")
	assertString(t, roleBinding, "subjects", 0, "namespace", "load-tester")

	prometheus := resource(t, manifests, "Deployment", "prometheus")
	assertBool(t, prometheus, false, "spec", "template", "spec", "automountServiceAccountToken")
	assertContainerImage(t, prometheus, "prometheus", "prom/prometheus:v3.13.1")
	assertStringContains(t, resource(t, manifests, "ConfigMap", "prometheus-config"), "data", "prometheus.yml", "load-tester-orchestrator:9091")

	grafana := resource(t, manifests, "Deployment", "grafana")
	assertBool(t, grafana, false, "spec", "template", "spec", "automountServiceAccountToken")
	assertContainerImage(t, grafana, "grafana", "grafana/grafana:13.1.0")
	assertStringContains(t, resource(t, manifests, "ConfigMap", "grafana-dashboards"), "data", "load-tester-overview.json", "Load Tester Overview")
	assertDashboardMatchesCompose(t, resource(t, manifests, "ConfigMap", "grafana-dashboards"))
	assertConfigMapVolume(t, grafana, "grafana-dashboard-provisioning")
	assertConfigMapVolume(t, grafana, "grafana-dashboards")
	assertVolumeMount(t, grafana, "grafana", "dashboards-provisioning", "/etc/grafana/provisioning/dashboards/dashboards.yml")
	assertVolumeMount(t, grafana, "grafana", "dashboards", "/var/lib/grafana/dashboards")
}

func assertDashboardMatchesCompose(t *testing.T, dashboardConfig manifest) {
	t.Helper()
	want, err := os.ReadFile("../compose/grafana/dashboards/load-tester-overview.json")
	if err != nil {
		t.Fatalf("read Compose dashboard: %v", err)
	}
	got, _ := nested(dashboardConfig, "data", "load-tester-overview.json").(string)
	if strings.TrimSpace(got) != strings.TrimSpace(string(want)) {
		t.Fatal("Kubernetes dashboard differs from the provisioned Compose dashboard")
	}
}

func renderKindOverlay(t *testing.T) []manifest {
	t.Helper()
	command := exec.Command("kubectl", "kustomize", "overlays/kind")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render kind overlay: %v\n%s", err, output)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(output))
	var manifests []manifest
	for {
		var value manifest
		err := decoder.Decode(&value)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("decode rendered manifest: %v", err)
		}
		if len(value) != 0 {
			manifests = append(manifests, value)
		}
	}
	return manifests
}

func resource(t *testing.T, manifests []manifest, kind, name string) manifest {
	t.Helper()
	for _, value := range manifests {
		if nested(value, "kind") == kind && nested(value, "metadata", "name") == name {
			return value
		}
	}
	t.Fatalf("missing %s %q", kind, name)
	return nil
}

func nested(value any, path ...any) any {
	for _, key := range path {
		switch typed := value.(type) {
		case manifest:
			key, ok := key.(string)
			if !ok {
				return nil
			}
			value = typed[key]
		case map[string]any:
			key, ok := key.(string)
			if !ok {
				return nil
			}
			value = typed[key]
		case []any:
			index, ok := key.(int)
			if !ok || index < 0 || index >= len(typed) {
				return nil
			}
			value = typed[index]
		default:
			return nil
		}
	}
	return value
}

func assertString(t *testing.T, value any, path ...any) {
	t.Helper()
	want, ok := path[len(path)-1].(string)
	if !ok {
		t.Fatalf("test error: final assertion value must be a string")
	}
	got, ok := nested(value, path[:len(path)-1]...).(string)
	if !ok || got != want {
		t.Errorf("%s = %q, want %q", formatPath(path[:len(path)-1]), got, want)
	}
}

func assertStringContains(t *testing.T, value any, path ...string) {
	t.Helper()
	want := path[len(path)-1]
	keys := make([]any, len(path)-1)
	for index, key := range path[:len(path)-1] {
		keys[index] = key
	}
	got, _ := nested(value, keys...).(string)
	if !strings.Contains(got, want) {
		t.Errorf("%s does not contain %q", strings.Join(path[:len(path)-1], "."), want)
	}
}

func assertInt(t *testing.T, value any, want int, path ...any) {
	t.Helper()
	got, ok := nested(value, path...).(int)
	if !ok || got != want {
		t.Errorf("%s = %v, want %d", formatPath(path), got, want)
	}
}

func assertBool(t *testing.T, value any, want bool, path ...any) {
	t.Helper()
	got, ok := nested(value, path...).(bool)
	if !ok || got != want {
		t.Errorf("%s = %v, want %t", formatPath(path), got, want)
	}
}

func assertContainerImage(t *testing.T, resource manifest, name, want string) {
	t.Helper()
	containers, _ := nested(resource, "spec", "template", "spec", "containers").([]any)
	for _, container := range containers {
		if nested(container, "name") == name {
			assertString(t, container, "image", want)
			return
		}
	}
	initContainers, _ := nested(resource, "spec", "template", "spec", "initContainers").([]any)
	for _, container := range initContainers {
		if nested(container, "name") == name {
			assertString(t, container, "image", want)
			return
		}
	}
	t.Errorf("missing container %q", name)
}

func assertConfigMapVolume(t *testing.T, resource manifest, want string) {
	t.Helper()
	volumes, _ := nested(resource, "spec", "template", "spec", "volumes").([]any)
	for _, volume := range volumes {
		if nested(volume, "configMap", "name") == want {
			return
		}
	}
	t.Errorf("missing ConfigMap volume %q", want)
}

func assertVolumeMount(t *testing.T, resource manifest, containerName, volumeName, mountPath string) {
	t.Helper()
	containers, _ := nested(resource, "spec", "template", "spec", "containers").([]any)
	for _, container := range containers {
		if nested(container, "name") != containerName {
			continue
		}
		mounts, _ := nested(container, "volumeMounts").([]any)
		for _, mount := range mounts {
			if nested(mount, "name") == volumeName && nested(mount, "mountPath") == mountPath {
				return
			}
		}
	}
	t.Errorf("missing volume mount %q at %q in container %q", volumeName, mountPath, containerName)
}

func assertContainerDoesNotMountVolume(t *testing.T, resource manifest, containerName, volumeName string) {
	t.Helper()
	containers, _ := nested(resource, "spec", "template", "spec", "initContainers").([]any)
	for _, container := range containers {
		if nested(container, "name") != containerName {
			continue
		}
		mounts, _ := nested(container, "volumeMounts").([]any)
		for _, mount := range mounts {
			if nested(mount, "name") == volumeName {
				t.Errorf("container %q must not mount privileged volume %q", containerName, volumeName)
			}
		}
		return
	}
	t.Errorf("missing init container %q", containerName)
}

func assertProjectedServiceAccountToken(t *testing.T, resource manifest, volumeName string) {
	t.Helper()
	volumes, _ := nested(resource, "spec", "template", "spec", "volumes").([]any)
	for _, volume := range volumes {
		if nested(volume, "name") != volumeName {
			continue
		}
		sources, _ := nested(volume, "projected", "sources").([]any)
		for _, source := range sources {
			if nested(source, "serviceAccountToken", "path") == "token" {
				return
			}
		}
		t.Errorf("projected volume %q has no ServiceAccount token source", volumeName)
		return
	}
	t.Errorf("missing projected ServiceAccount volume %q", volumeName)
}

func assertWorkerIdentityEnvironment(t *testing.T, worker manifest) {
	t.Helper()
	environment, _ := nested(worker, "spec", "template", "spec", "containers", 0, "env").([]any)
	for _, item := range environment {
		switch nested(item, "name") {
		case "ORCHESTRATOR_ADDR":
			assertString(t, item, "value", "load-tester-orchestrator:9090")
		case "WORKER_ID":
			assertString(t, item, "valueFrom", "fieldRef", "fieldPath", "metadata.name")
		}
	}
	if len(environment) != 2 {
		t.Errorf("worker environment count = %d, want 2", len(environment))
	}
}

func assertServicePort(t *testing.T, service manifest, name string, want int) {
	t.Helper()
	ports, _ := nested(service, "spec", "ports").([]any)
	for _, port := range ports {
		if nested(port, "name") == name {
			assertInt(t, port, want, "port")
			return
		}
	}
	t.Errorf("missing service port %q", name)
}

func assertRoleRule(t *testing.T, role manifest, apiGroup, resourceName string, verbs ...string) {
	t.Helper()
	rules, _ := nested(role, "rules").([]any)
	for _, rule := range rules {
		apiGroups := stringList(t, nested(rule, "apiGroups"))
		resources := stringList(t, nested(rule, "resources"))
		gotVerbs := stringList(t, nested(rule, "verbs"))
		if contains(apiGroups, apiGroup) && contains(resources, resourceName) {
			for _, verb := range verbs {
				assertContains(t, gotVerbs, verb, "Role rule verbs")
			}
			return
		}
	}
	t.Errorf("missing Role rule for %s/%s", apiGroup, resourceName)
}

func assertRoleResourceNames(t *testing.T, role manifest, apiGroup, resource, want string) {
	t.Helper()
	rules, _ := nested(role, "rules").([]any)
	for _, rule := range rules {
		if contains(stringList(t, nested(rule, "apiGroups")), apiGroup) && contains(stringList(t, nested(rule, "resources")), resource) {
			if !contains(stringList(t, nested(rule, "resourceNames")), want) {
				t.Errorf("Role rule for %s/%s is not scoped to %q", apiGroup, resource, want)
			}
			return
		}
	}
	t.Errorf("missing Role rule for %s/%s", apiGroup, resource)
}

func stringList(t *testing.T, value any) []string {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("value = %T, want a list", value)
	}
	values := make([]string, 0, len(items))
	for _, item := range items {
		stringValue, ok := item.(string)
		if !ok {
			t.Fatalf("list item = %T, want string", item)
		}
		values = append(values, stringValue)
	}
	return values
}

func assertContains(t *testing.T, values []string, want, label string) {
	t.Helper()
	if !contains(values, want) {
		t.Errorf("%s %v does not contain %q", label, values, want)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func formatPath(path []any) string {
	parts := make([]string, len(path))
	for index, value := range path {
		parts[index] = "?"
		if stringValue, ok := value.(string); ok {
			parts[index] = stringValue
		}
	}
	return strings.Join(parts, ".")
}
