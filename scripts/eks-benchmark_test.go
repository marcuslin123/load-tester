package scripts_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type eksCommandResult struct {
	Output string
	Status int
	Calls  []string
	Root   string
}

const (
	eksProfile = "load-tester-eks"
	eksRegion  = "us-west-2"
	eksName    = "load-tester-benchmark-abc123"
	eksSHA     = "0123456789abcdef0123456789abcdef01234567"
)

// parallelEKSBenchmarkTest is safe because each top-level test creates an isolated
// fake command path, environment, state directory, and artifact root.
func parallelEKSBenchmarkTest(t *testing.T) {
	t.Helper()
	t.Parallel()
}

func TestEKSBenchmarkSyntaxAndDispatch(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	for _, file := range []string{"eks-benchmark.sh", "lib/eks-benchmark.sh"} {
		command := exec.Command("bash", "-n", file)
		command.Dir = "."
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("bash -n %s: %v\n%s", file, err, output)
		}
	}

	result := runEKSBenchmark(t, "unknown", baseEKSEnvironment(), nil)
	if result.Status == 0 || !strings.Contains(result.Output, "unknown command") {
		t.Fatalf("unknown command status=%d output=%q", result.Status, result.Output)
	}
}

func TestEKSBenchmarkExecutableEntrypointDispatchesWithoutBash(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	command := exec.Command("./eks-benchmark.sh", "unknown")
	command.Dir = "."
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "unknown command") {
		t.Fatalf("direct entrypoint err=%v output=%q", err, output)
	}
}

func TestEKSBenchmarkPreflightIsReadOnlyAndPrintsTopology(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "preflight", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("preflight failed: %s\ncalls:\n%s", result.Output, strings.Join(result.Calls, "\n"))
	}
	for _, want := range []string{
		"Account: 123456789012",
		"Region: us-west-2",
		"Cluster: " + eksName,
		"Zones: us-west-2a, us-west-2b",
		"Topology: 2 c7i.large load-generation nodes; 2 m7i.large platform nodes",
		"API CIDR: 8.8.8.8/32",
		"Deadline: 90 minutes",
	} {
		if !strings.Contains(result.Output, want) {
			t.Errorf("preflight output missing %q:\n%s", want, result.Output)
		}
	}
	assertCallsOnly(t, result.Calls,
		"aws sts get-caller-identity",
		"aws configure get sso_session --profile load-tester-eks",
		"aws ec2 describe-instance-type-offerings",
		"aws eks describe-cluster",
		"curl --fail --connect-timeout 5 --max-time 20 https://checkip.amazonaws.com",
		"git rev-parse HEAD",
		"eksctl create cluster --config-file",
	)
	assertCallContains(t, result.Calls, "eksctl create cluster --config-file", "--dry-run")
}

func TestEKSBenchmarkSupervisorPublishesReadyPIDAtomically(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "sitecustomize.py"), []byte(`
import builtins
import os
import time

real_open = builtins.open

class delayed_ready_file:
    def __init__(self, file):
        self.file = file

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self.file.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self.file, name)

    def write(self, value):
        time.sleep(0.25)
        return self.file.write(value)

def open_with_delayed_ready(path, mode="r", *args, **kwargs):
    file = real_open(path, mode, *args, **kwargs)
    if isinstance(path, str) and path.endswith(".ready.tmp") and "w" in mode:
        with real_open(os.environ["EKS_READY_TMP_MARKER"], "w", encoding="utf-8") as marker:
            marker.write("delayed")
        return delayed_ready_file(file)
    return file

builtins.open = open_with_delayed_ready
`), 0o644); err != nil {
		t.Fatal(err)
	}
	env := baseEKSEnvironment()
	env["PYTHONPATH"] = root
	env["EKS_READY_TMP_MARKER"] = filepath.Join(root, "ready-tmp-used")
	result := runEKSBenchmarkInRoot(t, "preflight", env, nil, root)
	if result.Status != 0 {
		t.Fatalf("preflight failed while ready-file publication was delayed: %s", result.Output)
	}
	if _, err := os.Stat(env["EKS_READY_TMP_MARKER"]); err != nil {
		t.Fatalf("supervisor did not write the temporary ready file: %v", err)
	}
}

func TestEKSBenchmarkPreflightRejectsUnsafeInputsBeforeMutation(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	cases := []struct {
		name     string
		env      map[string]string
		fixtures map[string]string
		want     string
	}{
		{name: "wrong region", env: map[string]string{"AWS_REGION": "us-east-1"}, want: "AWS_REGION must be us-west-2"},
		{name: "malformed cluster", env: map[string]string{"EKS_CLUSTER_NAME": "production"}, want: "invalid EKS_CLUSTER_NAME"},
		{name: "root identity", fixtures: map[string]string{"AWS_IDENTITY_ARN": "arn:aws:iam::123456789012:root"}, want: "root identity"},
		{name: "existing cluster", fixtures: map[string]string{"EKS_CLUSTER_EXISTS": "1"}, want: "already exists"},
		{name: "single common zone", fixtures: map[string]string{"AWS_OFFERINGS": "us-west-2a"}, want: "two common availability zones"},
		{name: "non ipv4", fixtures: map[string]string{"PUBLIC_IP": "2001:db8::1"}, want: "public IPv4"},
		{name: "private ipv4", fixtures: map[string]string{"PUBLIC_IP": "10.0.0.1"}, want: "public IPv4"},
		{name: "keep cluster", env: map[string]string{"KEEP_CLUSTER": "1"}, want: "keep-cluster"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseEKSEnvironment()
			mergeEnvironment(env, tc.env)
			result := runEKSBenchmark(t, "preflight", env, tc.fixtures)
			if result.Status == 0 || !strings.Contains(result.Output, tc.want) {
				t.Fatalf("status=%d output=%q, want %q", result.Status, result.Output, tc.want)
			}
			assertNoMutatingCalls(t, result.Calls)
		})
	}
}

func TestEKSBenchmarkPreflightUsesOnlyMutuallySupportedAvailabilityZones(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "preflight", baseEKSEnvironment(), map[string]string{
		"AWS_C7I_OFFERINGS": "us-west-2a,us-west-2b,us-west-2c",
		"AWS_M7I_OFFERINGS": "us-west-2b,us-west-2c",
	})
	if result.Status != 0 {
		t.Fatalf("preflight failed: %s", result.Output)
	}
	if !strings.Contains(result.Output, "Zones: us-west-2b, us-west-2c") {
		t.Fatalf("preflight selected incompatible zones:\n%s", result.Output)
	}
}

func TestEKSBenchmarkRenderWritesSafeImmutableManifestsWithoutBillingApproval(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "render", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("render failed: %s", result.Output)
	}
	runDir := latestRunDir(t, result.Root)
	for _, name := range []string{"cluster.yaml", "eks-worker.yaml", "eks-hpa.yaml"} {
		contents, err := os.ReadFile(filepath.Join(runDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(contents), "example.invalid") || strings.Contains(string(contents), "sha-required") {
			t.Fatalf("%s retains a safe placeholder", name)
		}
	}
	workerManifest, err := os.ReadFile(filepath.Join(runDir, "eks-worker.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	clusterConfig, err := os.ReadFile(filepath.Join(runDir, "cluster.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(clusterConfig), "__RUN_ID__") || strings.Contains(string(clusterConfig), "__WORKFLOW_IDENTITY__") {
		t.Fatalf("cluster config retains ownership placeholders:\n%s", clusterConfig)
	}
	if !strings.Contains(string(clusterConfig), "load-tester.io/workflow-identity: \"eks-benchmark\"") ||
		!strings.Contains(string(clusterConfig), "load-tester.io/run-id: \"20260828T000000Z-0123456\"") {
		t.Fatalf("cluster config has incorrect rendered ownership tags:\n%s", clusterConfig)
	}
	for _, role := range []string{"orchestrator", "worker", "target"} {
		want := fmt.Sprintf("123456789012.dkr.ecr.us-west-2.amazonaws.com/load-tester-%s:sha-%s", role, eksSHA)
		if !strings.Contains(string(workerManifest), want) {
			t.Errorf("worker manifest missing immutable image %q", want)
		}
	}
	assertNoMutatingCalls(t, result.Calls)
}

func TestEKSBenchmarkRenderRejectsUnsafeManifestBeforeMutation(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "render", baseEKSEnvironment(), map[string]string{
		"EKS_KUSTOMIZE_BUILD": "apiVersion: v1\nkind: Service\nspec:\n  type: LoadBalancer\n",
	})
	if result.Status == 0 || !strings.Contains(result.Output, "unsafe deployment value") {
		t.Fatalf("unsafe render status=%d output=%q", result.Status, result.Output)
	}
	assertNoMutatingCalls(t, result.Calls)
}

func TestEKSBenchmarkECRPushRequiresApprovalAndPublishesThreeImmutableImages(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	withoutApproval := baseEKSEnvironment()
	delete(withoutApproval, "ALLOW_AWS_BILLING")
	result := runEKSBenchmark(t, "ecr-push", withoutApproval, nil)
	if result.Status == 0 || !strings.Contains(result.Output, "ALLOW_AWS_BILLING=1") {
		t.Fatalf("ecr-push without approval status=%d output=%q", result.Status, result.Output)
	}
	assertNoMutatingCalls(t, result.Calls)

	result = runEKSBenchmark(t, "ecr-push", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("ecr-push failed: %s", result.Output)
	}
	assertCallCount(t, result.Calls, "aws ecr describe-repositories", 3)
	assertCallCount(t, result.Calls, "aws ecr put-lifecycle-policy", 3)
	assertCallCount(t, result.Calls, "docker login --username AWS --password-stdin", 1)
	for _, target := range []string{"orchestrator", "worker", "target"} {
		assertCallContains(t, result.Calls, "docker buildx build --platform linux/amd64 --push", "--target "+target, "--build-arg VERSION=sha-"+eksSHA, "--build-arg VCS_REF="+eksSHA)
	}
}

func TestEKSBenchmarkECRPushPassesPasswordToDockerLogin(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "ecr-push", baseEKSEnvironment(), map[string]string{
		"EKS_FAKE_DOCKER_LOGIN_REQUIRES_PASSWORD": "1",
	})
	if result.Status != 0 {
		t.Fatalf("ecr-push did not pass the ECR password to docker login: %s", result.Output)
	}
}

func TestEKSBenchmarkECRPushRejectsDirtySourceBeforeMutation(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "ecr-push", baseEKSEnvironment(), map[string]string{"GIT_DIRTY": "1"})
	if result.Status == 0 || !strings.Contains(result.Output, "working tree is dirty") {
		t.Fatalf("dirty source status=%d output=%q", result.Status, result.Output)
	}
	assertNoMutatingCalls(t, result.Calls)
}

func TestEKSBenchmarkECRPushCreatesOnlyMissingRepositories(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "ecr-push", baseEKSEnvironment(), map[string]string{"EKS_REPOSITORY_EXISTS": "0"})
	if result.Status != 0 {
		t.Fatalf("ecr-push failed: %s", result.Output)
	}
	assertCallCount(t, result.Calls, "aws ecr create-repository", 3)
}

func TestEKSBenchmarkECRPushRefusesUnownedMutableOrUnreadableRepositories(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	for name, fixtures := range map[string]map[string]string{
		"unowned":   {"EKS_FAKE_ECR_TAGS": "mismatch"},
		"mutable":   {"EKS_FAKE_ECR_MUTABILITY": "MUTABLE"},
		"API error": {"EKS_FAKE_ECR_DESCRIBE_ERROR": "1"},
	} {
		t.Run(name, func(t *testing.T) {
			result := runEKSBenchmark(t, "ecr-push", baseEKSEnvironment(), fixtures)
			if result.Status == 0 {
				t.Fatalf("%s repository unexpectedly reached mutation: %s", name, result.Output)
			}
			assertCallAbsent(t, result.Calls, "aws ecr create-repository")
			assertCallAbsent(t, result.Calls, "aws ecr put-lifecycle-policy")
			assertCallAbsent(t, result.Calls, "docker login")
			assertCallAbsent(t, result.Calls, "docker buildx build")
		})
	}
}

func TestEKSBenchmarkECRPushReapsBuildxOnInterrupt(t *testing.T) {
	root := t.TempDir()
	command, callLog := startEKSBenchmarkInRoot(t, "ecr-push", baseEKSEnvironment(), map[string]string{
		"EKS_FAKE_DOCKER_BUILDX_BLOCK": "1",
	}, root)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatalf("start ecr-push: %v", err)
	}
	pidPath := filepath.Join(root, "fake-state", "docker-buildx-pid")
	readyPath := filepath.Join(root, "fake-state", "docker-buildx-ready")
	if !waitForPath(readyPath, 15*time.Second) {
		calls, _ := os.ReadFile(callLog)
		repository, _ := os.ReadFile(filepath.Join(root, "fake-state", "last-ecr-repository"))
		t.Fatalf("timed out waiting for %s; calls=%q repository=%q output=%q", readyPath, calls, repository, output.String())
	}
	pid := readRecordedPID(t, pidPath)
	defer killRecordedProcessGroup(t, pid)
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatalf("signal ecr-push process group: %v", err)
	}
	result := waitForEKSCommand(t, command, callLog, root, &output, 8*time.Second)
	if result.Status == 0 {
		t.Fatalf("interrupted ecr-push unexpectedly succeeded: %s", result.Output)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("buildx process %d survived ecr-push cleanup: %v", pid, err)
	}
	terminated, err := os.ReadFile(filepath.Join(root, "fake-state", "docker-buildx-terminated"))
	if err != nil || strings.TrimSpace(string(terminated)) != "terminated" {
		t.Fatalf("buildx did not receive TERM: err=%v marker=%q", err, terminated)
	}
}

func TestEKSBenchmarkRunWritesOwnedStateAndAlwaysTeardownAfterSuccess(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	runDir := latestRunDir(t, result.Root)
	state := readJSONFixture(t, filepath.Join(runDir, "owned-run.json"))
	if got, want := state["schema_version"], float64(1); got != want {
		t.Fatalf("sentinel schema_version=%v want=%v", got, want)
	}
	if got, want := state["deadline_epoch"].(float64)-state["start_epoch"].(float64), float64(5400); got != want {
		t.Fatalf("deadline duration=%v want=%v", got, want)
	}
	assertCallOrder(t, result.Calls,
		"eksctl create cluster",
		"kubectl apply -f "+filepath.Join(runDir, "eks-worker.yaml"),
		"kubectl wait --for=condition=Available deployment/autoscale-target",
		"kubectl logs job/load-tester-orchestrator",
		"eksctl delete cluster --name "+eksName+" --region us-west-2 --wait",
	)
	assertCallBefore(t, result.Calls, "git rev-parse HEAD", "eksctl create cluster")
	assertCallBefore(t, result.Calls, "eksctl delete cluster", "aws ec2 describe-instances")
}

func TestEKSBenchmarkRunLetsRenderedConfigSetClusterRegion(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	assertCallContains(t, result.Calls, "eksctl create cluster --config-file")
	assertNoCallContaining(t, result.Calls, "eksctl create cluster --config-file", "--region")
}

func TestEKSBenchmarkRunInstallsStacksBeforeReadinessAndUsesConfigMapsForScenarios(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	runDir := latestRunDir(t, result.Root)
	workerStack := filepath.Join(runDir, "eks-worker.yaml")
	hpaStack := filepath.Join(runDir, "eks-hpa.yaml")

	assertCallOrder(t, result.Calls,
		"eksctl create cluster",
		"kubectl apply -f "+workerStack,
		"kubectl wait --for=condition=Available deployment/autoscale-target",
	)
	assertCallBefore(t, result.Calls,
		"kubectl apply -f "+workerStack,
		"kubectl apply -f "+hpaStack,
	)
	assertCallBefore(t, result.Calls,
		"kubectl wait --for=condition=Available deployment/metrics-server",
		"curl --fail --connect-timeout 5 --max-time 20 --get --data-urlencode query=kube_horizontalpodautoscaler_status_current_replicas",
	)
	assertCallBefore(t, result.Calls,
		"curl --fail --connect-timeout 5 --max-time 20 --get --data-urlencode query=kube_horizontalpodautoscaler_status_current_replicas",
		"kubectl create configmap load-test-config --namespace load-tester --from-file=demo.yaml="+filepath.Join(runDir, "scenarios", "target-baseline.yaml"),
	)

	assertCallCount(t, result.Calls, "kubectl create configmap load-test-config", 4)
	assertCallCount(t, result.Calls, "kubectl apply -f "+filepath.Join(runDir, "configmaps")+string(filepath.Separator), 4)
	assertCallCount(t, result.Calls, "kubectl delete job/load-tester-orchestrator", 4)
	assertCallCount(t, result.Calls, "kubectl patch job/load-tester-orchestrator", 4)
	assertCallSubsequence(t, result.Calls,
		"kubectl create configmap load-test-config --namespace load-tester --from-file=demo.yaml="+filepath.Join(runDir, "scenarios", "worker-baseline.yaml"),
		"kubectl apply -f "+filepath.Join(runDir, "configmaps", "worker-baseline.configmap.yaml"),
		"kubectl delete job/load-tester-orchestrator",
		"kubectl apply -f "+workerStack,
		"kubectl patch job/load-tester-orchestrator",
		"kubectl wait --for=condition=complete job/load-tester-orchestrator",
		"kubectl logs job/load-tester-orchestrator",
		"kubectl create configmap load-test-config --namespace load-tester --from-file=demo.yaml="+filepath.Join(runDir, "scenarios", "worker-autoscale.yaml"),
		"kubectl apply -f "+filepath.Join(runDir, "configmaps", "worker-autoscale.configmap.yaml"),
		"kubectl delete job/load-tester-orchestrator",
		"kubectl apply -f "+workerStack,
		"kubectl patch job/load-tester-orchestrator",
		"kubectl create configmap load-test-config --namespace load-tester --from-file=demo.yaml="+filepath.Join(runDir, "scenarios", "target-baseline.yaml"),
		"kubectl apply -f "+filepath.Join(runDir, "configmaps", "target-baseline.configmap.yaml"),
		"kubectl delete job/load-tester-orchestrator",
		"kubectl apply -f "+hpaStack,
		"kubectl patch job/load-tester-orchestrator",
		"kubectl create configmap load-test-config --namespace load-tester --from-file=demo.yaml="+filepath.Join(runDir, "scenarios", "target-hpa.yaml"),
		"kubectl apply -f "+filepath.Join(runDir, "configmaps", "target-hpa.configmap.yaml"),
		"kubectl delete job/load-tester-orchestrator",
		"kubectl apply -f "+hpaStack,
		"kubectl patch job/load-tester-orchestrator",
	)
	for _, call := range result.Calls {
		if strings.HasPrefix(call, "kubectl create configmap load-test-config") &&
			(!strings.Contains(call, "--from-file=demo.yaml=") ||
				!strings.Contains(call, "--dry-run=client") ||
				!strings.Contains(call, "-o yaml")) {
			t.Fatalf("scenario ConfigMap call has the wrong payload contract: %s", call)
		}
		if strings.HasPrefix(call, "kubectl apply -f ") && strings.Contains(call, "/scenarios/") && !strings.Contains(call, "/configmaps/") {
			t.Fatalf("load-test YAML must be stored in load-test-config, not applied as Kubernetes YAML: %s", call)
		}
	}
}

func TestEKSBenchmarkRunStopsOwnedPortForwards(t *testing.T) {
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), map[string]string{
		"EKS_FAKE_REQUIRE_PORT_FORWARDS_STOPPED_BEFORE_CLEANUP": "1",
	})
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	contents, err := os.ReadFile(filepath.Join(result.Root, "fake-state", "stopped-port-forwards"))
	if err != nil {
		t.Fatalf("port-forward cleanup marker: %v", err)
	}
	if got := strings.Count(string(contents), "stopped\n"); got != 2 {
		t.Fatalf("stopped port-forwards=%d want=2, contents=%q", got, contents)
	}
	assertRecordedProcessesGone(t, filepath.Join(result.Root, "fake-state", "port-forward-pids"))
}

func TestEKSBenchmarkDeadlineStopsAndReapsStuckPortForwards(t *testing.T) {
	env := baseEKSEnvironment()
	env["EKS_BENCHMARK_DEADLINE_SECONDS"] = "3"
	started := time.Now()
	result := runEKSBenchmark(t, "run", env, map[string]string{
		"EKS_FAKE_PORT_FORWARD_IGNORE_TERM":     "1",
		"EKS_FAKE_LOCAL_ENDPOINT_DOWN":          "1",
		"EKS_PORT_FORWARD_STOP_TIMEOUT_SECONDS": "1",
		"EKS_PROCESS_TERMINATION_GRACE_SECONDS": "5",
	})
	if result.Status == 0 {
		t.Fatalf("stuck port-forward status=%d output=%q", result.Status, result.Output)
	}
	if elapsed := time.Since(started); elapsed >= 15*time.Second {
		t.Fatalf("port-forward teardown exceeded the bounded lifecycle budget: %s", elapsed)
	}
	assertCallCount(t, result.Calls, "kubectl port-forward", 2)
	assertCallBefore(t, result.Calls, "kubectl port-forward", "eksctl delete cluster")
	markers, err := os.ReadFile(filepath.Join(result.Root, "fake-state", "port-forward-term"))
	if err != nil || strings.Count(string(markers), "term\n") != 2 {
		t.Fatalf("stuck port-forwards did not receive TERM before forced reaping: err=%v markers=%q", err, markers)
	}
	assertRecordedProcessesGone(t, filepath.Join(result.Root, "fake-state", "port-forward-pids"))
	report := readJSONFixture(t, filepath.Join(latestRunDir(t, result.Root), "cleanup-report.json"))
	if report["status"] != "FAIL" {
		t.Fatalf("deadline timeout reported cleanup %v, want FAIL", report["status"])
	}
}

func TestEKSBenchmarkDashboardWaitRechecksGrafanaPortForward(t *testing.T) {
	env := baseEKSEnvironment()
	// The full fake workflow must reach dashboard collection before its deadline.
	env["EKS_BENCHMARK_DEADLINE_SECONDS"] = "60"
	env["EKS_DASHBOARD_PNG"] = filepath.Join(t.TempDir(), "missing-dashboard.png")
	env["EKS_DASHBOARD_REVIEW"] = filepath.Join(t.TempDir(), "missing-dashboard-review.json")
	result := runEKSBenchmark(t, "run", env, map[string]string{"EKS_FAKE_GRAFANA_FAIL_AFTER": "1"})
	if result.Status == 0 {
		t.Fatalf("Grafana failure during dashboard wait unexpectedly succeeded: %q", result.Output)
	}
	checks, err := os.ReadFile(filepath.Join(result.Root, "fake-state", "grafana-health-check-count"))
	if err != nil || strings.TrimSpace(string(checks)) != "2" {
		t.Fatalf("dashboard wait did not recheck Grafana health: err=%v checks=%q", err, checks)
	}
}

func TestEKSBenchmarkSignalReapsManagedCommandSubstitution(t *testing.T) {
	root := t.TempDir()
	env := baseEKSEnvironment()
	env["EKS_PROCESS_TERMINATION_GRACE_SECONDS"] = "2"
	command, callLog := startEKSBenchmarkInRoot(t, "run", env, map[string]string{
		"EKS_FAKE_BLOCK_PROMETHEUS_QUERY": "1",
	}, root)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatalf("start benchmark: %v", err)
	}
	defer func() {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		_, _ = command.Process.Wait()
	}()

	blockedPIDPath := filepath.Join(root, "fake-state", "blocked-prometheus-query-pid")
	waitForFile(t, filepath.Join(root, "fake-state", "blocked-prometheus-query-ready"), 15*time.Second)
	waitForFile(t, blockedPIDPath, 15*time.Second)
	started := time.Now()
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatalf("signal benchmark process group: %v", err)
	}
	result := waitForEKSCommand(t, command, callLog, root, &output, 8*time.Second)
	if result.Status == 0 {
		t.Fatalf("interrupted benchmark unexpectedly succeeded: %s", result.Output)
	}
	if elapsed := time.Since(started); elapsed >= 5*time.Second {
		t.Fatalf("interrupt delayed teardown for %s: %q", elapsed, result.Output)
	}
	assertCallContains(t, result.Calls, "eksctl delete cluster", "--wait")

	contents, err := os.ReadFile(blockedPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
	if err != nil || pid <= 0 {
		t.Fatalf("blocked Prometheus query PID=%q err=%v", contents, err)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("managed command substitution process %d survived signal cleanup: %v", pid, err)
	}
	terminated, err := os.ReadFile(filepath.Join(root, "fake-state", "blocked-prometheus-query-terminated"))
	if err != nil || strings.TrimSpace(string(terminated)) != "terminated" {
		t.Fatalf("active managed child did not receive TERM: err=%v marker=%q", err, terminated)
	}
	// kill -0 is sandbox-safe and, together with the fake command's PID file,
	// proves the command substitution and its recorded child are both reaped.
	if err := syscall.Kill(command.Process.Pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("benchmark process %d survived signal cleanup: %v", command.Process.Pid, err)
	}
}

func TestEKSBenchmarkDeadlineTerminatesTheActiveClusterCommandBeforeTeardown(t *testing.T) {
	env := baseEKSEnvironment()
	env["EKS_BENCHMARK_DEADLINE_SECONDS"] = "30"
	env["EKS_PROCESS_TERMINATION_GRACE_SECONDS"] = "1"
	env["EKS_EMERGENCY_DIAGNOSTICS_BUDGET_SECONDS"] = "1"
	env["EKS_EMERGENCY_DIAGNOSTIC_GRACE_SECONDS"] = "0"
	root := t.TempDir()
	command, callLog := startEKSBenchmarkInRoot(t, "run", env, map[string]string{
		"EKS_FAKE_EKSCTL_BLOCK":     "1",
		"EKS_FAKE_DIAGNOSTIC_BLOCK": "1",
	}, root)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatalf("start benchmark: %v", err)
	}
	defer func() {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
	}()
	activePath := filepath.Join(root, "artifacts", "eks", "20260828T000000Z-0123456", ".active-command-pid")
	waitForFile(t, activePath, 15*time.Second)
	waitForFile(t, filepath.Join(root, "fake-state", "eksctl-create-ready"), 15*time.Second)
	activeState, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatalf("read active command state: %v", err)
	}
	activeFields := strings.Fields(string(activeState))
	if len(activeFields) < 3 {
		t.Fatalf("active command state=%q, want owner, supervisor, and active command PIDs", activeState)
	}
	supervisorPID, err := strconv.Atoi(activeFields[1])
	if err != nil || supervisorPID <= 0 {
		t.Fatalf("active command state=%q has invalid supervisor PID: %v", activeState, err)
	}
	if processGroup, err := syscall.Getpgid(supervisorPID); err != nil || processGroup != supervisorPID {
		t.Fatalf("supervisor PID=%d process group=%d err=%v, want isolated leader", supervisorPID, processGroup, err)
	}
	activeCommandPID, err := strconv.Atoi(activeFields[2])
	if err != nil || activeCommandPID <= 0 {
		t.Fatalf("active command state=%q has invalid active command PID: %v", activeState, err)
	}
	started := time.Now()
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatalf("signal benchmark process group: %v", err)
	}
	waitForCall(t, callLog, "eksctl delete cluster", 3500*time.Millisecond)
	waitForTimedFakeEvent(t, filepath.Join(root, "events.log"), "eksctl delete cluster", 500*time.Millisecond)
	deleteStarted := time.Since(started)
	if deleteStarted >= 3500*time.Millisecond {
		t.Fatalf("cluster deletion did not start within the deadline bound: %s", deleteStarted)
	}

	events := readTimedFakeEvents(t, filepath.Join(root, "events.log"))
	assertTimedEventOrder(t, events,
		"eksctl create cluster",
		"kubectl get all",
		"eksctl delete cluster",
	)
	assertLifecycleEvent(t, filepath.Join(root, "artifacts", "eks", "20260828T000000Z-0123456", "lifecycle-events.log"), "active-command-termination-attempted")
	assertLifecycleEvent(t, filepath.Join(root, "artifacts", "eks", "20260828T000000Z-0123456", "lifecycle-events.log"), "diagnostic-attempted")
	if elapsed := eventElapsed(t, events, "eksctl create cluster", "eksctl delete cluster"); elapsed >= 3500*time.Millisecond {
		t.Fatalf("timestamped fake events show cluster deletion after %s, want <3.5s: %+v", elapsed, events)
	}
	completionStarted := time.Now()
	result := waitForEKSCommand(t, command, callLog, root, &output, 10*time.Second)
	completionTime := time.Since(completionStarted)
	if result.Status == 0 {
		t.Fatalf("deadline status=%d output=%q", result.Status, result.Output)
	}
	if err := syscall.Kill(supervisorPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("active command supervisor %d survived group termination: %v", supervisorPID, err)
	}
	if err := syscall.Kill(activeCommandPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("active command group leader %d survived group termination: %v", activeCommandPID, err)
	}
	t.Logf("cluster deletion started %s after deadline signal; timestamped active-to-delete=%s; benchmark shell completed in %s; supervisor=%d", deleteStarted, eventElapsed(t, events, "eksctl create cluster", "eksctl delete cluster"), completionTime, supervisorPID)
}

func TestEKSBenchmarkClusterCreationHasDedicatedTimeoutWithinRunDeadline(t *testing.T) {
	env := baseEKSEnvironment()
	env["EKS_COMMAND_TIMEOUT_SECONDS"] = "1"
	env["EKS_CLUSTER_CREATE_TIMEOUT_SECONDS"] = "5"
	env["EKS_BENCHMARK_FAIL_PHASE"] = "readiness"
	result := runEKSBenchmark(t, "run", env, map[string]string{
		"EKS_FAKE_EKSCTL_CREATE_DELAY_SECONDS": "2",
	})
	if result.Status == 0 || !strings.Contains(result.Output, "simulated failure during readiness") {
		t.Fatalf("status=%d output=%q, want cluster creation to complete before readiness failure", result.Status, result.Output)
	}
	if _, err := os.Stat(filepath.Join(result.Root, "fake-state", "eksctl-create-completed")); err != nil {
		t.Fatalf("cluster creation did not outlive the generic command timeout: %v", err)
	}
}

func TestEKSBenchmarkBoundsEveryBenchmarkHTTPCall(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	for _, call := range result.Calls {
		if !strings.HasPrefix(call, "curl ") || strings.Contains(call, "checkip.amazonaws.com") {
			continue
		}
		if !strings.Contains(call, "--connect-timeout 5") || !strings.Contains(call, "--max-time 20") {
			t.Fatalf("benchmark HTTP call is not bounded by the deadline watchdog contract: %s", call)
		}
	}
}

func TestEKSBenchmarkWorkerEvidenceRateDerivesFromCleanCalibration(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	contents, err := os.ReadFile(filepath.Join(latestRunDir(t, result.Root), "scenarios", "worker-autoscale.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	// The fake calibration returns 125 RPS, so the next whole request rate is ceil(125 * 1.25).
	if !strings.Contains(string(contents), "rate: 157") {
		t.Fatalf("worker evidence does not use the approved calibration multiplier:\n%s", contents)
	}
}

func TestEKSBenchmarkTargetHPAIncreasesRateByFiftyPercentWhenCalibrationNeedsMorePressure(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), map[string]string{"EKS_HPA_REPLICAS_SEQUENCE": "2,3"})
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	contents, err := os.ReadFile(filepath.Join(latestRunDir(t, result.Root), "scenarios", "target-hpa.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "rate: 68") {
		t.Fatalf("target HPA evidence does not use the approved 50 percent escalation:\n%s", contents)
	}
	for name, rate := range map[string]string{"target-hpa-attempt-1.yaml": "rate: 45", "target-hpa-attempt-2.yaml": "rate: 68"} {
		contents, err := os.ReadFile(filepath.Join(latestRunDir(t, result.Root), "scenarios", name))
		if err != nil || !strings.Contains(string(contents), rate) {
			t.Fatalf("archived target HPA scenario %s err=%v contents=%q", name, err, contents)
		}
	}
}

func TestEKSBenchmarkRunWritesRawEvidenceInTheRedactorContract(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	metrics := readJSONFixture(t, filepath.Join(latestRunDir(t, result.Root), "metrics.json"))
	topology, ok := metrics["topology"].(map[string]any)
	if !ok {
		t.Fatalf("metrics topology=%T, want object", metrics["topology"])
	}
	instanceTypes, ok := topology["instance_types"].(map[string]any)
	if !ok {
		t.Fatalf("metrics topology.instance_types=%T, want object", topology["instance_types"])
	}
	if got, want := instanceTypes["loadgen"], "c7i.large"; got != want {
		t.Fatalf("loadgen instance type=%v want=%v", got, want)
	}
	assertCallContains(t, result.Calls, "aws eks describe-cluster", "--query cluster.version")
	assertCallContains(t, result.Calls, "kubectl version", "--output json")
	assertCallContains(t, result.Calls, "kubectl get nodes", "load-tester.io/pool=loadgen")
	assertCallContains(t, result.Calls, "kubectl get nodes", "load-tester.io/pool=platform")
	for _, experimentName := range []string{"worker_experiment", "target_hpa_experiment"} {
		experiment, ok := metrics[experimentName].(map[string]any)
		if !ok {
			t.Errorf("%s=%T, want object", experimentName, metrics[experimentName])
			continue
		}
		scenario, scenarioOK := experiment["scenario"].(map[string]any)
		measured, measuredOK := experiment["measured"].(map[string]any)
		if !scenarioOK || !measuredOK {
			t.Errorf("%s lacks scenario/measured objects", experimentName)
			continue
		}
		if scenario["model"] != "constant-rate" {
			t.Errorf("%s model=%v", experimentName, scenario["model"])
		}
		for _, field := range []string{"peak_rps", "p50_ms", "p95_ms", "p99_ms", "peak_workers", "dropped_samples", "unmet_demand"} {
			if _, ok := measured[field]; !ok {
				t.Errorf("%s measured result is missing %s", experimentName, field)
			}
		}
	}
	teardown, ok := metrics["teardown"].(map[string]any)
	if !ok || teardown["status"] != "PASS" || teardown["verified_clean"] != true {
		t.Fatalf("metrics teardown=%v, want verified PASS", teardown)
	}
}

func TestEKSBenchmarkRunCollectsDiagnosticsBeforeTeardownOnFailure(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	env := baseEKSEnvironment()
	env["EKS_BENCHMARK_FAIL_PHASE"] = "worker-evidence"
	result := runEKSBenchmark(t, "run", env, nil)
	if result.Status == 0 || !strings.Contains(result.Output, "worker-evidence") {
		t.Fatalf("run failure status=%d output=%q", result.Status, result.Output)
	}
	assertCallBefore(t, result.Calls, "kubectl get all", "eksctl delete cluster")
	assertCallBefore(t, result.Calls, "kubectl get events", "eksctl delete cluster")
	assertCallBefore(t, result.Calls, "eksctl delete cluster", "aws ec2 describe-instances")
}

func TestEKSBenchmarkCleanupFailureDoesNotRetryClusterDeletion(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), map[string]string{"EKS_LEFTOVER_CATEGORY": "ec2"})
	if result.Status == 0 || !strings.Contains(result.Output, "cleanup verification failed") {
		t.Fatalf("cleanup failure status=%d output=%q", result.Status, result.Output)
	}
	assertCallCount(t, result.Calls, "eksctl delete cluster", 1)
}

func TestEKSBenchmarkRunRejectsExpiredDeadlineBeforeClusterCreation(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	env := baseEKSEnvironment()
	env["EKS_BENCHMARK_DEADLINE_SECONDS"] = "0"
	result := runEKSBenchmark(t, "run", env, nil)
	if result.Status == 0 || !strings.Contains(result.Output, "deadline") {
		t.Fatalf("deadline status=%d output=%q", result.Status, result.Output)
	}
	assertNoCallContaining(t, result.Calls, "eksctl create cluster --config-file", "--region")
}

func TestEKSBenchmarkTeardownRefusesUnownedStateAndWorksWithoutBillingApproval(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	root := t.TempDir()
	runDir := writeOwnedRunFixture(t, root, map[string]any{"cluster": eksName})
	env := baseEKSEnvironment()
	delete(env, "ALLOW_AWS_BILLING")
	env["EKS_RUN_DIR"] = runDir

	result := runEKSBenchmarkInRoot(t, "teardown", env, nil, root)
	if result.Status != 0 {
		t.Fatalf("teardown failed: %s", result.Output)
	}
	assertCallBefore(t, result.Calls, "kubectl delete service", "eksctl delete cluster")
	assertCallContains(t, result.Calls, "eksctl delete cluster", "--name "+eksName, "--region us-west-2", "--wait")

	bad := baseEKSEnvironment()
	delete(bad, "ALLOW_AWS_BILLING")
	badRoot := t.TempDir()
	bad["EKS_RUN_DIR"] = writeOwnedRunFixture(t, badRoot, map[string]any{"account": "999999999999"})
	result = runEKSBenchmarkInRoot(t, "teardown", bad, nil, badRoot)
	if result.Status == 0 || !strings.Contains(result.Output, "owned run state") {
		t.Fatalf("unowned teardown status=%d output=%q", result.Status, result.Output)
	}
	assertCallAbsent(t, result.Calls, "eksctl delete cluster")
}

func TestEKSBenchmarkEntrypointIsExecutableAndRedactorUsesPython(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	info, err := os.Stat("eks-benchmark.sh")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatalf("eks-benchmark.sh mode=%#o, want executable", info.Mode().Perm())
	}

	root := t.TempDir()
	runDir := writeOwnedRunFixture(t, root, nil)
	writeCleanupReport(t, runDir, "PASS")
	redactor := writeRedactorFixture(t, root)
	env := baseEKSEnvironment()
	env["EKS_RUN_DIR"] = runDir
	env["EKS_REDACTOR"] = redactor
	env["EKS_BENCHMARK_DATE"] = "2026-08-29"
	result := runEKSBenchmarkInRoot(t, "acknowledge-billing", env, nil, root, "--checked-on", "2026-08-29")
	if result.Status != 0 {
		t.Fatalf("acknowledge billing failed: %s", result.Output)
	}
	assertCallContains(t, result.Calls, "python3 "+redactor, "--input", runDir, "--output")
}

func TestEKSBenchmarkPreflightFailsClosedWhenClusterLookupFails(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "preflight", baseEKSEnvironment(), map[string]string{"EKS_FAKE_AWS_ERROR_FOR": "eks-describe-cluster"})
	if result.Status == 0 || !strings.Contains(result.Output, "unable to determine whether EKS cluster exists") {
		t.Fatalf("status=%d output=%q", result.Status, result.Output)
	}
	assertNoMutatingCalls(t, result.Calls)
}

func TestEKSBenchmarkTeardownDeletesOwnedClusterWhenExposedResourceCleanupFails(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	root := t.TempDir()
	runDir := writeOwnedRunFixture(t, root, nil)
	env := baseEKSEnvironment()
	delete(env, "ALLOW_AWS_BILLING")
	env["EKS_RUN_DIR"] = runDir
	result := runEKSBenchmarkInRoot(t, "teardown", env, map[string]string{"EKS_FAKE_KUBECTL_DELETE_EXPOSED_FAIL": "1"}, root)
	if result.Status == 0 || !strings.Contains(result.Output, "exposed Kubernetes resource cleanup failed") {
		t.Fatalf("status=%d output=%q", result.Status, result.Output)
	}
	assertCallContains(t, result.Calls, "eksctl delete cluster", "--name "+eksName, "--wait")
}

func TestEKSBenchmarkCleanupTimeoutReapsTermIgnoringDescendant(t *testing.T) {
	root := t.TempDir()
	runDir := writeOwnedRunFixture(t, root, nil)
	env := baseEKSEnvironment()
	delete(env, "ALLOW_AWS_BILLING")
	env["EKS_RUN_DIR"] = runDir
	env["EKS_CLEANUP_COMMAND_TIMEOUT_SECONDS"] = "2"
	env["EKS_PROCESS_TERMINATION_GRACE_SECONDS"] = "1"
	command, callLog := startEKSBenchmarkInRoot(t, "teardown", env, map[string]string{
		"EKS_FAKE_EKSCTL_DELETE_BLOCK":      "1",
		"EKS_FAKE_EKSCTL_DELETE_DESCENDANT": "1",
	}, root)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatalf("start teardown: %v", err)
	}

	descendantPath := filepath.Join(root, "fake-state", "eksctl-delete-descendant-pid")
	waitForFile(t, filepath.Join(root, "fake-state", "eksctl-delete-ready"), 5*time.Second)
	waitForFile(t, filepath.Join(root, "fake-state", "eksctl-delete-descendant-ready"), 5*time.Second)
	waitForCall(t, callLog, "eksctl delete cluster", 5*time.Second)
	waitForFile(t, descendantPath, 5*time.Second)
	descendantPID := readRecordedPID(t, descendantPath)
	defer killRecordedProcessGroup(t, descendantPID)

	type completed struct {
		err error
	}
	done := make(chan completed, 1)
	started := time.Now()
	go func() { done <- completed{err: command.Wait()} }()

	select {
	case result := <-done:
		if result.err == nil {
			t.Fatalf("cleanup timeout unexpectedly succeeded: %q", output.String())
		}
		if elapsed := time.Since(started); elapsed >= 4*time.Second {
			t.Fatalf("cleanup timeout exceeded the bounded lifecycle budget: %s", elapsed)
		}
	case <-time.After(5 * time.Second):
		killRecordedProcessGroup(t, descendantPID)
		<-done
		t.Fatalf("cleanup timeout left a TERM-ignoring descendant holding the command pipe: %q", output.String())
	}

	if err := syscall.Kill(descendantPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("cleanup descendant %d survived timeout handling: %v", descendantPID, err)
	}
	marker, err := os.ReadFile(filepath.Join(root, "fake-state", "eksctl-delete-terminated"))
	if err != nil || strings.TrimSpace(string(marker)) != "terminated" {
		t.Fatalf("cleanup command did not receive TERM before group KILL: err=%v marker=%q", err, marker)
	}
	assertCallContains(t, readCalls(t, callLog), "eksctl delete cluster", "--wait")
}

func TestEKSBenchmarkCleanupTimeoutFailsVerificationAfterDeletionAttempt(t *testing.T) {
	env := baseEKSEnvironment()
	env["EKS_CLEANUP_COMMAND_TIMEOUT_SECONDS"] = "1"
	env["EKS_PROCESS_TERMINATION_GRACE_SECONDS"] = "1"
	// Fail immediately after cluster creation so EXIT cleanup exercises the
	// diagnostics-before-deletion path without running every benchmark scenario.
	env["EKS_BENCHMARK_FAIL_PHASE"] = "readiness"
	env["EKS_EMERGENCY_DIAGNOSTICS_BUDGET_SECONDS"] = "1"
	env["EKS_EMERGENCY_DIAGNOSTIC_GRACE_SECONDS"] = "0"
	root := t.TempDir()
	command, callLog := startEKSBenchmarkInRoot(t, "run", env, map[string]string{"EKS_FAKE_EKSCTL_DELETE_BLOCK": "1"}, root)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatalf("start benchmark: %v", err)
	}
	defer func() {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
	}()
	waitForFile(t, filepath.Join(root, "fake-state", "eksctl-delete-ready"), 15*time.Second)
	waitForCall(t, callLog, "eksctl delete cluster", 15*time.Second)
	started := time.Now()
	result := waitForEKSCommand(t, command, callLog, root, &output, 4*time.Second)
	if result.Status == 0 || time.Since(started) >= 4*time.Second {
		t.Fatalf("cleanup timeout status=%d elapsed=%s output=%q", result.Status, time.Since(started), result.Output)
	}
	assertCallBefore(t, result.Calls, "kubectl get all", "eksctl delete cluster")
	assertCallCount(t, result.Calls, "eksctl delete cluster", 1)
	report := readJSONFixture(t, filepath.Join(latestRunDir(t, result.Root), "cleanup-report.json"))
	if report["status"] != "FAIL" {
		t.Fatalf("cleanup timeout reported %v, want FAIL", report["status"])
	}
}

func TestEKSBenchmarkTeardownRequiresCloudOwnershipTags(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	for _, tags := range []string{"mismatch", "missing-workflow", "missing-run-id"} {
		t.Run(tags, func(t *testing.T) {
			root := t.TempDir()
			runDir := writeOwnedRunFixture(t, root, nil)
			env := baseEKSEnvironment()
			delete(env, "ALLOW_AWS_BILLING")
			env["EKS_RUN_DIR"] = runDir
			result := runEKSBenchmarkInRoot(t, "teardown", env, map[string]string{"EKS_FAKE_CLUSTER_TAGS": tags}, root)
			if result.Status == 0 || !strings.Contains(result.Output, "cloud ownership tags") {
				t.Fatalf("status=%d output=%q", result.Status, result.Output)
			}
			assertCallAbsent(t, result.Calls, "eksctl delete cluster")
		})
	}

	root := t.TempDir()
	runDir := writeOwnedRunFixture(t, root, nil)
	env := baseEKSEnvironment()
	delete(env, "ALLOW_AWS_BILLING")
	env["EKS_RUN_DIR"] = runDir
	result := runEKSBenchmarkInRoot(t, "teardown", env, map[string]string{"EKS_FAKE_CLUSTER_TAGS": "match"}, root)
	if result.Status != 0 {
		t.Fatalf("matching tags should permit teardown: status=%d output=%q", result.Status, result.Output)
	}
	assertCallContains(t, result.Calls, "eksctl delete cluster", "--wait")
}

func TestEKSBenchmarkTeardownRejectsMalformedOrCopiedOwnedState(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	for name, overrides := range map[string]map[string]any{
		"invalid git SHA":        {"git_sha": "not-a-commit"},
		"mismatched run ID":      {"run_id": "copied-run"},
		"duplicate availability": {"zones": []string{"us-west-2a", "us-west-2a"}},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			runDir := writeOwnedRunFixture(t, root, overrides)
			env := baseEKSEnvironment()
			delete(env, "ALLOW_AWS_BILLING")
			env["EKS_RUN_DIR"] = runDir
			result := runEKSBenchmarkInRoot(t, "teardown", env, nil, root)
			if result.Status == 0 || !strings.Contains(result.Output, "owned run state") {
				t.Fatalf("status=%d output=%q", result.Status, result.Output)
			}
			assertCallAbsent(t, result.Calls, "eksctl delete cluster")
		})
	}
}

func TestEKSBenchmarkCleanupDetectsEachRemainingResourceCategory(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	for _, category := range []string{"eks", "nodegroups", "ec2", "asg", "cloudformation", "ebs", "elb", "nat", "eip"} {
		t.Run(category, func(t *testing.T) {
			root := t.TempDir()
			runDir := writeOwnedRunFixture(t, root, nil)
			env := baseEKSEnvironment()
			env["EKS_RUN_DIR"] = runDir
			fixtures := map[string]string{"EKS_LEFTOVER_CATEGORY": category}
			if category == "nodegroups" {
				fixtures["EKS_CLUSTER_EXISTS"] = "1"
			}
			result := runEKSBenchmarkInRoot(t, "verify-cleanup", env, fixtures, root)
			if result.Status == 0 || !strings.Contains(result.Output, category) {
				t.Fatalf("category=%s status=%d output=%q", category, result.Status, result.Output)
			}
			contents, err := os.ReadFile(filepath.Join(runDir, "cleanup-report.json"))
			if err != nil || !strings.Contains(string(contents), "FAIL") {
				t.Fatalf("cleanup report err=%v contents=%q", err, contents)
			}
		})
	}
}

func TestEKSBenchmarkCleanupIgnoresTerminalAWSHistoryAndSkipsAbsentClusterNodegroups(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	root := t.TempDir()
	runDir := writeOwnedRunFixture(t, root, nil)
	env := baseEKSEnvironment()
	env["EKS_RUN_DIR"] = runDir
	result := runEKSBenchmarkInRoot(t, "verify-cleanup", env, map[string]string{
		"EKS_FAKE_TERMINAL_RESOURCE_HISTORY": "1",
	}, root)
	if result.Status != 0 {
		t.Fatalf("terminal resource history failed cleanup verification: %s", result.Output)
	}
	assertCallAbsent(t, result.Calls, "aws eks list-nodegroups")
	contents, err := os.ReadFile(filepath.Join(runDir, "cleanup-report.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"nodegroups": "0"`, `"ec2": "0"`, `"cloudformation": "0"`, `"nat": "0"`, `"status": "PASS"`} {
		if !strings.Contains(string(contents), want) {
			t.Errorf("cleanup report missing %s: %s", want, contents)
		}
	}
}

func TestEKSBenchmarkCleanupReportsECRAsLifecycleManaged(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	root := t.TempDir()
	runDir := writeOwnedRunFixture(t, root, nil)
	env := baseEKSEnvironment()
	env["EKS_RUN_DIR"] = runDir
	result := runEKSBenchmarkInRoot(t, "verify-cleanup", env, nil, root)
	if result.Status != 0 {
		t.Fatalf("verify cleanup failed: %s", result.Output)
	}
	contents, err := os.ReadFile(filepath.Join(runDir, "cleanup-report.json"))
	if err != nil || !strings.Contains(string(contents), `"ecr": "lifecycle-managed"`) {
		t.Fatalf("cleanup report err=%v contents=%q", err, contents)
	}
}

func TestEKSBenchmarkCleanupInventoryFailsClosedOnAWSErrorsAndFailedStacks(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	for name, fixtures := range map[string]map[string]string{
		"EKS error":            {"EKS_FAKE_AWS_ERROR_FOR": "eks-describe-cluster"},
		"node group error":     {"EKS_FAKE_AWS_ERROR_FOR": "eks-list-nodegroups", "EKS_CLUSTER_EXISTS": "1"},
		"EC2 error":            {"EKS_FAKE_AWS_ERROR_FOR": "ec2-describe-instances"},
		"ASG error":            {"EKS_FAKE_AWS_ERROR_FOR": "autoscaling-describe-auto-scaling-groups"},
		"CloudFormation error": {"EKS_FAKE_AWS_ERROR_FOR": "cloudformation-list-stacks"},
		"EBS error":            {"EKS_FAKE_AWS_ERROR_FOR": "ec2-describe-volumes"},
		"ELB error":            {"EKS_FAKE_AWS_ERROR_FOR": "elbv2-describe-load-balancers"},
		"NAT error":            {"EKS_FAKE_AWS_ERROR_FOR": "ec2-describe-nat-gateways"},
		"EIP error":            {"EKS_FAKE_AWS_ERROR_FOR": "ec2-describe-addresses"},
		"delete failed stack":  {"EKS_LEFTOVER_CATEGORY": "cloudformation", "EKS_FAKE_CLOUDFORMATION_STATUS": "DELETE_FAILED"},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			runDir := writeOwnedRunFixture(t, root, nil)
			env := baseEKSEnvironment()
			env["EKS_RUN_DIR"] = runDir
			result := runEKSBenchmarkInRoot(t, "verify-cleanup", env, fixtures, root)
			if result.Status == 0 || !strings.Contains(result.Output, "cleanup verification failed") {
				t.Fatalf("status=%d output=%q", result.Status, result.Output)
			}
			contents, err := os.ReadFile(filepath.Join(runDir, "cleanup-report.json"))
			if err != nil || !strings.Contains(string(contents), "FAIL") {
				t.Fatalf("cleanup report err=%v contents=%q", err, contents)
			}
			assertNoCallContaining(t, result.Calls, "aws cloudformation list-stacks", "--stack-status-filter")
		})
	}
}

func TestEKSBenchmarkRunUsesWholeExperimentPrometheusRangesAndSavesRawEvidence(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	assertCallContains(t, result.Calls, "curl --fail --connect-timeout 5 --max-time 20 --get", "/api/v1/query_range", "start=", "end=", "step=15")
	for _, name := range []string{"prometheus-worker-rps-range.json", "prometheus-worker-p95-range.json", "prometheus-hpa-rps-range.json", "prometheus-hpa-p99-range.json"} {
		if _, err := os.Stat(filepath.Join(latestRunDir(t, result.Root), name)); err != nil {
			t.Fatalf("missing whole-experiment evidence %s: %v", name, err)
		}
	}
}

func TestEKSBenchmarkRunRequiresHumanReviewedDashboardInsteadOfGrafanaRenderAPI(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	env := baseEKSEnvironment()
	env["EKS_DASHBOARD_PNG"] = ""
	env["EKS_DASHBOARD_REVIEW"] = ""
	result := runEKSBenchmark(t, "run", env, nil)
	if result.Status == 0 || !strings.Contains(result.Output, "EKS_DASHBOARD_PNG") {
		t.Fatalf("status=%d output=%q", result.Status, result.Output)
	}
	for _, call := range result.Calls {
		if strings.Contains(call, "/render/") {
			t.Fatalf("run attempted unsupported Grafana render API: %s", call)
		}
	}
}

func TestEKSBenchmarkRunCopiesOnlyHashBoundHumanReviewedDashboard(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), nil)
	if result.Status != 0 {
		t.Fatalf("run failed: %s", result.Output)
	}
	runDir := latestRunDir(t, result.Root)
	if _, err := os.Stat(filepath.Join(runDir, "dashboard.png")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "dashboard-review.json")); err != nil {
		t.Fatal(err)
	}
}

func TestEKSBenchmarkStopsHPAImmediatelyOnExperimentIntegrityFailure(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), map[string]string{"EKS_PROMETHEUS_NONZERO": "hpa-dropped-samples"})
	if result.Status == 0 || !strings.Contains(result.Output, "target HPA dropped samples") {
		t.Fatalf("status=%d output=%q", result.Status, result.Output)
	}
	assertCallCount(t, result.Calls, "kubectl create configmap load-test-config", 4)
}

func TestEKSBenchmarkRefusesLowCalibrationThatCannotStayWithinChallengeBand(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), map[string]string{"EKS_PROMETHEUS_BASELINE_RPS": "1"})
	if result.Status == 0 || !strings.Contains(result.Output, "inside the approved 1.25x-1.5x band") {
		t.Fatalf("status=%d output=%q", result.Status, result.Output)
	}
	assertCallCount(t, result.Calls, "kubectl create configmap load-test-config", 1)
}

func TestEKSBenchmarkBillingAttestationRequiresCleanNextDayAndInvokesRedactor(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	root := t.TempDir()
	runDir := writeOwnedRunFixture(t, root, nil)
	writeCleanupReport(t, runDir, "PASS")
	redactor := writeRedactorFixture(t, root)
	env := baseEKSEnvironment()
	env["EKS_RUN_DIR"] = runDir
	env["EKS_REDACTOR"] = redactor
	env["EKS_BENCHMARK_DATE"] = "2026-08-29"
	result := runEKSBenchmarkInRoot(t, "acknowledge-billing", env, map[string]string{"EKS_FAKE_CLUSTER_ABSENT": "1"}, root, "--checked-on", "2026-08-29")
	if result.Status != 0 {
		t.Fatalf("acknowledge billing failed: %s", result.Output)
	}
	assertCallAbsent(t, result.Calls, "aws eks describe-cluster")
	assertCallContains(t, result.Calls, "python3 "+redactor, "--input", runDir, "--output")
	contents, err := os.ReadFile(filepath.Join(runDir, "report.json"))
	if err != nil || !strings.Contains(string(contents), `"type": "human"`) {
		t.Fatalf("attestation err=%v contents=%q", err, contents)
	}

	result = runEKSBenchmarkInRoot(t, "acknowledge-billing", env, nil, root, "--checked-on", "2026-08-28")
	if result.Status == 0 || !strings.Contains(result.Output, "day after") {
		t.Fatalf("same-day acknowledgement status=%d output=%q", result.Status, result.Output)
	}

	lateRoot := t.TempDir()
	lateRunDir := writeOwnedRunFixture(t, lateRoot, nil)
	writeCleanupReport(t, lateRunDir, "PASS")
	lateRedactor := writeRedactorFixture(t, lateRoot)
	lateEnv := baseEKSEnvironment()
	lateEnv["EKS_RUN_DIR"] = lateRunDir
	lateEnv["EKS_REDACTOR"] = lateRedactor
	lateEnv["EKS_BENCHMARK_DATE"] = "2026-08-30"
	result = runEKSBenchmarkInRoot(t, "acknowledge-billing", lateEnv, nil, lateRoot, "--checked-on", "2026-08-30")
	if result.Status == 0 || !strings.Contains(result.Output, "day after") {
		t.Fatalf("late acknowledgement status=%d output=%q", result.Status, result.Output)
	}

	mismatchRoot := t.TempDir()
	mismatchRunDir := writeOwnedRunFixture(t, mismatchRoot, nil)
	writeCleanupReport(t, mismatchRunDir, "PASS")
	mismatchRedactor := writeRedactorFixture(t, mismatchRoot)
	mismatch := baseEKSEnvironment()
	mismatch["EKS_RUN_DIR"] = mismatchRunDir
	mismatch["EKS_REDACTOR"] = mismatchRedactor
	mismatch["EKS_BENCHMARK_DATE"] = "2026-08-29"
	mismatch["AWS_ACCOUNT_ID"] = "999999999999"
	mismatch["EKS_FAKE_CLUSTER_ABSENT"] = "1"
	result = runEKSBenchmarkInRoot(t, "acknowledge-billing", mismatch, nil, mismatchRoot, "--checked-on", "2026-08-29")
	if result.Status == 0 || !strings.Contains(result.Output, "owned run state") {
		t.Fatalf("account-mismatched acknowledgement status=%d output=%q", result.Status, result.Output)
	}
	assertCallAbsent(t, result.Calls, "python3 "+mismatchRedactor)
}

func TestEKSBenchmarkRecoverTeardownReconstructsCloudOwnedState(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	root := t.TempDir()
	env := baseEKSEnvironment()
	delete(env, "ALLOW_AWS_BILLING")
	result := runEKSBenchmarkInRoot(t, "recover-teardown", env, nil, root, "--acknowledge-recovery")
	if result.Status != 0 {
		t.Fatalf("recover teardown failed: %s", result.Output)
	}
	assertCallContains(t, result.Calls, "aws eks describe-cluster", "--query cluster.arn")
	assertCallContains(t, result.Calls, "aws eks list-tags-for-resource")
	assertCallContains(t, result.Calls, "eksctl delete cluster", "--wait")
	recovered := filepath.Join(root, "artifacts", "eks", "20260828T000000Z-0123456", "owned-run.json")
	state := readJSONFixture(t, recovered)
	if state["schema_version"] != float64(2) || state["recovered"] != true {
		t.Fatalf("recovered state=%v, want explicit recovery schema", state)
	}
}

func TestEKSBenchmarkPartialCreateAndRecoveryUseOwnedCloudFormationStack(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	result := runEKSBenchmark(t, "run", baseEKSEnvironment(), map[string]string{
		"EKS_BENCHMARK_FAIL_PHASE":           "cluster",
		"EKS_FAKE_CLUSTER_ABSENT":            "1",
		"EKS_FAKE_CLOUDFORMATION_STACK_TAGS": "match",
	})
	if result.Status == 0 {
		t.Fatal("partial cluster creation unexpectedly succeeded")
	}
	if !callsContain(result.Calls, "aws cloudformation describe-stacks", "eksctl-"+eksName+"-cluster") {
		t.Fatalf("partial-create cleanup did not inspect CloudFormation ownership: calls=%q output=%q", result.Calls, result.Output)
	}
	assertCallContains(t, result.Calls, "eksctl delete cluster", "--wait")

	root := t.TempDir()
	env := baseEKSEnvironment()
	delete(env, "ALLOW_AWS_BILLING")
	result = runEKSBenchmarkInRoot(t, "recover-teardown", env, map[string]string{
		"EKS_FAKE_CLUSTER_ABSENT":            "1",
		"EKS_FAKE_CLOUDFORMATION_STACK_TAGS": "match",
	}, root, "--acknowledge-recovery")
	if result.Status != 0 {
		t.Fatalf("CloudFormation recovery failed: %s", result.Output)
	}
	assertCallContains(t, result.Calls, "aws cloudformation describe-stacks", "eksctl-"+eksName+"-cluster")
	assertCallContains(t, result.Calls, "eksctl delete cluster", "--wait")

	for _, tags := range []string{"missing-workflow", "missing-run-id", "mismatch"} {
		t.Run("refuses "+tags+" CloudFormation ownership", func(t *testing.T) {
			result := runEKSBenchmark(t, "run", baseEKSEnvironment(), map[string]string{
				"EKS_BENCHMARK_FAIL_PHASE":           "cluster",
				"EKS_FAKE_CLUSTER_ABSENT":            "1",
				"EKS_FAKE_CLOUDFORMATION_STACK_TAGS": tags,
			})
			if result.Status == 0 || !strings.Contains(result.Output, "CloudFormation cloud ownership tags") {
				t.Fatalf("tags=%s status=%d output=%q", tags, result.Status, result.Output)
			}
			assertCallAbsent(t, result.Calls, "eksctl delete cluster")
		})
	}
}

func TestEKSBenchmarkRecoverTeardownRefusesUnsafeCloudOwnership(t *testing.T) {
	parallelEKSBenchmarkTest(t)
	for name, fixtures := range map[string]map[string]string{
		"missing workflow tag": {"EKS_FAKE_CLUSTER_TAGS": "missing-workflow"},
		"missing run ID tag":   {"EKS_FAKE_CLUSTER_TAGS": "missing-run-id"},
		"mismatched tags":      {"EKS_FAKE_CLUSTER_TAGS": "mismatch"},
		"cluster account":      {"EKS_FAKE_CLUSTER_ACCOUNT": "999999999999"},
		"cluster absent":       {"EKS_FAKE_CLUSTER_ABSENT": "1"},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			env := baseEKSEnvironment()
			delete(env, "ALLOW_AWS_BILLING")
			result := runEKSBenchmarkInRoot(t, "recover-teardown", env, fixtures, root, "--acknowledge-recovery")
			if result.Status == 0 {
				t.Fatalf("unsafe recovery unexpectedly succeeded: %q", result.Output)
			}
			assertCallAbsent(t, result.Calls, "eksctl delete cluster")
		})
	}
}

func baseEKSEnvironment() map[string]string {
	return map[string]string{
		"AWS_PROFILE":           eksProfile,
		"AWS_REGION":            eksRegion,
		"EKS_CLUSTER_NAME":      eksName,
		"ALLOW_AWS_BILLING":     "1",
		"EKS_BENCHMARK_NOW":     "1724889600",
		"EKS_BENCHMARK_DATE":    "2026-08-28",
		"EKS_BENCHMARK_TESTING": "1",
	}
}

func runEKSBenchmark(t *testing.T, subcommand string, env map[string]string, fixtures map[string]string, args ...string) eksCommandResult {
	t.Helper()
	return runEKSBenchmarkInRoot(t, subcommand, env, fixtures, t.TempDir(), args...)
}

func runEKSBenchmarkInRoot(t *testing.T, subcommand string, env map[string]string, fixtures map[string]string, root string, args ...string) eksCommandResult {
	t.Helper()
	command, callLog := startEKSBenchmarkInRoot(t, subcommand, env, fixtures, root, args...)
	output, err := command.CombinedOutput()
	return finishEKSCommand(t, command, callLog, root, output, err)
}

func startEKSBenchmarkInRoot(t *testing.T, subcommand string, env map[string]string, fixtures map[string]string, root string, args ...string) (*exec.Cmd, string) {
	t.Helper()
	fakeBin := filepath.Join(root, "bin")
	if err := os.MkdirAll(fakeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	callLog := filepath.Join(root, "calls.log")
	if err := os.MkdirAll(filepath.Join(root, "fake-state"), 0o755); err != nil {
		t.Fatal(err)
	}
	tmpDir := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	dashboardPath, dashboardReviewPath := writeDashboardFixture(t, root)
	for _, command := range []string{"aws", "eksctl", "kubectl", "kustomize", "docker", "curl", "git", "date"} {
		writeFakeCommand(t, fakeBin, command)
	}
	command := exec.Command("bash", append([]string{"eks-benchmark.sh", subcommand}, args...)...)
	command.Dir = "."
	command.Env = append(os.Environ(),
		"PATH="+fakeBin+":"+os.Getenv("PATH"),
		"EKS_FAKE_CALL_LOG="+callLog,
		"EKS_FAKE_EVENT_LOG="+filepath.Join(root, "events.log"),
		"EKS_FAKE_STATE_DIR="+filepath.Join(root, "fake-state"),
		"TMPDIR="+tmpDir,
		"ARTIFACT_ROOT="+filepath.Join(root, "artifacts", "eks"),
		"HOME="+filepath.Join(root, "home"),
		"XDG_CACHE_HOME="+filepath.Join(root, "cache"),
		"XDG_CONFIG_HOME="+filepath.Join(root, "config"),
		"KUBECONFIG="+filepath.Join(root, "kubeconfig"),
		"AWS_ACCOUNT_ID=123456789012",
		"AWS_IDENTITY_ARN=arn:aws:iam::123456789012:role/AWSReservedSSO_LoadTester",
		"AWS_OFFERINGS=us-west-2a,us-west-2b,us-west-2c",
		"PUBLIC_IP=8.8.8.8",
		"FAKE_GIT_SHA="+eksSHA,
		"EKS_KUSTOMIZE_BUILD="+fakeRenderedManifest(),
		"EKS_DASHBOARD_PNG="+dashboardPath,
		"EKS_DASHBOARD_REVIEW="+dashboardReviewPath,
	)
	for key, value := range env {
		command.Env = append(command.Env, key+"="+value)
	}
	for key, value := range fixtures {
		command.Env = append(command.Env, key+"="+value)
	}
	return command, callLog
}

func finishEKSCommand(t *testing.T, command *exec.Cmd, callLog, root string, output []byte, err error) eksCommandResult {
	t.Helper()
	status := 0
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			status = exit.ExitCode()
		} else {
			t.Fatalf("wait for benchmark: %v", err)
		}
	}
	var calls []string
	if contents, err := os.ReadFile(callLog); err == nil {
		calls = strings.FieldsFunc(strings.TrimSpace(string(contents)), func(r rune) bool { return r == '\n' })
	}
	return eksCommandResult{Output: string(output), Status: status, Calls: calls, Root: root}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	if waitForPath(path, timeout) {
		return
	}
	t.Fatalf("timed out waiting for %s", path)
}

func waitForPath(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func waitForCall(t *testing.T, path, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(contents), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	events, _ := os.ReadFile(filepath.Join(filepath.Dir(path), "events.log"))
	t.Fatalf("timed out waiting for call %q in %s; timestamped events=%q", want, path, events)
}

type timedFakeEvent struct {
	At   time.Duration
	Call string
}

func readTimedFakeEvents(t *testing.T, path string) []timedFakeEvent {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fake event log: %v", err)
	}
	var events []timedFakeEvent
	for _, line := range strings.Split(strings.TrimSpace(string(contents)), "\n") {
		fields := strings.SplitN(line, " ", 2)
		if len(fields) != 2 {
			t.Fatalf("malformed fake event %q", line)
		}
		seconds, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			t.Fatalf("parse fake event timestamp %q: %v", fields[0], err)
		}
		events = append(events, timedFakeEvent{At: time.Duration(seconds * float64(time.Second)), Call: fields[1]})
	}
	return events
}

func waitForTimedFakeEvent(t *testing.T, path, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(contents), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	contents, _ := os.ReadFile(path)
	t.Fatalf("timed event %q did not arrive within %s: %q", want, timeout, contents)
}

func assertTimedEventOrder(t *testing.T, events []timedFakeEvent, calls ...string) {
	t.Helper()
	previous := -1
	for _, want := range calls {
		index := -1
		for i := previous + 1; i < len(events); i++ {
			if strings.Contains(events[i].Call, want) {
				index = i
				break
			}
		}
		if index < 0 {
			t.Fatalf("timed events missing %q after index %d: %+v", want, previous, events)
		}
		previous = index
	}
}

func eventElapsed(t *testing.T, events []timedFakeEvent, from, to string) time.Duration {
	t.Helper()
	var start time.Duration
	for _, event := range events {
		// Preflight also renders an eksctl dry-run. Keep the most recent source
		// event so this measures the active live command, not earlier setup.
		if strings.Contains(event.Call, from) {
			start = event.At
			continue
		}
		if start != 0 && strings.Contains(event.Call, to) {
			return event.At - start
		}
	}
	t.Fatalf("cannot measure %q to %q in events: %+v", from, to, events)
	return 0
}

func assertLifecycleEvent(t *testing.T, path, want string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(contents), want) {
		t.Fatalf("lifecycle event %q missing: err=%v contents=%q", want, err, contents)
	}
}

func waitForEKSCommand(t *testing.T, command *exec.Cmd, callLog, root string, output *bytes.Buffer, timeout time.Duration) eksCommandResult {
	t.Helper()
	type commandResult struct {
		output []byte
		err    error
	}
	done := make(chan commandResult, 1)
	go func() {
		err := command.Wait()
		done <- commandResult{output: output.Bytes(), err: err}
	}()
	select {
	case result := <-done:
		return finishEKSCommand(t, command, callLog, root, result.output, result.err)
	case <-time.After(timeout):
		calls, _ := os.ReadFile(callLog)
		watchdog, _ := os.ReadFile(filepath.Join(root, "artifacts", "eks", "20260828T000000Z-0123456", "deadline-watchdog.log"))
		t.Fatalf("benchmark did not exit within %s after signal: %q calls=%q watchdog=%q", timeout, output.String(), calls, watchdog)
		return eksCommandResult{}
	}
}

func writeFakeCommand(t *testing.T, directory, name string) {
	t.Helper()
	path := filepath.Join(directory, name)
	script := `#!/usr/bin/env bash
set -euo pipefail
call="$(basename "$0")"
for argument in "$@"; do
  printf -v call '%s %q' "$call" "$argument"
done
# One append keeps concurrent fake port-forwards from corrupting call records.
printf '%s\n' "$call" >>"$EKS_FAKE_CALL_LOG"
# A monotonic timestamp makes deadline ordering assertions independent of wall
# clock changes and shell scheduling. Keep the ordinary call log unchanged.
printf '%s %s\n' "$(perl -MTime::HiRes=clock_gettime,CLOCK_MONOTONIC -e 'print clock_gettime(CLOCK_MONOTONIC)')" "$call" >>"$EKS_FAKE_EVENT_LOG"
publish_ready() {
  local name=$1 temporary="$EKS_FAKE_STATE_DIR/.$1.ready.tmp.$$"
  printf '%s\n' "$$" >"$temporary"
  mv -f "$temporary" "$EKS_FAKE_STATE_DIR/$name"
}
case "$(basename "$0")" in
aws)
  case "$*" in
    *"eks describe-cluster"*) command_key=eks-describe-cluster ;;
    *"eks list-nodegroups"*) command_key=eks-list-nodegroups ;;
    *"ec2 describe-instances"*) command_key=ec2-describe-instances ;;
    *"autoscaling describe-auto-scaling-groups"*) command_key=autoscaling-describe-auto-scaling-groups ;;
    *"cloudformation list-stacks"*) command_key=cloudformation-list-stacks ;;
    *"ec2 describe-volumes"*) command_key=ec2-describe-volumes ;;
    *"elbv2 describe-load-balancers"*) command_key=elbv2-describe-load-balancers ;;
    *"ec2 describe-nat-gateways"*) command_key=ec2-describe-nat-gateways ;;
    *"ec2 describe-addresses"*) command_key=ec2-describe-addresses ;;
    *) command_key='' ;;
  esac
  if [[ -n "${EKS_FAKE_AWS_ERROR_FOR:-}" && "${EKS_FAKE_AWS_ERROR_FOR}" == "$command_key" ]]; then
    printf 'An error occurred (AccessDeniedException): fake access denied\n' >&2
    exit 255
  fi
  if [[ "$*" == "sts get-caller-identity"* ]]; then printf '{"Account":"%s","Arn":"%s"}\n' "$AWS_ACCOUNT_ID" "$AWS_IDENTITY_ARN"; exit 0; fi
  if [[ "$*" == "configure get sso_session"* ]]; then printf 'load-tester-sso\n'; exit 0; fi
	if [[ "$*" == "ec2 describe-instance-type-offerings"* ]]; then
	  if [[ "$*" == *"c7i.large"* ]]; then
	    printf '%s\n' "${AWS_C7I_OFFERINGS:-$AWS_OFFERINGS}"
	  else
	    printf '%s\n' "${AWS_M7I_OFFERINGS:-$AWS_OFFERINGS}"
	  fi
	  exit 0
	fi
  if [[ "${EKS_FAKE_CLUSTER_ABSENT:-0}" == 1 && "$*" == "eks describe-cluster"* ]]; then
    printf 'An error occurred (ResourceNotFoundException): cluster not found\n' >&2
    exit 254
  fi
  if [[ "$*" == "eks describe-cluster"*"--query cluster.version"* ]]; then
    printf '1.34\n'
    exit 0
  fi
  if [[ "$*" == "eks describe-cluster"*"--query cluster.arn"* ]]; then
    printf 'arn:aws:eks:us-west-2:%s:cluster/load-tester-benchmark-abc123\n' "${EKS_FAKE_CLUSTER_ACCOUNT:-123456789012}"
    exit 0
  fi
  if [[ "$*" == "eks list-tags-for-resource"* ]]; then
    if [[ "${EKS_FAKE_CLUSTER_TAGS:-match}" == mismatch ]]; then
      printf '{"tags":{"load-tester.io/workflow-identity":"other","load-tester.io/run-id":"other-run"}}\n'
    elif [[ "${EKS_FAKE_CLUSTER_TAGS:-match}" == missing-workflow ]]; then
      printf '{"tags":{"load-tester.io/run-id":"20260828T000000Z-0123456"}}\n'
    elif [[ "${EKS_FAKE_CLUSTER_TAGS:-match}" == missing-run-id ]]; then
      printf '{"tags":{"load-tester.io/workflow-identity":"eks-benchmark"}}\n'
    else
      printf '{"tags":{"load-tester.io/workflow-identity":"eks-benchmark","load-tester.io/run-id":"20260828T000000Z-0123456"}}\n'
    fi
    exit 0
  fi
	if [[ "$*" == "eks describe-cluster"* ]]; then
    if [[ "${EKS_CLUSTER_EXISTS:-0}" == 1 || "${EKS_LEFTOVER_CATEGORY:-}" == eks || -f "$EKS_FAKE_STATE_DIR/cluster-created" ]]; then printf '{"cluster":{}}\n'; exit 0; fi
    printf 'An error occurred (ResourceNotFoundException): cluster not found\n' >&2
    exit 254
  fi
  if [[ "$*" == "eks list-nodegroups"* ]]; then
    [[ "${EKS_LEFTOVER_CATEGORY:-}" == nodegroups ]] && printf '1\n' || printf '0\n'
    exit 0
  fi
  if [[ "$*" == "ecr describe-repositories"* ]]; then
    if [[ "${EKS_FAKE_ECR_DESCRIBE_ERROR:-0}" == 1 ]]; then
      printf 'An error occurred (AccessDeniedException): fake ECR access denied\n' >&2
      exit 255
    fi
    repository="${4:-}"
    printf '%s\n' "$repository" >"$EKS_FAKE_STATE_DIR/last-ecr-repository"
    [[ "$repository" == load-tester-* ]] || { printf 'missing ECR repository name\n' >&2; exit 2; }
    if [[ "${EKS_REPOSITORY_EXISTS:-1}" != 1 && ! -f "$EKS_FAKE_STATE_DIR/ecr-${repository}-created" ]]; then
      printf 'An error occurred (RepositoryNotFoundException): repository not found\n' >&2
      exit 254
    fi
    printf '{"repositoryArn":"arn:aws:ecr:us-west-2:123456789012:repository/%s","imageTagMutability":"%s"}\n' "$repository" "${EKS_FAKE_ECR_MUTABILITY:-IMMUTABLE}"
    exit 0
  fi
  if [[ "$*" == "ecr create-repository"* ]]; then
    repository="${4:-}"
    [[ "$repository" == load-tester-* ]] || { printf 'missing ECR repository name\n' >&2; exit 2; }
    : >"$EKS_FAKE_STATE_DIR/ecr-${repository}-created"
    printf '{"repository":{"repositoryArn":"arn:aws:ecr:us-west-2:123456789012:repository/%s"}}\n' "$repository"
    exit 0
  fi
  if [[ "$*" == "ecr list-tags-for-resource"* ]]; then
    if [[ "${EKS_FAKE_ECR_TAGS:-match}" == mismatch ]]; then
      printf '{"tags":[{"Key":"load-tester.io/workflow-identity","Value":"other"}]}\n'
    else
      printf '{"tags":[{"Key":"load-tester.io/workflow-identity","Value":"eks-benchmark"}]}\n'
    fi
    exit 0
  fi
  if [[ "$*" == "ecr get-login-password"* ]]; then printf 'fake-password\n'; exit 0; fi
  if [[ "$*" == "cloudformation describe-stacks"* ]]; then
    stack_name=''
    for ((index = 1; index <= $#; index++)); do
      if [[ "${!index}" == "--stack-name" ]]; then
        next=$((index + 1))
        stack_name="${!next}"
        break
      fi
    done
    [[ "$stack_name" == "eksctl-${EKS_CLUSTER_NAME}-cluster" ]] || { printf 'unexpected stack name\n' >&2; exit 2; }
    case "${EKS_FAKE_CLOUDFORMATION_STACK_TAGS:-missing}" in
      match) tags='[{"Key":"load-tester.io/workflow-identity","Value":"eks-benchmark"},{"Key":"load-tester.io/run-id","Value":"20260828T000000Z-0123456"}]' ;;
      missing-workflow) tags='[{"Key":"load-tester.io/run-id","Value":"20260828T000000Z-0123456"}]' ;;
      missing-run-id) tags='[{"Key":"load-tester.io/workflow-identity","Value":"eks-benchmark"}]' ;;
      mismatch) tags='[{"Key":"load-tester.io/workflow-identity","Value":"other"},{"Key":"load-tester.io/run-id","Value":"other-run"}]' ;;
      *) printf 'An error occurred (ValidationError): stack not found\n' >&2; exit 254 ;;
    esac
    printf '{"StackId":"arn:aws:cloudformation:us-west-2:123456789012:stack/%s/fake-id","Tags":%s}\n' "$stack_name" "$tags"
    exit 0
  fi
  if [[ "${EKS_FAKE_TERMINAL_RESOURCE_HISTORY:-0}" == 1 ]]; then
    if [[ "$*" == "ec2 describe-instances"* ]]; then
      [[ "$*" == *"State.Name != 'terminated'"* ]] && printf '0\n' || printf '1\n'
      exit 0
    fi
    if [[ "$*" == "cloudformation list-stacks"* ]]; then
      [[ "$*" == *"StackStatus != 'DELETE_COMPLETE'"* ]] && printf '0\n' || printf '1\n'
      exit 0
    fi
    if [[ "$*" == "ec2 describe-nat-gateways"* ]]; then
      [[ "$*" == *"State != 'deleted'"* ]] && printf '0\n' || printf '1\n'
      exit 0
    fi
  fi
  if [[ "$*" == "ecr"* || "$*" == "ec2"* || "$*" == "autoscaling"* || "$*" == "cloudformation"* || "$*" == "elbv2"* ]]; then
    category=${EKS_LEFTOVER_CATEGORY:-}
    if [[ ( "$category" == ec2 && "$*" == "ec2 describe-instances"* ) || ( "$category" == asg && "$*" == "autoscaling"* ) || ( "$category" == cloudformation && "$*" == "cloudformation"* ) || ( "$category" == ebs && "$*" == "ec2 describe-volumes"* ) || ( "$category" == elb && "$*" == "elbv2"* ) || ( "$category" == nat && "$*" == "ec2 describe-nat-gateways"* ) || ( "$category" == eip && "$*" == "ec2 describe-addresses"* ) ]]; then printf '1\n'; else printf '0\n'; fi
    exit 0
  fi
  ;;
	eksctl)
	if [[ "$*" == "delete cluster"* && "${EKS_FAKE_REQUIRE_PORT_FORWARDS_STOPPED_BEFORE_CLEANUP:-0}" == 1 ]]; then
	  markers="$EKS_FAKE_STATE_DIR/stopped-port-forwards"
	  [[ -f "$markers" && $(grep -c '^stopped$' "$markers") -eq 2 ]] || { printf 'port-forwards remained active when cleanup began\n' >&2; exit 1; }
	fi
	if [[ "$*" == "delete cluster"* && "${EKS_FAKE_EKSCTL_DELETE_BLOCK:-0}" == 1 ]]; then
		  if [[ "${EKS_FAKE_EKSCTL_DELETE_DESCENDANT:-0}" == 1 ]]; then
		    (
		      trap '' TERM
		      publish_ready eksctl-delete-descendant-ready
		      while sleep 1; do :; done
		    ) &
		    printf '%s\n' "$!" >"$EKS_FAKE_STATE_DIR/eksctl-delete-descendant-pid"
	  fi
	  trap 'printf "terminated\\n" >"$EKS_FAKE_STATE_DIR/eksctl-delete-terminated"; exit 143' TERM
	  publish_ready eksctl-delete-ready
	  while sleep 1; do :; done
	fi
	if [[ "$*" == "create cluster"* && "$*" != *"--dry-run"* && "${EKS_FAKE_EKSCTL_BLOCK:-0}" == 1 ]]; then
	    printf 'created\n' >"$EKS_FAKE_STATE_DIR/cluster-created"
	    trap 'printf "terminated\\n" >"$EKS_FAKE_STATE_DIR/eksctl-terminated"; exit 143' TERM
	    publish_ready eksctl-create-ready
	    while sleep 0.05; do :; done
    printf 'completed\n' >"$EKS_FAKE_STATE_DIR/eksctl-terminated"
    exit 0
  fi
	if [[ "$*" == "create cluster"* && "$*" != *"--dry-run"* && -n "${EKS_FAKE_EKSCTL_CREATE_DELAY_SECONDS:-}" ]]; then
	  sleep "$EKS_FAKE_EKSCTL_CREATE_DELAY_SECONDS"
	  printf 'completed\n' >"$EKS_FAKE_STATE_DIR/eksctl-create-completed"
	  printf 'created\n' >"$EKS_FAKE_STATE_DIR/cluster-created"
	  exit 0
	fi
	if [[ "$*" == "create cluster"* && "$*" != *"--dry-run"* && "${EKS_BENCHMARK_FAIL_PHASE:-}" == "cluster" ]]; then exit 1; fi
  exit 0
  ;;
kubectl)
	  if [[ "${EKS_FAKE_DIAGNOSTIC_BLOCK:-0}" == 1 && ( "$*" == "get all"* || "$*" == "get events"* || "$*" == "logs job/load-tester-orchestrator"* ) ]]; then
	    while sleep 0.05; do :; done
	  fi
  if [[ "${EKS_FAKE_KUBECTL_DELETE_EXPOSED_FAIL:-0}" == 1 && "$*" == "delete service"* ]]; then
    printf 'forbidden: fake exposed cleanup failure\n' >&2
    exit 1
  fi
  if [[ "$*" == "port-forward"* ]]; then
    printf '%s\n' "$$" >>"$EKS_FAKE_STATE_DIR/port-forward-pids"
    if [[ "${EKS_FAKE_PORT_FORWARD_IGNORE_TERM:-0}" == 1 ]]; then
      trap 'printf "term\\n" >>"$EKS_FAKE_STATE_DIR/port-forward-term"' TERM
    else
      trap 'printf "stopped\\n" >>"$EKS_FAKE_STATE_DIR/stopped-port-forwards"; exit 0' INT TERM
    fi
    while true; do sleep 1; done
  fi
  if [[ "$*" == "version --output json"* ]]; then printf '{"serverVersion":{"gitVersion":"v1.34.1"}}\n'; exit 0; fi
  if [[ "$*" == "get nodes"*"load-tester.io/pool=loadgen"* || "$*" == "get nodes"*"load-tester.io/pool=platform"* ]]; then printf 'node-a Ready\nnode-b Ready\n'; exit 0; fi
  if [[ "$*" == "logs job/load-tester-orchestrator"* ]]; then printf 'Result: PASS\n'; fi
  exit 0
  ;;
kustomize)
  if [[ "$1" == build ]]; then printf '%s\n' "$EKS_KUSTOMIZE_BUILD"; fi
  exit 0
  ;;
docker)
	if [[ "$*" == "login --username AWS --password-stdin"* && "${EKS_FAKE_DOCKER_LOGIN_REQUIRES_PASSWORD:-0}" == 1 ]]; then
	  IFS= read -r password || { printf 'fake docker login requires a password on stdin\n' >&2; exit 1; }
	  [[ "$password" == fake-password ]] || { printf 'fake docker login received an unexpected password\n' >&2; exit 1; }
	fi
  if [[ "$*" == "buildx build"* && "${EKS_FAKE_DOCKER_BUILDX_BLOCK:-0}" == 1 ]]; then
    printf '%s\n' "$$" >"$EKS_FAKE_STATE_DIR/docker-buildx-pid"
    trap 'printf "terminated\\n" >"$EKS_FAKE_STATE_DIR/docker-buildx-terminated"; exit 143' TERM
    publish_ready docker-buildx-ready
    while sleep 1; do :; done
  fi
  exit 0
  ;;
curl)
  if [[ "$*" == *checkip.amazonaws.com* ]]; then
    printf '%s\n' "$PUBLIC_IP"
	  elif [[ "${EKS_FAKE_BLOCK_PROMETHEUS_QUERY:-0}" == 1 && "$*" == *"/api/v1/query"* && "$*" != *"/api/v1/query_range"* ]]; then
	    printf '%s\n' "$$" >"$EKS_FAKE_STATE_DIR/blocked-prometheus-query-pid"
	    trap 'printf "terminated\\n" >"$EKS_FAKE_STATE_DIR/blocked-prometheus-query-terminated"; exit 143' TERM
	    publish_ready blocked-prometheus-query-ready
	    while true; do :; done
  elif [[ "${EKS_FAKE_LOCAL_ENDPOINT_DOWN:-0}" == 1 && "$*" == *127.0.0.1* ]]; then
    printf 'fake local endpoint is not ready\n' >&2
    exit 7
  elif [[ -n "${EKS_FAKE_GRAFANA_FAIL_AFTER:-}" && "$*" == *"/api/health"* ]]; then
    state="$EKS_FAKE_STATE_DIR/grafana-health-check-count"
    count=0
    [[ -f "$state" ]] && count=$(cat "$state")
    count=$((count + 1))
    printf '%s\n' "$count" >"$state"
    if (( count > EKS_FAKE_GRAFANA_FAIL_AFTER )); then
      printf 'fake Grafana endpoint became unreachable\n' >&2
      exit 7
    fi
  elif [[ "$*" == *"/api/v1/query_range"* ]]; then
    value=1
    case "$*" in
      *loadtester_request_failures_total*|*loadtester_dropped_samples_total*|*loadtester_unmet_demand_total*) value=0 ;;
      *loadtester_autoscaler_scheduler_coverage_ratio*) value=0.95 ;;
      *loadtester_active_workers*) [[ "${EKS_PROMETHEUS_PHASE:-}" == target-hpa ]] && value=2 || value=4 ;;
      *kube_horizontalpodautoscaler_status_current_replicas*)
        if [[ "${EKS_PROMETHEUS_PHASE:-}" == target-hpa && -n "${EKS_HPA_REPLICAS_SEQUENCE:-}" ]]; then
          state="$EKS_FAKE_STATE_DIR/hpa-range-replica-query-count"
          count=0
          [[ -f "$state" ]] && count=$(cat "$state")
          IFS=, read -r -a values <<<"$EKS_HPA_REPLICAS_SEQUENCE"
          index=$count
          (( index < ${#values[@]} )) || index=$((${#values[@]} - 1))
          value=${values[$index]}
          printf '%s\n' $((count + 1)) >"$state"
        else
          value=3
        fi
        ;;
      *histogram_quantile*) [[ "${EKS_PROMETHEUS_PHASE:-}" == target-baseline ]] && value=0.02 || value=0.01 ;;
      *loadtester_requests_total*) value=125 ;;
    esac
    if [[ "${EKS_PROMETHEUS_NONZERO:-}" == hpa-dropped-samples && "$*" == *loadtester_dropped_samples_total* ]]; then
      state="$EKS_FAKE_STATE_DIR/dropped-range-query-count"
      count=0
      [[ -f "$state" ]] && count=$(cat "$state")
      count=$((count + 1))
      printf '%s\n' "$count" >"$state"
      (( count >= 2 )) && value=1
    fi
    printf '{"data":{"result":[{"values":[["1","%s"],["2","%s"]]}]}}\n' "$value" "$value"
  elif [[ "$*" == *" --output "* ]]; then
    for ((index = 1; index <= $#; index++)); do
      if [[ "${!index}" == "--output" ]]; then
        next=$((index + 1))
        printf 'fake-dashboard' >"${!next}"
      fi
    done
  else
    value=1
    case "$*" in
      *loadtester_request_failures_total*|*loadtester_dropped_samples_total*|*loadtester_unmet_demand_total*) value=0 ;;
      *loadtester_autoscaler_scheduler_coverage_ratio*) value=0.95 ;;
      *loadtester_active_workers*) [[ "${EKS_PROMETHEUS_PHASE:-}" == target-hpa ]] && value=2 || value=4 ;;
	      *kube_horizontalpodautoscaler_status_current_replicas*)
	        if [[ "${EKS_PROMETHEUS_PHASE:-}" == target-hpa && -n "${EKS_HPA_REPLICAS_SEQUENCE:-}" ]]; then
	          state="$EKS_FAKE_STATE_DIR/hpa-replica-query-count"
	          count=0
	          [[ -f "$state" ]] && count=$(cat "$state")
	          IFS=, read -r -a values <<<"$EKS_HPA_REPLICAS_SEQUENCE"
	          index=$count
	          (( index < ${#values[@]} )) || index=$((${#values[@]} - 1))
	          value=${values[$index]}
	          printf '%s\n' $((count + 1)) >"$state"
	        else
	          value=3
	        fi
	        ;;
      *histogram_quantile*)
        [[ "${EKS_PROMETHEUS_PHASE:-}" == target-baseline ]] && value=0.02 || value=0.01
        ;;
      *loadtester_requests_total*) value=${EKS_PROMETHEUS_BASELINE_RPS:-125} ;;
    esac
    printf '{"data":{"result":[{"value":["0","%s"]}]}}\n' "$value"
  fi
  exit 0
  ;;
git)
  if [[ "${1:-}" == "rev-parse" && "${2:-}" == "HEAD" ]]; then
    printf '%s\n' "$FAKE_GIT_SHA"
  elif [[ "${1:-}" == "status" && "${GIT_DIRTY:-0}" == 1 ]]; then
    printf '?? untracked-file\n'
  fi
  ;;
date)
  if [[ "$*" == *+%s* ]]; then
    printf '%s\n' "${EKS_BENCHMARK_NOW:-1724889600}"
  elif [[ "$*" == *+%Y%m%dT%H%M%SZ* ]]; then
    printf '20260828T000000Z\n'
  else
    printf '%s\n' "${EKS_BENCHMARK_DATE:-2026-08-28}"
  fi
  exit 0
  ;;
esac
exit 0
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func fakeRenderedManifest() string {
	return strings.Join([]string{
		"apiVersion: v1", "kind: Service", "metadata:", "  name: target", "spec:", "  type: ClusterIP",
		"---", "apiVersion: apps/v1", "kind: Deployment", "metadata:", "  name: worker", "spec:", "  template:", "    spec:", "      nodeSelector:", "        load-tester.io/pool: loadgen",
		"      containers:", "      - image: 123456789012.dkr.ecr.us-west-2.amazonaws.com/load-tester-worker:sha-" + eksSHA,
		"---", "apiVersion: apps/v1", "kind: Deployment", "metadata:", "  name: target", "spec:", "  template:", "    spec:", "      nodeSelector:", "        load-tester.io/pool: platform",
		"      containers:", "      - image: 123456789012.dkr.ecr.us-west-2.amazonaws.com/load-tester-target:sha-" + eksSHA,
		"---", "apiVersion: batch/v1", "kind: Job", "metadata:", "  name: orchestrator", "spec:", "  template:", "    spec:", "      nodeSelector:", "        load-tester.io/pool: platform",
		"      containers:", "      - image: 123456789012.dkr.ecr.us-west-2.amazonaws.com/load-tester-orchestrator:sha-" + eksSHA,
	}, "\n")
}

func latestRunDir(t *testing.T, root string) string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "artifacts", "eks"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("run directories entries=%v err=%v", entries, err)
	}
	return filepath.Join(root, "artifacts", "eks", entries[0].Name())
}

func assertRecordedProcessesGone(t *testing.T, path string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read recorded process IDs: %v", err)
	}
	pids := strings.Fields(string(contents))
	if len(pids) != 2 {
		t.Fatalf("recorded port-forward pids=%q want two", contents)
	}
	for _, value := range pids {
		pid, err := strconv.Atoi(value)
		if err != nil || pid <= 0 {
			t.Fatalf("invalid recorded process ID %q", value)
		}
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("port-forward process %d still exists or could not be checked: %v", pid, err)
		}
	}
}

func readRecordedPID(t *testing.T, path string) int {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read recorded PID: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
	if err != nil || pid <= 0 {
		t.Fatalf("invalid recorded PID %q: %v", contents, err)
	}
	return pid
}

func killRecordedProcessGroup(t *testing.T, pid int) {
	t.Helper()
	group, err := syscall.Getpgid(pid)
	if errors.Is(err, syscall.ESRCH) {
		return
	}
	if err != nil {
		t.Fatalf("read process group for %d: %v", pid, err)
	}
	if err := syscall.Kill(-group, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("kill test process group %d: %v", group, err)
	}
}

func readCalls(t *testing.T, callLog string) []string {
	t.Helper()
	contents, err := os.ReadFile(callLog)
	if err != nil {
		t.Fatalf("read calls: %v", err)
	}
	return strings.FieldsFunc(strings.TrimSpace(string(contents)), func(r rune) bool { return r == '\n' })
}

func readJSONFixture(t *testing.T, path string) map[string]any {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(contents, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func writeOwnedRunFixture(t *testing.T, root string, overrides map[string]any) string {
	t.Helper()
	runDir := filepath.Join(root, "artifacts", "eks", "20260828T000000Z-0123456")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	state := map[string]any{
		"schema_version": 1,
		"account":        "123456789012",
		"region":         eksRegion,
		"cluster":        eksName,
		"run_id":         "20260828T000000Z-0123456",
		"git_sha":        eksSHA,
		"zones":          []string{"us-west-2a", "us-west-2b"},
		"start_epoch":    1724889600,
		"deadline_epoch": 1724895000,
		"run_date":       "2026-08-28",
	}
	for key, value := range overrides {
		state[key] = value
	}
	contents, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "owned-run.json"), contents, 0o600); err != nil {
		t.Fatal(err)
	}
	return runDir
}

func writeCleanupReport(t *testing.T, runDir, status string) {
	t.Helper()
	contents := []byte(fmt.Sprintf(`{"status":%q,"account":"123456789012","region":%q,"cluster":%q}`, status, eksRegion, eksName))
	if err := os.WriteFile(filepath.Join(runDir, "cleanup-report.json"), contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "report.json"), []byte(`{"run_id":"20260828T000000Z-0123456","billing_attestation":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "metrics.json"), []byte(`{"run_id":"20260828T000000Z-0123456","billing_attestation":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeRedactorFixture(t *testing.T, root string) string {
	t.Helper()
	path := filepath.Join(root, "fake-redactor.py")
	script := "import os\nimport sys\nwith open(os.environ['EKS_FAKE_CALL_LOG'], 'a', encoding='utf-8') as out:\n    out.write('python3 ' + sys.argv[0] + ' ' + ' '.join(sys.argv[1:]) + '\\n')\n"
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeDashboardFixture(t *testing.T, root string) (string, string) {
	t.Helper()
	pngPath := filepath.Join(root, "reviewed-dashboard.png")
	png := []byte("human-reviewed-dashboard-fixture")
	if err := os.WriteFile(pngPath, png, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(png)
	reviewPath := filepath.Join(root, "dashboard-review.json")
	review := fmt.Sprintf(`{"approved":true,"reviewed_by":"test operator","reviewed_on":"2026-08-28","dashboard_sha256":"%x"}`, digest)
	if err := os.WriteFile(reviewPath, []byte(review), 0o644); err != nil {
		t.Fatal(err)
	}
	return pngPath, reviewPath
}

func mergeEnvironment(target, source map[string]string) {
	for key, value := range source {
		target[key] = value
	}
}

func assertNoMutatingCalls(t *testing.T, calls []string) {
	t.Helper()
	for _, call := range calls {
		if strings.Contains(call, "--help") || strings.Contains(call, "--dry-run") {
			continue
		}
		for _, forbidden := range []string{" create ", " delete ", " put-", " update ", " terminate ", "--push"} {
			if strings.Contains(" "+call+" ", forbidden) {
				t.Fatalf("unexpected mutation call: %s", call)
			}
		}
	}
}

func assertCallsOnly(t *testing.T, calls []string, prefixes ...string) {
	t.Helper()
	for _, call := range calls {
		matched := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(call, prefix) {
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("unexpected call: %s\nallowed: %s", call, strings.Join(prefixes, ", "))
		}
	}
}

func assertCallCount(t *testing.T, calls []string, prefix string, want int) {
	t.Helper()
	count := 0
	for _, call := range calls {
		if strings.HasPrefix(call, prefix) {
			count++
		}
	}
	if count != want {
		t.Fatalf("calls with %q = %d want %d\n%s", prefix, count, want, strings.Join(calls, "\n"))
	}
}

func assertCallContains(t *testing.T, calls []string, prefix string, wants ...string) {
	t.Helper()
	for _, call := range calls {
		if strings.HasPrefix(call, prefix) {
			matched := true
			for _, want := range wants {
				if !strings.Contains(call, want) {
					matched = false
				}
			}
			if matched {
				return
			}
		}
	}
	t.Fatalf("no call matching prefix=%q contents=%q\n%s", prefix, wants, strings.Join(calls, "\n"))
}

func callsContain(calls []string, prefix string, wants ...string) bool {
	for _, call := range calls {
		if !strings.HasPrefix(call, prefix) {
			continue
		}
		matched := true
		for _, want := range wants {
			if !strings.Contains(call, want) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func assertCallAbsent(t *testing.T, calls []string, prefix string) {
	t.Helper()
	for _, call := range calls {
		if strings.HasPrefix(call, prefix) {
			t.Fatalf("unexpected call: %s", call)
		}
	}
}

func assertNoCallContaining(t *testing.T, calls []string, prefix, forbidden string) {
	t.Helper()
	for _, call := range calls {
		if strings.HasPrefix(call, prefix) && strings.Contains(call, forbidden) {
			t.Fatalf("unexpected call: %s", call)
		}
	}
}

func assertCallBefore(t *testing.T, calls []string, first, second string) {
	t.Helper()
	firstIndex, secondIndex := -1, -1
	for index, call := range calls {
		if firstIndex < 0 && strings.HasPrefix(call, first) {
			firstIndex = index
		}
		if secondIndex < 0 && strings.HasPrefix(call, second) {
			secondIndex = index
		}
	}
	if firstIndex < 0 || secondIndex < 0 || firstIndex >= secondIndex {
		t.Fatalf("expected %q before %q, calls:\n%s", first, second, strings.Join(calls, "\n"))
	}
}

func assertCallOrder(t *testing.T, calls []string, prefixes ...string) {
	t.Helper()
	indexes := make([]int, 0, len(prefixes))
	for _, prefix := range prefixes {
		index := -1
		for callIndex, call := range calls {
			if strings.HasPrefix(call, prefix) {
				index = callIndex
				break
			}
		}
		if index < 0 {
			t.Fatalf("missing call %q\n%s", prefix, strings.Join(calls, "\n"))
		}
		indexes = append(indexes, index)
	}
	if !sort.IntsAreSorted(indexes) {
		t.Fatalf("calls out of order %v\n%s", indexes, strings.Join(calls, "\n"))
	}
}

func assertCallSubsequence(t *testing.T, calls []string, prefixes ...string) {
	t.Helper()
	from := 0
	for _, prefix := range prefixes {
		found := -1
		for index := from; index < len(calls); index++ {
			if strings.HasPrefix(calls[index], prefix) {
				found = index
				break
			}
		}
		if found < 0 {
			t.Fatalf("missing ordered call %q after index %d:\n%s", prefix, from, strings.Join(calls, "\n"))
		}
		from = found + 1
	}
}
