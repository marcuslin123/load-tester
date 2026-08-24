package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestKindFaultScriptHasValidShellSyntax(t *testing.T) {
	command := exec.Command("bash", "-n", "test-kind-fault.sh")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("bash -n test-kind-fault.sh: %v\n%s", err, output)
	}
}

func TestKindFaultScriptCoversAcceptanceContract(t *testing.T) {
	contents, err := os.ReadFile("test-kind-fault.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(contents)

	for _, want := range []string{
		"KEEP_KIND_CLUSTER",
		"CLUSTER_OWNED=0",
		"create cluster --name",
		"deploy/kubernetes/overlays/kind-fault",
		"docker build --quiet --target orchestrator",
		"crictl pull",
		"for deployment in autoscale-target prometheus grafana; do",
		"kubectl_kind rollout status \"deployment/$deployment\"",
		"metadata.uid",
		"loadtester_active_workers",
		"loadtester_worker_active",
		"loadtester_worker_requests_total",
		"\"uid\":\"load-tester-overview\"",
		"patch job/load-tester-orchestrator",
		"up{job=\"orchestrator\"}",
		"VICTIM_REQUESTS_BEFORE",
		"REPLACEMENT_NAME",
		"loadtester_worker_active{worker_id=",
		"DIP_ACTIVE_WORKERS",
		"DIP_EPOCH < DELETE_EPOCH || DIP_EPOCH > RECOVERY_EPOCH",
		"rate(loadtester_requests_total[10s])",
		"replacement worker did not contribute requests",
		"deleted worker request history regressed",
		"orchestrator_exit",
		`orchestrator_exit" != 1`,
		"Result: FAIL",
		"worker $VICTIM_NAME failed:",
		"unreported metric interval lost",
		"failure_violation_count",
		"Dropped samples: 0",
		"Cleanup: PASS (workers=0 cluster=deleted)",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("test-kind-fault.sh does not contain %q", want)
		}
	}
}

func kindFaultSummary(violations ...string) string {
	lines := []string{
		"Test: kubernetes-worker-fault-injection",
		"Model: constant-rate",
		"Result: FAIL",
		"Duration: 2m0s",
		"Requests: 123",
		"Succeeded: 120",
		"Failed: 3",
		"Transport errors: 0",
		"Client errors: 0",
		"Server errors: 3",
		"Unmet demand: 9",
		"Dropped samples: 0",
		"Bytes read: 456",
		"Throughput: 1.02 req/s",
		"Error rate: 2.44%",
		"Latency: p50=1ms p95=2ms p99=3ms",
		"Violations:",
	}
	for _, violation := range violations {
		lines = append(lines, "- "+violation)
	}
	return strings.Join(lines, "\n")
}

func bashSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func TestKindFaultScriptAcceptsOnlyOrderedFinalFaultResult(t *testing.T) {
	workerLoss := "worker worker-victim failed: stream closed; unreported metric interval lost"
	validSummary := kindFaultSummary(workerLoss, "unmet demand: 9")
	workerOnlySummary := kindFaultSummary(workerLoss)
	reorderedSummary := strings.Replace(validSummary, "Duration: 2m0s\nRequests: 123", "Requests: 123\nDuration: 2m0s", 1)
	insertedSummary := strings.Replace(validSummary, "Requests: 123", "Requests: 123\nunexpected content", 1)
	duplicateWorkerLoss := kindFaultSummary(workerLoss, "worker worker-other failed: stream closed; unreported metric interval lost")
	duplicateUnmetViolation := kindFaultSummary(workerLoss, "unmet demand: 9", "unmet demand: 9")
	mismatchedUnmetViolation := kindFaultSummary(workerLoss, "unmet demand: 10")
	zeroUnmetViolation := kindFaultSummary(workerLoss, "unmet demand: 0")
	wrongModelSummary := strings.Replace(validSummary, "Model: constant-rate", "Model: open-loop", 1)
	decoyBeforeInvalidFinal := validSummary + "\nlogger output\n" + reorderedSummary
	trailingDecoy := validSummary + "\nlogger output after final summary"

	output, err := runKindFaultHelpers(t, fmt.Sprintf(`
VICTIM_NAME=worker-victim
valid_summary=%s
worker_only_summary=%s
reordered_summary=%s
inserted_summary=%s
duplicate_worker_loss=%s
duplicate_unmet_violation=%s
mismatched_unmet_violation=%s
zero_unmet_violation=%s
wrong_model_summary=%s
decoy_before_invalid_final=%s
trailing_decoy=%s
set +e
validate_orchestrator_result 1 "$valid_summary" 2>/dev/null; valid_status=$?
validate_orchestrator_result 1 "$worker_only_summary" 2>/dev/null; worker_only_status=$?
validate_orchestrator_result 0 "$valid_summary" 2>/dev/null; pass_exit_status=$?
validate_orchestrator_result 2 "$valid_summary" 2>/dev/null; platform_exit_status=$?
validate_orchestrator_result 130 "$valid_summary" 2>/dev/null; interrupt_exit_status=$?
validate_orchestrator_result 1 'orchestrator finished without a summary' 2>/dev/null; missing_summary_status=$?
validate_orchestrator_result 1 "$reordered_summary" 2>/dev/null; reordered_status=$?
validate_orchestrator_result 1 "$inserted_summary" 2>/dev/null; inserted_status=$?
validate_orchestrator_result 1 "$duplicate_worker_loss" 2>/dev/null; duplicate_worker_loss_status=$?
validate_orchestrator_result 1 "$duplicate_unmet_violation" 2>/dev/null; duplicate_unmet_status=$?
validate_orchestrator_result 1 "$mismatched_unmet_violation" 2>/dev/null; mismatched_unmet_status=$?
validate_orchestrator_result 1 "$zero_unmet_violation" 2>/dev/null; zero_unmet_violation_status=$?
validate_orchestrator_result 1 "$wrong_model_summary" 2>/dev/null; wrong_model_status=$?
validate_orchestrator_result 1 "$decoy_before_invalid_final" 2>/dev/null; decoy_status=$?
validate_orchestrator_result 1 "$trailing_decoy" 2>/dev/null; trailing_decoy_status=$?
set -e
printf '%%s %%s %%s %%s %%s %%s %%s %%s %%s %%s %%s %%s %%s %%s %%s\n' "$valid_status" "$worker_only_status" "$pass_exit_status" "$platform_exit_status" "$interrupt_exit_status" "$missing_summary_status" "$reordered_status" "$inserted_status" "$duplicate_worker_loss_status" "$duplicate_unmet_status" "$mismatched_unmet_status" "$zero_unmet_violation_status" "$wrong_model_status" "$decoy_status" "$trailing_decoy_status"
`, bashSingleQuote(validSummary), bashSingleQuote(workerOnlySummary), bashSingleQuote(reorderedSummary), bashSingleQuote(insertedSummary), bashSingleQuote(duplicateWorkerLoss), bashSingleQuote(duplicateUnmetViolation), bashSingleQuote(mismatchedUnmetViolation), bashSingleQuote(zeroUnmetViolation), bashSingleQuote(wrongModelSummary), bashSingleQuote(decoyBeforeInvalidFinal), bashSingleQuote(trailingDecoy)))
	if err != nil {
		t.Fatalf("source and validate orchestrator result fixtures: %v\n%s", err, output)
	}
	if got, want := output, "0 0 1 1 1 1 1 1 1 1 1 1 1 1 1\n"; got != want {
		t.Fatalf("orchestrator result statuses = %q, want %q", got, want)
	}
}

func TestKindFaultScriptRequiresZeroWorkerDeploymentAndPodsForTeardown(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
set +e
kubectl_kind() {
  if [[ "$*" == *"deployment/load-tester-worker"* ]]; then
    printf '%s' "$DEPLOYMENT_REPLICAS"
  else
    printf '%s' "$WORKER_PODS_JSON"
  fi
}
DEPLOYMENT_REPLICAS=0
WORKER_PODS_JSON='{"items":[]}'
worker_teardown_observed
gone=$?
DEPLOYMENT_REPLICAS=1
WORKER_PODS_JSON='{"items":[]}'
worker_teardown_observed
deployment_present=$?
DEPLOYMENT_REPLICAS=0
WORKER_PODS_JSON='{"items":[{"metadata":{"name":"worker-still-running"}}]}'
worker_teardown_observed
pod_present=$?
DEPLOYMENT_REPLICAS=0
WORKER_PODS_JSON='{malformed'
worker_teardown_observed
malformed=$?
set -e
printf '%s %s %s %s\n' "$gone" "$deployment_present" "$pod_present" "$malformed"
`)
	if err != nil {
		t.Fatalf("source and validate worker teardown fixtures: %v\n%s", err, output)
	}
	if got, want := output, "0 1 1 2\n"; got != want {
		t.Fatalf("worker teardown statuses = %q, want %q", got, want)
	}
}

func TestKindFaultScriptVictimUIDCallerHonorsTriState(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
set +e
VICTIM_UID=uid-victim
pod_uid_exists() { return "$POD_UID_STATUS"; }
POD_UID_STATUS=1
verify_victim_uid_absent 2>/dev/null
absent_status=$?
POD_UID_STATUS=2
verify_victim_uid_absent 2>/dev/null
malformed_status=$?
set -e
printf '%s %s\n' "$absent_status" "$malformed_status"
`)
	if err != nil {
		t.Fatalf("source and exercise victim UID caller: %v\n%s", err, output)
	}
	if got, want := output, "0 1\n"; got != want {
		t.Fatalf("victim UID caller statuses = %q, want %q", got, want)
	}
}

func TestKindFaultScriptWorkerTeardownWaitHonorsTriState(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
TIMEOUT_SECONDS=1
sleep() { SECONDS=$((SECONDS + 1)); }
set +e
retry_calls=0
worker_teardown_observed() {
  retry_calls=$((retry_calls + 1))
  if (( retry_calls == 1 )); then
    return 1
  fi
  return 0
}
wait_for_worker_teardown 2>/dev/null
retry_status=$?
error_calls=0
worker_teardown_observed() {
  error_calls=$((error_calls + 1))
  return 2
}
wait_for_worker_teardown 2>/dev/null
error_status=$?
set -e
printf '%s %s %s %s\n' "$retry_status" "$retry_calls" "$error_status" "$error_calls"
`)
	if err != nil {
		t.Fatalf("source and exercise worker teardown wait: %v\n%s", err, output)
	}
	if got, want := output, "0 2 1 1\n"; got != want {
		t.Fatalf("worker teardown wait statuses = %q, want %q", got, want)
	}
}

func TestKindFaultScriptClusterDeletionHonorsLifecycleAndTriStateLookup(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
KIND_BIN=kind
CLUSTER_NAME=fixture-cluster
KUBE_CONTEXT=kind-fixture-cluster
cleanup_failure() { CLEANUP_FAILED=1; }
kind() {
  case "$1 $2" in
    'delete cluster') return "$DELETE_STATUS" ;;
    'get clusters')
      if (( LOOKUP_STATUS != 0 )); then
        return "$LOOKUP_STATUS"
      fi
      printf '%s\n' "$CLUSTER_LIST"
      ;;
  esac
}
run_deletion_fixture() {
  CLEANUP_FAILED=0
  KEEP_KIND_CLUSTER=0
  LIFECYCLE_PROVEN=$1
  DELETE_STATUS=$2
  LOOKUP_STATUS=$3
  CLUSTER_LIST=$4
  delete_owned_cluster
  printf '|status=%s cleanup_failed=%s\n' "$?" "$CLEANUP_FAILED"
}
set +e
run_deletion_fixture 1 1 0 ''
run_deletion_fixture 1 0 2 ''
run_deletion_fixture 1 0 0 fixture-cluster
run_deletion_fixture 0 0 0 ''
run_deletion_fixture 1 0 0 ''
CLEANUP_FAILED=0
KEEP_KIND_CLUSTER=1
LIFECYCLE_PROVEN=1
DELETE_STATUS=99
LOOKUP_STATUS=99
CLUSTER_LIST=fixture-cluster
delete_owned_cluster
keep_status=$?
printf '|keep_status=%s cleanup_failed=%s\n' "$keep_status" "$CLEANUP_FAILED"
set -e
`)
	if err != nil {
		t.Fatalf("source and exercise cluster deletion helper: %v\n%s", err, output)
	}
	want := "|status=1 cleanup_failed=1\n|status=1 cleanup_failed=1\n|status=1 cleanup_failed=1\n|status=1 cleanup_failed=1\nCleanup: PASS (workers=0 cluster=deleted)\n|status=0 cleanup_failed=0\nCluster preserved for debugging: fixture-cluster (context kind-fixture-cluster)\n|keep_status=0 cleanup_failed=0\n"
	if got := output; got != want {
		t.Fatalf("cluster deletion results = %q, want %q", got, want)
	}
}

func TestKindFaultScriptOrdersOwnershipAndObservability(t *testing.T) {
	contents, err := os.ReadFile("test-kind-fault.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(contents)

	createClusterIndex := strings.Index(script, `"$KIND_BIN" create cluster`)
	clusterOwnedIndex := strings.Index(script, "CLUSTER_OWNED=1")
	if createClusterIndex < 0 || clusterOwnedIndex < 0 || clusterOwnedIndex < createClusterIndex {
		t.Error("CLUSTER_OWNED=1 must follow successful kind cluster creation")
	}

	dashboardValidationIndex := strings.Index(script, `if [[ "$dashboard_search"`)
	unsuspendIndex := strings.Index(script, "kubectl_kind patch job/load-tester-orchestrator")
	if dashboardValidationIndex < 0 || unsuspendIndex < 0 || unsuspendIndex < dashboardValidationIndex {
		t.Error("orchestrator unsuspension must follow Grafana dashboard validation")
	}
}

func TestKindFaultScriptAvoidsForcedPodDeletion(t *testing.T) {
	contents, err := os.ReadFile("test-kind-fault.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(contents)

	for _, forbidden := range []string{"--force", "--grace-period=0"} {
		if strings.Contains(script, forbidden) {
			t.Errorf("test-kind-fault.sh must not contain %q", forbidden)
		}
	}
}

func runKindFaultHelpers(t *testing.T, script string) (string, error) {
	t.Helper()
	command := exec.Command("/bin/bash", "-c", "TEST_KIND_FAULT_SOURCE_ONLY=1 source ./test-kind-fault.sh\n"+script)
	output, err := command.CombinedOutput()
	return string(output), err
}

func TestKindFaultScriptCollectsReadyWorkersWithHostBash(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
EXPECTED_WORKERS=3
current_ready_worker_pods() {
  printf 'worker-a\tuid-a\nworker-b\tuid-b\nworker-c\tuid-c\n'
}
deployment_has_replica_count() { return 0; }
prometheus_value() { printf '3\n'; }
worker_is_contributing() { return 0; }
capture_steady_workers
printf '%s|%s|%s\n' "${STEADY_WORKERS[0]}" "${STEADY_WORKERS[1]}" "${STEADY_WORKERS[2]}"
`)
	if err != nil {
		t.Fatalf("source and collect with /bin/bash: %v\n%s", err, output)
	}
	if got, want := output, "worker-a\tuid-a|worker-b\tuid-b|worker-c\tuid-c\n"; got != want {
		t.Fatalf("collected workers = %q, want %q", got, want)
	}

	contents, err := os.ReadFile("test-kind-fault.sh")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "mapfile") {
		t.Fatal("test-kind-fault.sh must not use mapfile because macOS Bash 3.2 lacks it")
	}
}

func TestKindFaultScriptPodUIDExistsDistinguishesAbsenceFromMalformedJSON(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
kubectl_kind() { printf '%s' "$POD_JSON"; }
POD_JSON='{"items":[{"metadata":{"uid":"uid-present"}}]}'
set +e
pod_uid_exists uid-present
present=$?
POD_JSON='{"items":[{"metadata":{"uid":"uid-other"}}]}'
pod_uid_exists uid-absent
absent=$?
POD_JSON='{not-json'
pod_uid_exists uid-absent
malformed=$?
set -e
printf '%s %s %s\n' "$present" "$absent" "$malformed"
`)
	if err != nil {
		t.Fatalf("source and check pod UID fixtures: %v\n%s", err, output)
	}
	if got, want := output, "0 1 2\n"; got != want {
		t.Fatalf("pod UID statuses = %q, want %q", got, want)
	}
}

func TestKindFaultScriptRefreshesWorkersBeforeSelectingVictim(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
steady_worker_captures=0
wait_for_steady_workers() {
  steady_worker_captures=$((steady_worker_captures + 1))
  if (( steady_worker_captures == 1 )); then
    STEADY_WORKERS=("worker-before-throughput"$'\t'"uid-before-throughput")
  else
    STEADY_WORKERS=("worker-fresh"$'\t'"uid-fresh")
  fi
}
wait_for_steady_throughput() {
  STEADY_WORKERS=("worker-stale-after-throughput"$'\t'"uid-stale-after-throughput")
}
prometheus_value() { printf '7\n'; }
reach_steady_state_and_select_victim
printf '%s %s %s %s\n' "$VICTIM_NAME" "$VICTIM_UID" "$VICTIM_REQUESTS_BEFORE" "$steady_worker_captures"
`)
	if err != nil {
		t.Fatalf("source and refresh victim selection: %v\n%s", err, output)
	}
	if got, want := output, "worker-fresh uid-fresh 7 2\n"; got != want {
		t.Fatalf("refreshed victim = %q, want %q", got, want)
	}
}

func TestKindFaultScriptWaitsForFullPostRecoveryRateWindow(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
TIMEOUT_SECONDS=10
RECOVERY_EPOCH=100
RECOVERY_WINDOW_SECONDS=10
MIN_STEADY_RPS=81
mock_now=109
date() { printf '%s\n' "$mock_now"; }
sleep() { mock_now=$((mock_now + 1)); }
prometheus_value() {
  if (( mock_now < 111 )); then
    printf 'Prometheus queried before the post-recovery window elapsed\n' >&2
    return 1
  fi
  printf '81\n'
}
wait_for_recovery_throughput
`)
	if err != nil {
		t.Fatalf("source and wait for recovery rate window: %v\n%s", err, output)
	}
	if got, want := output, "Recovery throughput: PASS (rps=81, window=10s)\n"; got != want {
		t.Fatalf("recovery rate evaluation = %q, want %q", got, want)
	}
}

func TestKindFaultScriptUncordonsSelectedNodeAfterAmbiguousCordon(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
FAULT_NODE_NAME=kind-control-plane
uncordon_args=''
kubectl_kind() { uncordon_args=$*; }
uncordon_fault_node
printf '%s|%s\n' "$uncordon_args" "$FAULT_NODE_NAME"
`)
	if err != nil {
		t.Fatalf("source and uncordon selected node: %v\n%s", err, output)
	}
	if got, want := output, "uncordon kind-control-plane|\n"; got != want {
		t.Fatalf("uncordon after ambiguous cordon = %q, want %q", got, want)
	}
}

func TestKindFaultScriptWaitsForObservableDipBeforeReplacement(t *testing.T) {
	output, err := runKindFaultHelpers(t, `
TIMEOUT_SECONDS=10
mock_active_workers=3
sleep() { mock_active_workers=2; }
date() { printf '105\n'; }
prometheus_value() { printf '%s\n' "$mock_active_workers"; }
wait_for_active_worker_dip
printf 'captured=%s@%s\n' "$DIP_ACTIVE_WORKERS" "$DIP_EPOCH"
`)
	if err != nil {
		t.Fatalf("source and wait for observable worker dip: %v\n%s", err, output)
	}
	if got, want := output, "Active worker dip: PASS (active_workers=2 epoch=105)\ncaptured=2@105\n"; got != want {
		t.Fatalf("active worker dip output = %q, want %q", got, want)
	}
}

func TestKindFaultScriptCordonBoundsReplacementScheduling(t *testing.T) {
	contents, err := os.ReadFile("test-kind-fault.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(contents)

	cordon := strings.Index(script, `kubectl_kind cordon "$FAULT_NODE_NAME"`)
	deletion := strings.Index(script, `kubectl_kind delete pod "$VICTIM_NAME"`)
	dip := strings.LastIndex(script, "wait_for_active_worker_dip")
	uncordon := strings.LastIndex(script, "\nuncordon_fault_node\n")
	replacement := strings.LastIndex(script, "wait_for_replacement")
	if cordon < 0 || deletion < cordon || dip < deletion || uncordon < dip || replacement < uncordon {
		t.Fatalf("fault ordering must be cordon -> delete -> observed dip -> uncordon -> replacement")
	}
}
