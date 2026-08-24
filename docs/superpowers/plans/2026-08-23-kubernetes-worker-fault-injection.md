# Kubernetes Worker Fault Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove that an active Kubernetes worker pod can be deleted during a distributed constant-rate run, with load redistributed, the pod replaced, accepted metrics retained, and the integrity loss reported honestly.

**Architecture:** A dedicated `kind-fault` Kustomize overlay deploys the existing stack with a fixed three-worker, one-target scenario. A disposable-cluster acceptance script selects a worker that has produced requests, deletes it normally, and correlates Kubernetes pod identity with orchestrator Prometheus metrics to prove loss, replacement, and recovery. Existing Piece 14 recovery logic remains unchanged unless the live test exposes a reproducible defect covered by a focused failing test.

**Tech Stack:** Go 1.25 tests, YAML/Kustomize, Bash, kind/Kubernetes 1.34.8, Prometheus HTTP API, Grafana HTTP API, Docker

**Spec:** `docs/design.md` section "Worker fault injection under Kubernetes" and `docs/roadmap.md` Piece 22

## Global Constraints

- Use a normal `kubectl delete pod`; do not use `--force`, zero grace periods, or `SIGKILL`.
- Keep the target at exactly one replica and worker desired capacity at exactly three throughout active load.
- Use `constant-rate` at 90 requests/second for two minutes, a 10-second ramp, and `max_in_flight: 300` against `http://autoscale-target:8080/delay?ms=20`.
- Require every worker to contribute traffic before selecting the victim.
- Require a 10-second post-recovery rate of at least 81 requests/second, which is 90 percent of configured demand.
- Preserve accepted complete deltas; do not reconstruct the deleted process's unfinished interval.
- The expected load-test outcome is exit code 1 and `Result: FAIL`; the expected acceptance-script outcome is exit code 0.
- Do not require nonzero unmet demand because recovery can occur between scheduled arrivals.
- Reuse the `Load Tester Overview` dashboard without changing its Compose-compatible JSON.
- Preserve failed clusters only when `KEEP_KIND_CLUSTER=1`; otherwise always delete the owned kind cluster.
- Do not stage, commit, or push until Marcus reviews the complete implementation and explicitly approves it.

---

### Task 1: Add the fixed fault scenario and Kustomize overlay

**Files:**
- Create: `deploy/kubernetes/fault-injection/demo.yaml`
- Create: `deploy/kubernetes/fault-injection/kustomization.yaml`
- Create: `deploy/kubernetes/overlays/kind-fault/kustomization.yaml`
- Create: `deploy/kubernetes/fault_manifest_test.go`

**Interfaces:**
- Consumes: `deploy/kubernetes/overlays/kind`, the base `load-test-config` volume expected by the orchestrator Job, and `internal/config.Parse(io.Reader)`.
- Produces: a renderable `deploy/kubernetes/overlays/kind-fault` stack containing ConfigMap `load-test-config` with key `demo.yaml`.

- [ ] **Step 1: Write failing manifest and scenario tests**

Add `TestFaultInjectionScenario` to parse the YAML and require this exact runtime configuration:

```go
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
```

Add `TestKindFaultOverlayRendersFixedExperiment` using the existing `renderOverlay` helper. Require a suspended orchestrator Job, worker Deployment starting at zero, target Deployment with one replica, and `load-test-config.data["demo.yaml"]` containing the exact parsed scenario.

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./deploy/kubernetes -run 'TestFaultInjectionScenario|TestKindFaultOverlayRendersFixedExperiment' -count=1
```

Expected: FAIL because the scenario and `kind-fault` overlay do not exist.

- [ ] **Step 3: Add the scenario and overlay**

Create `demo.yaml` with the exact values from Global Constraints and disabled latency/error thresholds:

```yaml
name: kubernetes-worker-fault-injection

target:
  protocol: http
  method: GET
  url: http://autoscale-target:8080/delay?ms=20

load:
  model: constant-rate
  rate: 90
  max_in_flight: 300
  duration: 2m
  ramp_up: 10s

fleet:
  min_workers: 3
  max_workers: 3
  autoscale: false

thresholds:
  p99_latency: 0
  error_rate: 1
```

The fault-injection Kustomization generates `load-test-config` from `demo.yaml`. The `kind-fault` overlay composes `../kind` and `../../fault-injection`, preserving the suspended Job and local image tags.

- [ ] **Step 4: Run focused and render verification**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./deploy/kubernetes -run 'TestFaultInjectionScenario|TestKindFaultOverlayRendersFixedExperiment' -count=1
kubectl kustomize deploy/kubernetes/overlays/kind-fault >/dev/null
```

Expected: both commands PASS.

### Task 2: Define the shell acceptance contract before implementation

**Files:**
- Create: `scripts/test-kind-fault_test.go`
- Create: `scripts/test-kind-fault.sh`

**Interfaces:**
- Consumes: the `kind-fault` overlay from Task 1 and the existing `KIND_BIN`, `KIND_CLUSTER_NAME`, `KIND_NODE_IMAGE`, `TIMEOUT_SECONDS`, and `KEEP_KIND_CLUSTER` environment conventions.
- Produces: an executable acceptance entrypoint at `scripts/test-kind-fault.sh`.

- [ ] **Step 1: Write failing shell syntax and contract tests**

Add `TestKindFaultScriptHasValidShellSyntax`, invoking:

```go
command := exec.Command("bash", "-n", "test-kind-fault.sh")
```

Add `TestKindFaultScriptCoversAcceptanceContract` and require the script to contain each contract marker:

```go
for _, want := range []string{
	"KEEP_KIND_CLUSTER", "deploy/kubernetes/overlays/kind-fault",
	"EXPECTED_WORKERS=3", "EXPECTED_RPS=90", "MIN_THROUGHPUT_RATIO=0.90",
	"RECOVERY_WINDOW_SECONDS=10", "loadtester_active_workers",
	"loadtester_worker_active", "loadtester_worker_requests_total",
	"kubectl_kind delete pod", "metadata.uid", "Result: FAIL",
	"unreported metric interval lost", "workers=0", "cluster=deleted",
} {
	if !strings.Contains(script, want) {
		t.Errorf("test-kind-fault.sh does not contain %q", want)
	}
}
```

Add a separate test asserting the script does not contain `--force` or `--grace-period=0`.

- [ ] **Step 2: Run the script tests and verify they fail**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run KindFault -count=1
```

Expected: FAIL because `test-kind-fault.sh` does not exist.

- [ ] **Step 3: Add lifecycle, deployment, and diagnostics scaffolding**

Follow the established Piece 21 script conventions for command checks, reserved localhost ports, namespaced `kubectl_kind`, image builds, sequential `crictl pull` inside the kind node, port-forward cleanup, and cluster ownership. Use a distinct default cluster name:

```bash
CLUSTER_NAME=${KIND_CLUSTER_NAME:-load-tester-piece22-$$}
EXPECTED_WORKERS=3
EXPECTED_RPS=90
MIN_THROUGHPUT_RATIO=0.90
RECOVERY_WINDOW_SECONDS=10
```

Diagnostics must include deployments and pods with UIDs, sorted events, the orchestrator Job log, logs from every current worker pod, and the final Prometheus values used by the assertions. Cleanup must preserve an owned cluster only under `KEEP_KIND_CLUSTER=1`.

- [ ] **Step 4: Deploy the isolated experiment and observability stack**

Build and load orchestrator, worker, and target images; cache Prometheus, Grafana, and BusyBox sequentially in the kind node; apply `deploy/kubernetes/overlays/kind-fault`; and wait for target, Prometheus, and Grafana Deployments. Verify Grafana search returns UID `load-tester-overview`, Prometheus reports `up{job="orchestrator"} == 1` after the Job starts, and target desired/ready replicas remain exactly one.

- [ ] **Step 5: Run syntax and focused contract tests**

Run:

```bash
bash -n scripts/test-kind-fault.sh
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run KindFault -count=1
```

Expected: PASS.

### Task 3: Inject one active-worker failure and prove recovery

**Files:**
- Modify: `scripts/test-kind-fault.sh`
- Modify: `scripts/test-kind-fault_test.go`

**Interfaces:**
- Consumes: Prometheus instant/range query helpers and Kubernetes pod JSON from Task 2.
- Produces: captured `VICTIM_NAME`, `VICTIM_UID`, `VICTIM_REQUESTS_BEFORE`, and `REPLACEMENT_NAME`, plus verified recovery timestamps and rate samples.

- [ ] **Step 1: Strengthen the failing contract test for victim selection and historical retention**

Require markers for all preconditions and observations:

```go
for _, want := range []string{
	"VICTIM_REQUESTS_BEFORE", "REPLACEMENT_NAME",
	"loadtester_worker_active{worker_id=", "/api/v1/query_range",
	"loadtester_active_workers",
	"rate(loadtester_requests_total[10s])", "replacement worker did not contribute requests",
	"deleted worker request history regressed",
} {
	if !strings.Contains(script, want) {
		t.Errorf("test-kind-fault.sh does not contain %q", want)
	}
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run KindFault -count=1
```

Expected: FAIL because recovery assertions are not implemented yet.

- [ ] **Step 3: Implement steady-state and victim selection**

Unsuspend the orchestrator Job. Poll until Deployment desired/ready replicas are three, `loadtester_active_workers == 3`, each current pod has `loadtester_worker_active{worker_id="<pod>"} == 1`, and each has a positive `loadtester_worker_requests_total`. Then require at least 20 seconds at or above 81 RPS using Prometheus samples. Select one of those contributing pod names deterministically, record its UID and current request counter, and print the injection timestamp.

- [ ] **Step 4: Delete normally and prove the dip**

Issue exactly:

```bash
kubectl_kind delete pod "$VICTIM_NAME" --namespace "$NAMESPACE" --wait=false >/dev/null
```

Record the deletion epoch immediately before the command. After recovery, query
Prometheus `/api/v1/query_range` for `loadtester_active_workers` with `start`
equal to that deletion epoch, `end` equal to the recorded recovery epoch, and a
one-second step. Require at least one returned value below three. Independently
require that no pod with `VICTIM_UID` remains. Do not use a rolling range that
can include startup samples, and do not infer the active-worker dip solely from
Kubernetes Ready count because the orchestrator connection is the recovery
contract being tested.

- [ ] **Step 5: Prove replacement registration and traffic recovery**

Within 30 seconds, find a Ready worker pod whose name and UID differ from the victim. Require its `loadtester_worker_active` gauge to become one and its request counter to become positive. Require `loadtester_active_workers` to return to three, worker Deployment desired/ready replicas to equal three, and a full 10-second Prometheus rate window to reach at least 81 RPS.

- [ ] **Step 6: Prove accepted victim history remains available**

Query the deleted worker's last exported `loadtester_worker_requests_total{worker_id="<victim>"}` sample before the orchestrator exits. Fail if it is absent or lower than `VICTIM_REQUESTS_BEFORE`. This permits a higher value because one final complete delta may race with deletion, while forbidding loss of already accepted counters.

- [ ] **Step 7: Run focused static verification**

Run:

```bash
bash -n scripts/test-kind-fault.sh
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run KindFault -count=1
```

Expected: PASS.

### Task 4: Validate failure semantics and intentional teardown

**Files:**
- Modify: `scripts/test-kind-fault.sh`
- Modify: `scripts/test-kind-fault_test.go`

**Interfaces:**
- Consumes: victim identity and completed orchestrator Job from Task 3.
- Produces: a successful acceptance-script exit only for the expected failed load result and clean lifecycle.

- [ ] **Step 1: Add failing result-contract assertions**

Require the script to check exact expected process semantics:

```go
for _, want := range []string{
	"orchestrator_exit", `orchestrator_exit" != 1`, "Result: FAIL",
	"worker $VICTIM_NAME failed:", "unreported metric interval lost",
	"failure_violation_count", "Dropped samples: 0",
} {
	if !strings.Contains(script, want) {
		t.Errorf("test-kind-fault.sh does not contain %q", want)
	}
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run KindFault -count=1
```

Expected: FAIL until result and teardown checks exist.

- [ ] **Step 3: Validate the expected failed load result**

After the Job terminates, require container exit code `1`, `Result: FAIL`, `Dropped samples: 0`, and exactly one summary violation matching:

```text
worker <victim-name> failed: stream closed; unreported metric interval lost
```

Allow a separate `unmet demand: N` violation when `N > 0`; do not require it. Reject exit 0 because it hides integrity loss, and reject exit 2/130 because those represent platform setup failure or interruption.

- [ ] **Step 4: Validate intentional teardown**

Wait for worker Deployment desired replicas and remaining worker pods to reach zero. Because the summary is finalized before managed-fleet cleanup, require only the one mid-run worker-failure violation in the captured result. Mark lifecycle proof complete only after zero workers is observed.

- [ ] **Step 5: Finish cleanup output and rerun focused checks**

On successful cluster deletion print:

```text
Cleanup: PASS (workers=0 cluster=deleted)
```

Run:

```bash
bash -n scripts/test-kind-fault.sh
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run KindFault -count=1
```

Expected: PASS.

### Task 5: Run the real kind experiment, calibrate only from evidence, and finish documentation

**Files:**
- Modify only if measured evidence requires it: `deploy/kubernetes/fault-injection/demo.yaml`
- Modify only with matching assertions: `deploy/kubernetes/fault_manifest_test.go`
- Modify: `docs/design.md`
- Modify: `docs/roadmap.md`

**Interfaces:**
- Consumes: the complete Piece 22 overlay and acceptance script.
- Produces: live evidence that the documented recovery contract works on kind.

- [ ] **Step 1: Run all static and unit verification**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./...
bash -n scripts/test-kind-fault.sh
kubectl kustomize deploy/kubernetes/overlays/kind >/dev/null
kubectl kustomize deploy/kubernetes/overlays/kind-fault >/dev/null
git diff --check
```

Expected: every command PASS.

- [ ] **Step 2: Run the live disposable-cluster acceptance test**

Run:

```bash
KIND_BIN=/tmp/load-tester-kind-bin/kind ./scripts/test-kind-fault.sh
```

Expected evidence includes three initial contributing workers, the victim name and UID, active-worker dip, new replacement identity, at least 81 RPS over the recovery window, retained victim requests, orchestrator exit 1 with the expected integrity violation, workers at zero, and cluster deletion.

- [ ] **Step 3: Debug failures systematically without weakening the contract**

Use `KEEP_KIND_CLUSTER=1` only for diagnostics. Change the 90 RPS rate or 20 ms delay only when repeated measured evidence shows the local environment cannot establish the healthy baseline or recovery window. Any calibration change must update scenario tests, this plan's documented constants, and the concise rationale in `docs/design.md`; do not lower the 90 percent recovery ratio.

- [ ] **Step 4: Update completion documentation with measured evidence**

Keep `docs/design.md` focused on decisions and update Piece 22 in `docs/roadmap.md` with the observed victim/replacement lifecycle and recovery rate. Do not add resume claims or Piece 23 EKS numbers.

- [ ] **Step 5: Request independent reviews**

Use Terra at high effort for a focused script/manifest test review. Use Sol at high effort for the distributed-systems invariants: active-versus-intentional loss, metric retention, replacement identity, and false-positive resistance. Resolve material findings and rerun affected verification.

- [ ] **Step 6: Present the complete diff for Marcus's review**

Report exact main implementation and test file/line references, the commands run, and the live kind evidence. Do not stage, commit, or push. Wait for explicit approval before any Git write operation.
