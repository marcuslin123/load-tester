# Piece 23 Stage 1 EKS Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and offline-test the guarded EKS benchmark workflow without
creating, modifying, or deleting AWS resources.

**Architecture:** One Bash entrypoint dispatches into a sourced lifecycle library;
Kustomize overlays preserve the existing Kubernetes base while adding EKS-only
placement and resources. An `eksctl` template fixes cluster shape while runtime
rendering supplies safe identity, availability-zone, image, and API-CIDR values.
A small Python program sanitizes raw local evidence before any measured Stage 2
bundle can be committed.

**Tech Stack:** Bash, Python 3 standard library, Go 1.26.5 tests, YAML/Kustomize,
`eksctl`, AWS CLI v2, Amazon ECR/EKS, Docker BuildKit, Kubernetes 1.34

**Spec:** `docs/superpowers/specs/2026-08-28-eks-benchmark-stage-1-design.md`

## Global Constraints

- Stage 1 must not create, modify, or delete any AWS resource.
- Use `eksctl`; Terraform remains a stretch goal.
- Require `AWS_PROFILE=load-tester-eks` and `AWS_REGION=us-west-2` for Stage 2
  mutation.
- Use two dynamically selected availability zones that offer both `c7i.large`
  and `m7i.large`.
- Fix the load-generator pool at two `c7i.large` On-Demand nodes and the
  platform pool at two `m7i.large` On-Demand nodes; do not add node autoscaling.
- Use private nodes, one NAT gateway, no SSH, no Ingress, no LoadBalancer
  Service, and a public Kubernetes API CIDR restricted to the operator's current
  IPv4 `/32`.
- Publish exactly three private ECR images tagged `sha-<full-git-commit>` and
  apply the approved retention policy.
- Keep worker-autoscaling and target-HPA experiments separate and derive
  published figures only from clean measured evidence runs.
- Start the 90-minute deadline before cluster creation and always collect
  diagnostics before sentinel-protected teardown; provide no keep-cluster path.
- Keep raw artifacts under gitignored `artifacts/eks/<run-id>/`; only the redactor
  may create `docs/benchmarks/eks/<run-id>/`.
- Offline tests must use fake commands and require no AWS credentials, network,
  Docker daemon, or Kubernetes cluster.
- Do not stage, commit, or push until Marcus reviews the complete Stage 1
  implementation and explicitly approves those Git operations.

## File Structure

- Create `deploy/eks/cluster.yaml.tmpl`: fixed `eksctl` topology with runtime
  tokens for cluster name, zones, and API CIDR.
- Create `deploy/eks/cluster_config_test.go`: structural tests for the rendered
  cluster contract.
- Create `deploy/eks/scenarios/worker-baseline.yaml`: one-worker 100-RPS
  calibration starting point.
- Create `deploy/eks/scenarios/worker-autoscale.yaml.tmpl`: measured-rate worker
  evidence scenario.
- Create `deploy/eks/scenarios/target-baseline.yaml`: fixed-worker 30-RPS HPA
  calibration starting point.
- Create `deploy/eks/scenarios/target-hpa.yaml.tmpl`: 50-percent-step target HPA
  evidence scenario.
- Create `deploy/eks/scenario_test.go`: parser-level scenario contracts.
- Create `deploy/kubernetes/overlays/eks/kustomization.yaml`: shared EKS image,
  placement, and resource policy over the existing base.
- Create `deploy/kubernetes/overlays/eks/workloads-patch.yaml`: EKS-only selectors
  and resource requests/limits.
- Create `deploy/kubernetes/overlays/eks-worker/kustomization.yaml`: worker-fleet
  experiment stack without target HPA.
- Create `deploy/kubernetes/overlays/eks-hpa/kustomization.yaml`: target-HPA stack
  with Metrics Server, kube-state-metrics, and target dashboard.
- Create `deploy/kubernetes/overlays/eks-hpa/platform-patch.yaml`: platform
  placement for HPA add-ons without the kind insecure-TLS patch.
- Create `deploy/kubernetes/overlays/eks-hpa/prometheus.yml`: HPA scrape config.
- Create `deploy/kubernetes/eks_manifest_test.go`: rendered EKS safety and
  placement assertions.
- Create `scripts/eks-benchmark.sh`: public subcommand dispatcher.
- Create `scripts/lib/eks-benchmark.sh`: guards, rendering, experiment,
  diagnostics, ownership, teardown, and evidence helpers.
- Create `scripts/eks-redact.py`: deterministic structured and text redaction.
- Create `scripts/eks-benchmark_test.go`: fake-command acceptance harness and
  lifecycle tests.
- Create `scripts/eks-redact_test.go`: redaction and evidence-schema tests.
- Modify `.gitignore`: ignore raw EKS artifacts while allowing curated docs.
- Create `docs/eks-benchmark.md`: manual account, SSO, cost, run, emergency
  cleanup, and next-day billing procedure.
- Modify `docs/design.md`: record the approved Piece 23 architecture and safety
  decisions.
- Modify `docs/roadmap.md`: split Piece 23 into Stage 1 automation and Stage 2
  measurement without marking it complete.

---

### Task 1: Lock The `eksctl` Cluster Contract

**Files:**
- Create: `deploy/eks/cluster_config_test.go`
- Create: `deploy/eks/cluster.yaml.tmpl`

**Interfaces:**
- Consumes: runtime values `cluster_name`, `az_a`, `az_b`, and
  `public_api_cidr` from `render_cluster_config` in Task 5.
- Produces: the exact `eksctl.io/v1alpha5` `ClusterConfig` consumed by
  `eksctl create cluster -f <rendered-path>` in Task 6.

- [ ] **Step 1: Write the failing cluster-template tests**

Create a test helper that reads the template, replaces its four complete tokens,
and unmarshals it through `gopkg.in/yaml.v3`:

```go
func renderClusterTemplate(t *testing.T, values map[string]string) map[string]any
```

Add `TestEKSClusterTemplateUsesApprovedFixedTopology` and require:

```go
assertString(t, cfg, "eksctl.io/v1alpha5", "apiVersion")
assertString(t, cfg, "ClusterConfig", "kind")
assertString(t, cfg, "us-west-2", "metadata", "region")
assertStringSlice(t, cfg, []string{"us-west-2a", "us-west-2b"}, "availabilityZones")
assertString(t, cfg, "Single", "vpc", "nat", "gateway")
assertBool(t, cfg, true, "vpc", "clusterEndpoints", "publicAccess")
assertBool(t, cfg, true, "vpc", "clusterEndpoints", "privateAccess")
assertStringSlice(t, cfg, []string{"203.0.113.10/32"}, "vpc", "publicAccessCIDRs")
```

Require exactly two managed node groups named `loadgen` and `platform`. Assert
`c7i.large` and `m7i.large`, `capacityType: ON_DEMAND`, `privateNetworking: true`,
fixed sizes of two, no SSH block, and the exact pool label. Add a rejection test
that fails if the rendered config contains `0.0.0.0/0`, mixed instance types,
Spot capacity, or unequal min/desired/max sizes.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./deploy/eks -run EKSCluster -count=1
```

Expected: FAIL because `cluster.yaml.tmpl` does not exist.

- [ ] **Step 3: Add the minimal cluster template**

Use only these runtime tokens:

```text
__CLUSTER_NAME__
__AZ_A__
__AZ_B__
__PUBLIC_API_CIDR__
```

The template must define Kubernetes version `1.34`, `us-west-2`, both private
managed groups, one NAT gateway, private and restricted public API access, fixed
group sizes, and the approved labels. Do not add IAM users, add-ons, SSH keys,
LoadBalancer resources, or autoscaler tags.

- [ ] **Step 4: Run focused verification**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./deploy/eks -run EKSCluster -count=1
git diff --check -- deploy/eks
```

Expected: PASS.

- [ ] **Step 5: Review checkpoint**

Review the rendered YAML and test output. Do not stage or commit.

---

### Task 2: Add EKS Placement, Resources, And Experiment Scenarios

**Files:**
- Create: `deploy/eks/scenario_test.go`
- Create: `deploy/eks/scenarios/worker-baseline.yaml`
- Create: `deploy/eks/scenarios/worker-autoscale.yaml.tmpl`
- Create: `deploy/eks/scenarios/target-baseline.yaml`
- Create: `deploy/eks/scenarios/target-hpa.yaml.tmpl`
- Create: `deploy/kubernetes/eks_manifest_test.go`
- Create: `deploy/kubernetes/overlays/eks/kustomization.yaml`
- Create: `deploy/kubernetes/overlays/eks/workloads-patch.yaml`
- Create: `deploy/kubernetes/overlays/eks-worker/kustomization.yaml`
- Create: `deploy/kubernetes/overlays/eks-hpa/kustomization.yaml`
- Create: `deploy/kubernetes/overlays/eks-hpa/platform-patch.yaml`
- Create: `deploy/kubernetes/overlays/eks-hpa/prometheus.yml`

**Interfaces:**
- Consumes: `deploy/kubernetes/base`, `deploy/kubernetes/target-hpa`, the existing
  Metrics Server and kube-state-metrics add-ons, and `internal/config.Parse`.
- Produces: `eks-worker` and `eks-hpa` render targets plus four exact scenario
  contracts consumed by Task 6.

- [ ] **Step 1: Write failing scenario tests**

Parse concrete files directly. Render each template by replacing
`__RATE__` with a numeric fixture before calling `config.Parse`.

Require the worker baseline to use `/echo`, `constant-rate`, 100 RPS, one fixed
worker, 90 seconds, a 20-second ramp, and `max_in_flight: 1000`. Require the
worker evidence template to use `/echo`, 1-to-4 workers with autoscaling enabled,
three minutes, a 30-second ramp, and the rendered rate.

Require the target baseline to use `/burn?ms=20`, `Connection: close`, 30 RPS,
two fixed workers, two minutes, and a 10-second ramp. Require the target evidence
template to preserve those isolation settings while accepting only the rendered
50-percent-step rate.

- [ ] **Step 2: Write failing manifest tests**

Extend the existing `renderOverlay` test pattern. In `TestEKSWorkerOverlay`,
require:

```text
worker -> load-tester.io/pool=loadgen
orchestrator, target, Prometheus, Grafana -> load-tester.io/pool=platform
worker request/limit -> 500m+512Mi / 1+1Gi
target request/limit -> 250m+256Mi / 500m+512Mi
orchestrator request/limit -> 500m+512Mi / 1+1Gi
Prometheus request/limit -> 500m+1Gi / 1+2Gi
Grafana request/limit -> 250m+256Mi / 500m+512Mi
```

Require the orchestrator Job to remain suspended until the script supplies a
scenario, the worker Deployment to start at zero, and all three project images
to use safe `example.invalid/load-tester-*:sha-required` markers before runtime
rewrite.

In `TestEKSHPAOverlay`, additionally require HPA 1-to-5 at 60-percent CPU,
Metrics Server and kube-state-metrics on `platform`, existing kube-state-metrics
resources, target-dashboard provisioning, and no
`--kubelet-insecure-tls`. Scan every rendered object and reject Ingress and
`Service.spec.type: LoadBalancer`.

- [ ] **Step 3: Run focused tests and verify RED**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./deploy/eks ./deploy/kubernetes -run 'EKS|Scenario' -count=1
```

Expected: FAIL because the scenarios and overlays do not exist.

- [ ] **Step 4: Add the scenario files and EKS overlays**

Use `configMapGenerator` only at runtime; do not commit a fake measured rate into
the Kubernetes stack. Keep every node selector and changed resource value in the
EKS overlays. The worker overlay composes only the shared EKS overlay. The HPA
overlay composes the shared EKS overlay, `target-hpa`, Metrics Server, and
kube-state-metrics, replaces the Prometheus config, and merges the existing
target-autoscaling dashboard.

- [ ] **Step 5: Run focused render verification**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./deploy/eks ./deploy/kubernetes -run 'EKS|Scenario' -count=1
kubectl kustomize deploy/kubernetes/overlays/eks-worker >/dev/null
kubectl kustomize deploy/kubernetes/overlays/eks-hpa >/dev/null
git diff --check -- deploy/eks deploy/kubernetes
```

Expected: PASS, with no AWS or cluster access.

- [ ] **Step 6: Review checkpoint**

Review both rendered stacks and exact scenario values. Do not stage or commit.

---

### Task 3: Implement Deterministic Evidence Redaction

**Files:**
- Create: `scripts/eks-redact_test.go`
- Create: `scripts/eks-redact.py`

**Interfaces:**
- Consumes: `--input <raw-run-dir>`, `--output <curated-dir>`, and an expected
  raw `metrics.json`, `report.json`, and `dashboard.png` contract from Task 6.
- Produces: sanitized `report.md`, `metrics.json`, and `dashboard.png`; exits
  nonzero before publishing when required fields are missing or sensitive values
  remain.

- [ ] **Step 1: Write failing redaction tests**

Create table-driven fixtures containing each sensitive category:

```text
123456789012
arn:aws:eks:us-west-2:123456789012:cluster/example
198.51.100.20
10.0.12.7
marcus@example.com
AKIAIOSFODNN7EXAMPLE
aws_secret_access_key
aws_session_token
```

Invoke the Python program with `exec.Command`. Require replacements such as
`<aws-account-id>`, `<aws-arn>`, `<ip-address>`, `<email-address>`, and
`<redacted-credential>`. Require recursive handling of JSON keys and values,
deterministic key ordering, preservation of numeric metrics, PNG byte equality,
and refusal to overwrite an existing curated directory.

Add schema assertions requiring `run_id`, `git_sha`, `region`, `topology`,
`worker_experiment`, `target_hpa_experiment`, `teardown`, and
`billing_attestation`. Reject nonzero dropped samples or unmet demand in the
published worker evidence object.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run EKSRedact -count=1
```

Expected: FAIL because `eks-redact.py` does not exist.

- [ ] **Step 3: Add the minimal standard-library redactor**

Use `argparse`, `json`, `pathlib`, `re`, `shutil`, and atomic temporary-directory
publication. Provide these testable functions:

```python
def redact_text(value: str) -> str
def redact_value(value: object) -> object
def validate_metrics(value: dict[str, object]) -> None
def publish_bundle(input_dir: Path, output_dir: Path) -> None
```

Generate Markdown from validated structured fields instead of copying arbitrary
raw Markdown. Scan the final Markdown and serialized JSON again before atomic
rename.

- [ ] **Step 4: Run focused verification**

Run:

```bash
python3 -m py_compile scripts/eks-redact.py
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run EKSRedact -count=1
git diff --check -- scripts/eks-redact.py scripts/eks-redact_test.go
```

Expected: PASS.

- [ ] **Step 5: Review checkpoint**

Review the adversarial fixtures and generated bundle. Do not stage or commit.

---

### Task 4: Define The Public Command And Offline Fake Harness

**Files:**
- Create: `scripts/eks-benchmark_test.go`
- Create: `scripts/eks-benchmark.sh`
- Create: `scripts/lib/eks-benchmark.sh`

**Interfaces:**
- Consumes: subcommands `preflight`, `ecr-push`, `render`, `run`, `teardown`,
  `verify-cleanup`, and `acknowledge-billing`.
- Produces: a strict dispatcher and sourceable library. Setting
  `EKS_BENCHMARK_LIBRARY_ONLY=1` sources functions without dispatching or
  installing traps.

- [ ] **Step 1: Build the fake-command test harness**

Define:

```go
type eksCommandResult struct {
	Output string
	Status int
	Calls  []string
	Root   string
}

func runEKSBenchmark(t *testing.T, subcommand string, env map[string]string,
	fixtures map[string]string) eksCommandResult
```

The helper creates executable fake `aws`, `eksctl`, `kubectl`, `kustomize`,
`docker`, `curl`, and `git` commands in a temporary `PATH`. Each records one
shell-quoted invocation per line and serves deterministic fixture output. It
sets isolated `HOME`, `XDG_*`, `ARTIFACT_ROOT`, and `KUBECONFIG` paths.

- [ ] **Step 2: Add failing syntax, dispatch, and preflight tests**

Require `bash -n` on both shell files and exact rejection of unknown commands.
For `preflight`, assert calls are limited to:

```text
aws sts get-caller-identity
aws configure get sso_session --profile load-tester-eks
aws ec2 describe-instance-type-offerings
aws eks describe-cluster
curl --fail https://checkip.amazonaws.com
git rev-parse HEAD
```

Require selection of two zones present in both instance-offering fixtures and
printing the account, region, cluster, zones, four-node topology, current `/32`,
and 90-minute deadline. Reject root identity, static root credentials, region
other than `us-west-2`, names not matching
`^load-tester-benchmark-[a-z0-9][a-z0-9-]{5,30}$`, an existing cluster, fewer
than two common zones, non-IPv4 public-IP responses, `0.0.0.0/0`, and any
`KEEP_CLUSTER` or `KEEP_EKS_CLUSTER` environment variable.

- [ ] **Step 3: Run focused tests and verify RED**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run 'EKSBenchmark.*(Syntax|Dispatch|Preflight)' -count=1
```

Expected: FAIL because the entrypoint and library do not exist.

- [ ] **Step 4: Implement the dispatcher and read-only preflight**

Use `set -euo pipefail`, arrays for command construction, quoted paths, and
short functions with these stable names:

```bash
require_command
require_common_environment
require_stage2_environment
load_caller_identity
select_availability_zones
discover_public_api_cidr
assert_cluster_absent
print_preflight_summary
```

`preflight` must never call an AWS create, update, put, delete, run, terminate,
or push operation. Missing `ALLOW_AWS_BILLING` does not block this read-only
command.

- [ ] **Step 5: Run focused verification**

Run:

```bash
bash -n scripts/eks-benchmark.sh scripts/lib/eks-benchmark.sh
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run 'EKSBenchmark.*(Syntax|Dispatch|Preflight)' -count=1
git diff --check -- scripts/eks-benchmark.sh scripts/lib/eks-benchmark.sh scripts/eks-benchmark_test.go
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Review the fake command log and prove preflight is read-only. Do not stage or
commit.

---

### Task 5: Add Safe Rendering And ECR Publication Contracts

**Files:**
- Modify: `scripts/eks-benchmark_test.go`
- Modify: `scripts/lib/eks-benchmark.sh`

**Interfaces:**
- Consumes: cluster template and overlays from Tasks 1–2, current Git SHA,
  account ID, region, selected zones, and current `/32`.
- Produces: raw-artifact copies of `cluster.yaml`, `eks-worker.yaml`, and
  `eks-hpa.yaml`; Stage 2 `ecr-push` publishes exactly three immutable image
  tags and lifecycle policies.

- [ ] **Step 1: Add failing render tests**

Require `render` to copy the Kubernetes tree to an isolated temporary directory,
use `kustomize edit set image` there, and write manifests only under the raw run
directory. Assert all three project images become:

```text
<account>.dkr.ecr.us-west-2.amazonaws.com/load-tester-<role>:sha-<full-sha>
```

Parse the output and reject placeholder images, mutable tags, Ingress,
LoadBalancer Services, missing selectors/resources, insecure Metrics Server
flags, unrestricted API CIDRs, or any AWS mutation. Require `render` to work
without `ALLOW_AWS_BILLING=1`.

- [ ] **Step 2: Add failing ECR tests**

Require `ecr-push` to fail before any mutation without
`ALLOW_AWS_BILLING=1`. With valid fixtures, require exactly three
`aws ecr describe-repositories` checks, only missing repositories created,
exactly three `put-lifecycle-policy` calls, one ECR login, and exactly three
`docker buildx build --push` operations using Dockerfile targets
`orchestrator`, `worker`, and `target` plus `VERSION` and `VCS_REF` build args.

The lifecycle fixture must expire untagged images after one day and retain only
three `sha-*` images. Reject a dirty Git tree so a tag always identifies the
exact source.

- [ ] **Step 3: Run focused tests and verify RED**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run 'EKSBenchmark.*(Render|ECR)' -count=1
```

Expected: FAIL because render and ECR helpers are absent.

- [ ] **Step 4: Implement rendering and ECR helpers**

Add:

```bash
render_cluster_config
render_kubernetes_overlay
assert_safe_rendered_manifests
ecr_repository_uri
ensure_ecr_repositories
apply_ecr_lifecycle_policy
push_project_images
```

Use arrays, temporary files, and atomic moves. Never edit a committed
Kustomization during image rewrite. `ecr-push` is implemented and fake-tested in
Stage 1 but must not be run against AWS until Stage 2.

- [ ] **Step 5: Run focused verification**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run 'EKSBenchmark.*(Render|ECR)' -count=1
bash -n scripts/eks-benchmark.sh scripts/lib/eks-benchmark.sh
git diff --check -- scripts
```

Expected: PASS using only fakes.

- [ ] **Step 6: Review checkpoint**

Inspect fake call order, rendered manifests, and lifecycle JSON. Do not stage or
commit.

---

### Task 6: Implement The Deadline, Experiments, And Raw Evidence Lifecycle

**Files:**
- Modify: `scripts/eks-benchmark_test.go`
- Modify: `scripts/lib/eks-benchmark.sh`
- Modify: `scripts/eks-benchmark.sh`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: rendered cluster/manifests, ECR image tags, Prometheus HTTP API,
  Kubernetes state, and the scenario contracts.
- Produces: `artifacts/eks/<UTC>-<short-sha>/` with owned-run sentinel,
  diagnostics, exact scenarios, structured results, and dashboard capture.

- [ ] **Step 1: Add failing ownership and deadline tests**

Require an atomic JSON sentinel with schema version, account, region, cluster,
run ID, Git SHA, zones, start epoch, and deadline epoch. Reject an existing
sentinel, expired deadline, mismatched identity, malformed JSON, or unsupported
schema. Assert the deadline is exactly 5,400 seconds after the recorded start
and is written before `eksctl create cluster`.

Add `.gitignore` contract coverage requiring `artifacts/eks/` while ensuring
`docs/benchmarks/eks/` is not ignored.

- [ ] **Step 2: Add failing success and failure-order tests**

For a successful fake run, require this order:

```text
preflight -> render -> sentinel -> eksctl create -> readiness ->
worker calibration -> worker evidence -> target baseline -> target HPA evidence ->
collect -> eksctl delete --wait -> verify cleanup
```

For simulated failures at cluster creation, readiness, each experiment, evidence
collection, interruption, and deadline, require diagnostics before deletion when
a cluster exists. Require no keep-cluster branch.

- [ ] **Step 3: Add failing experiment tests**

Worker calibration starts at 100 RPS with one worker and may increase only from
clean results. The evidence rate must be in the approved 1.25-to-1.5 multiple of
the last stable measured rate. Require 1-to-4 autoscaling, recovered scheduler
coverage of at least 90 percent, zero `/echo` errors, zero dropped samples, and
zero unmet demand before marking evidence publishable.

Target calibration starts at 30 RPS with two fixed workers and increases by
exactly 50 percent per attempt. Require 1-to-5 target HPA, at least three observed
replicas, no terminal pin at five, and measured p95 recovery. Every generated
scenario is copied into the raw run directory before use.

Tests must reject publication when `max_in_flight` is reported as measured
concurrency or when either experiment's metrics are missing p50/p95/p99,
throughput, duration, errors, dropped samples, or unmet demand.

- [ ] **Step 4: Run focused tests and verify RED**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run 'EKSBenchmark.*(Ownership|Deadline|Run|Experiment|Artifacts)' -count=1
```

Expected: FAIL until lifecycle and experiment helpers exist.

- [ ] **Step 5: Implement lifecycle and experiment helpers**

Add these stable functions:

```bash
write_owned_run_state
load_owned_run_state
deadline_remaining
create_owned_cluster
wait_for_platform_readiness
install_scenario_config
run_orchestrator_job
query_prometheus
run_worker_experiment
run_hpa_experiment
collect_diagnostics
collect_raw_evidence
capture_dashboard
```

Use a single `EXIT` trap installed only by `run`. Track whether cluster creation
was attempted and whether ownership was confirmed. A benchmark success is
provisional until cleanup verification passes.

- [ ] **Step 6: Run focused verification**

Run:

```bash
bash -n scripts/eks-benchmark.sh scripts/lib/eks-benchmark.sh
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run 'EKSBenchmark.*(Ownership|Deadline|Run|Experiment|Artifacts)' -count=1
git check-ignore artifacts/eks/example/raw.log
if git check-ignore docs/benchmarks/eks/example/metrics.json; then exit 1; fi
git diff --check -- .gitignore scripts
```

Expected: PASS using only fake commands and temporary directories.

- [ ] **Step 7: Review checkpoint**

Review the full fake success and failure call traces plus raw fixture bundle. Do
not stage or commit.

---

### Task 7: Enforce Mandatory Teardown And Cleanup Verification

**Files:**
- Modify: `scripts/eks-benchmark_test.go`
- Modify: `scripts/lib/eks-benchmark.sh`
- Modify: `scripts/eks-benchmark.sh`

**Interfaces:**
- Consumes: the valid owned-run sentinel from Task 6 and read-only AWS inventory
  fixtures.
- Produces: sentinel-protected cluster deletion, a cleanup report, and a dated
  human billing attestation.

- [ ] **Step 1: Add failing teardown authorization tests**

Require `teardown` to work without `ALLOW_AWS_BILLING`, but only when sentinel
account, region, cluster, and current caller identity match. Reject an arbitrary
`EKS_CLUSTER_NAME`, missing sentinel, wrong account, wrong region, stale schema,
or cluster-name mismatch without invoking deletion.

For valid ownership, require Kubernetes exposed-resource deletion first, then:

```text
eksctl delete cluster --name <owned-name> --region us-west-2 --wait
```

There must be no Terraform, keep-alive, `--disable-eviction`, or unbounded retry
path.

- [ ] **Step 2: Add failing cleanup-inventory tests**

Fake independent checks for EKS cluster/node groups, EC2 instances, Auto Scaling
Groups, CloudFormation stacks, unattached or tagged EBS volumes, load balancers,
NAT gateways, and Elastic IPs. Each nonempty fixture must independently make
`verify-cleanup` fail and name the resource category. ECR repositories are
reported as lifecycle-managed and do not make cluster cleanup fail.

Require cleanup failure to override an otherwise successful benchmark. Require
the cleanup report to contain exact identity, region, cluster, check time, every
resource category, and final PASS/FAIL without credentials.

- [ ] **Step 3: Add failing billing-attestation tests**

Require `acknowledge-billing --checked-on YYYY-MM-DD` to accept only a valid date
not before the run date and not in the future. It must update only a successfully
cleaned raw run, preserve prior evidence, record that the value is a human
attestation rather than an API-verified cost, and then invoke `eks-redact.py` to
publish the immutable curated bundle. Missing cleanup PASS must fail, and a
second publication attempt must not overwrite the first bundle.

- [ ] **Step 4: Run focused tests and verify RED**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run 'EKSBenchmark.*(Teardown|Cleanup|Billing)' -count=1
```

Expected: FAIL until teardown, inventory, and attestation helpers exist.

- [ ] **Step 5: Implement teardown and verification helpers**

Add:

```bash
assert_owned_cluster
delete_exposed_kubernetes_resources
delete_owned_cluster
inventory_remaining_resources
write_cleanup_report
acknowledge_billing_check
```

Keep inventory filters scoped to the owned cluster name and tags. Preserve raw
diagnostics when AWS deletion or verification fails so emergency commands can be
rerun from another terminal.

- [ ] **Step 6: Run focused verification**

Run:

```bash
bash -n scripts/eks-benchmark.sh scripts/lib/eks-benchmark.sh
GOCACHE=/tmp/load-tester-go-cache go test ./scripts -run 'EKSBenchmark.*(Teardown|Cleanup|Billing)' -count=1
git diff --check -- scripts
```

Expected: PASS with fake commands only.

- [ ] **Step 7: Review checkpoint**

Review every deletion refusal and leftover-resource fixture. Do not stage or
commit.

---

### Task 8: Document Stage 1 And Run The Complete Offline Gate

**Files:**
- Create: `docs/eks-benchmark.md`
- Modify: `docs/design.md`
- Modify: `docs/roadmap.md`
- Modify only if integration exposes a contract defect: files from Tasks 1–7

**Interfaces:**
- Consumes: the reviewed command and safety behavior from Tasks 1–7.
- Produces: a concise operator guide and final Stage 1 evidence without claiming
  an EKS run occurred.

- [ ] **Step 1: Write the operator guide**

Document manual account ownership, root MFA, IAM Identity Center, profile
`load-tester-eks`, `$10` actual/forecast Budget alerts, Cost Anomaly Detection,
and the fact that alerts are delayed rather than hard caps. Explicitly forbid
sharing passwords, access keys, MFA codes, recovery codes, or payment details.

Show the reviewed sequence:

```bash
./scripts/eks-benchmark.sh preflight
./scripts/eks-benchmark.sh ecr-push
./scripts/eks-benchmark.sh render
./scripts/eks-benchmark.sh run
./scripts/eks-benchmark.sh verify-cleanup
./scripts/eks-benchmark.sh acknowledge-billing --checked-on YYYY-MM-DD
```

Label `ecr-push` and `run` as Stage 2 billable operations. Include the
sentinel-protected emergency `teardown` and `verify-cleanup` commands for laptop
or network failure. Keep setup prose short and link authoritative AWS docs rather
than reproducing AWS console walkthroughs.

- [ ] **Step 2: Update architecture and roadmap documentation**

Add a concise Piece 23 section to `docs/design.md` covering fixed node pools,
pod placement, separate control-loop experiments, identity and cost boundary,
mandatory teardown, and raw-versus-sanitized evidence. Update Piece 23 in
`docs/roadmap.md` to mark Stage 1 as the automation milestone and Stage 2 as the
measured EKS run plus next-day billing check. Do not mark Piece 23 or Piece 24B
complete and do not add estimated results.

- [ ] **Step 3: Run the complete offline verification gate**

Run:

```bash
GOCACHE=/tmp/load-tester-go-cache go test ./...
python3 -m py_compile scripts/eks-redact.py
bash -n scripts/eks-benchmark.sh scripts/lib/eks-benchmark.sh
kubectl kustomize deploy/kubernetes/overlays/eks-worker >/dev/null
kubectl kustomize deploy/kubernetes/overlays/eks-hpa >/dev/null
git diff --check
```

Expected: every command PASS. Do not run `ecr-push`, `run`, or any real AWS
mutation command.

- [ ] **Step 4: Inspect repository scope and secrets**

Run:

```bash
git status --short
git diff --name-only
rg -n 'AKIA[0-9A-Z]{16}|aws_secret_access_key|aws_session_token|BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY' \
  deploy/eks deploy/kubernetes/overlays/eks* scripts docs .gitignore
```

Expected: only planned Stage 1 files changed, and the credential scan returns no
matches except intentional redaction-test fixtures. Verify those fixtures use
AWS documentation example values and are never treated as live credentials.

- [ ] **Step 5: Independent review gates**

Request a focused infrastructure/security review of the full base-to-working-tree
diff, including untracked files. Require review of AWS mutation boundaries,
sentinel authorization, shell quoting, cleanup completeness, manifest placement,
redaction bypasses, and benchmark-claim accuracy. Resolve material findings and
rerun the smallest affected focused test plus the complete offline gate.

- [ ] **Step 6: Present Stage 1 for Marcus's review**

Report exact files and line references for the entrypoint, safety guards,
cluster topology, EKS overlays, experiment logic, teardown, redaction, tests,
and documentation. State explicitly that no AWS resources were touched and no
cloud benchmark has been run. Do not stage, commit, or push until Marcus gives
explicit approval.
