# Piece 23 Stage 1 EKS Benchmark Design

**Date:** 2026-08-28
**Status:** Ready for review

## Purpose

Build a guarded, reproducible workflow for one short AWS EKS validation session.
Stage 1 adds configuration, Kubernetes overlays, automation, redaction,
documentation, and offline tests. It must not create, modify, or delete AWS
resources.

Stage 2 begins only after Marcus reviews Stage 1, configures the AWS account and
SSO profile, and explicitly accepts billing. Stage 2 runs the workflow, records
measured results, destroys the infrastructure, and completes a next-day billing
check. Piece 23 remains incomplete until Stage 2 is finished.

## Provisioning And Images

Use `eksctl`, not Terraform, to create a disposable cluster in `us-west-2`.
Terraform remains a stretch goal. Before rendering the cluster configuration, a
read-only EC2 offering check selects two availability zones where both approved
instance types are offered.

Publish three private ECR images:

- `load-tester-orchestrator`
- `load-tester-worker`
- `load-tester-target`

Tag each image `sha-<full-git-commit>`. EKS pulls through the managed-node IAM
role; Kubernetes receives no registry password. Each repository gets a lifecycle
policy that expires untagged images after one day and retains only the three most
recent `sha-*` images. ECR repositories are account-scoped resources and are not
owned by cluster teardown; cleanup verification reports them separately. Existing
repositories are mutated only after they prove the workflow ownership tag and
immutable image tags; all other lookup errors fail closed.

## Fixed Cluster Topology

Use x86_64 On-Demand managed node groups with node autoscaling disabled:

| Pool | Nodes | Instance | Label |
|---|---:|---|---|
| Load generation | 2 fixed | `c7i.large` | `load-tester.io/pool=loadgen` |
| Platform and target | 2 fixed | `m7i.large` | `load-tester.io/pool=platform` |

Each group has `minSize`, `desiredCapacity`, and `maxSize` set to `2`. The
orchestrator still scales worker pods, and the target HPA still scales target
pods. Piece 23 does not add an EC2 node autoscaler.

Create a dedicated VPC spanning the selected availability zones. Nodes use
private subnets and one NAT gateway. Do not create SSH access, a Kubernetes
`LoadBalancer` Service, an Ingress, or another public application endpoint.
Prometheus and Grafana remain `ClusterIP` Services reached through
`kubectl port-forward`. The Kubernetes API endpoint may be public only for the
operator's current public IPv4 address as a `/32` CIDR.

## Kubernetes Placement And Resources

Add placement only in the EKS overlays so local kind behavior remains unchanged.
Workers select `load-tester.io/pool=loadgen`. The target, orchestrator,
Prometheus, Grafana, Metrics Server, and kube-state-metrics select
`load-tester.io/pool=platform`.

Use these initial requests and limits:

| Workload | Request | Limit |
|---|---|---|
| Worker | `500m`, `512Mi` | `1`, `1Gi` |
| Target | `250m`, `256Mi` | `500m`, `512Mi` |
| Orchestrator | `500m`, `512Mi` | `1`, `1Gi` |
| Prometheus | `500m`, `1Gi` | `1`, `2Gi` |
| Grafana | `250m`, `256Mi` | `500m`, `512Mi` |
| kube-state-metrics | `10m`, `32Mi` | `100m`, `64Mi` |

Metrics Server keeps its upstream secure kubelet settings on EKS; the kind-only
`--kubelet-insecure-tls` patch must not appear. The existing namespace RBAC and
single-run orchestrator Job remain unchanged.

## Benchmark Method

Use the open `constant-rate` model. Do not choose a resume number before the
run, and do not describe `max_in_flight` as achieved concurrency.

Run two isolated experiments:

1. **Worker-fleet experiment.** Keep target HPA disabled and use `/echo`.
   Establish a one-worker stable rate, then challenge the orchestrator's
   1-to-4 worker autoscaler at 1.25 to 1.5 times that measured rate. The evidence
   run must recover scheduler coverage without dropped samples, unmet demand, or
   request errors.
2. **Target-HPA experiment.** Fix the worker fleet at two and use
   `/burn?ms=20` with `Connection: close`. Start at 30 RPS and increase in
   50-percent steps when needed. The evidence run must reach at least three
   target replicas, must not remain pinned at five, and must show latency
   recovery after scale-up.

Calibration attempts are diagnostic and are stored with the run. Only a clean
evidence run supplies README or resume figures. Stop the current experiment and
collect diagnostics when dropped samples or unmet demand become nonzero, normal
`/echo` requests report an error, worker registration or infrastructure fails,
four workers cannot restore coverage, the HPA remains pinned at five, or the
session deadline is reached.

Publish only measured peak sustained RPS, peak worker count, target replica
count, p50/p95/p99 latency, error rate, dropped samples, unmet demand, duration,
topology, and cost. The former 75,000-concurrent-request idea is not an acceptance
target.

## Identity, Cost, And Human Boundary

Marcus owns the AWS account, payment information, root-user MFA, IAM Identity
Center setup, SSO/MFA prompts, and billing acceptance. Credentials, MFA codes,
recovery codes, and payment details never enter the repository or automation.

Use a dedicated temporary-credential AWS CLI profile named `load-tester-eks`.
Before Stage 2, Marcus manually creates a `$10` monthly AWS Budget with actual
and forecast alerts and enables Cost Anomaly Detection. These are delayed alerts,
not hard caps. The workflow's enforceable cost boundary is a 90-minute deadline
starting before cluster creation plus mandatory teardown.

Every mutating command except emergency teardown requires:

```text
AWS_PROFILE=load-tester-eks
AWS_REGION=us-west-2
EKS_CLUSTER_NAME=load-tester-benchmark-<unique-suffix>
ALLOW_AWS_BILLING=1
```

Preflight rejects root identity, missing temporary SSO credentials, another
region, malformed or existing cluster names, unavailable instance offerings,
unrestricted API CIDRs, node autoscaling, and rendered LoadBalancer or Ingress
resources. It prints account, region, cluster name, selected zones, topology,
deadline, and current public IP before any mutation.

## Command Interface

Expose one entrypoint:

```text
./scripts/eks-benchmark.sh preflight
./scripts/eks-benchmark.sh ecr-push
./scripts/eks-benchmark.sh render
./scripts/eks-benchmark.sh run
./scripts/eks-benchmark.sh teardown
./scripts/eks-benchmark.sh verify-cleanup
./scripts/eks-benchmark.sh acknowledge-billing --checked-on YYYY-MM-DD
```

`preflight`, `render`, and all Stage 1 tests are read-only with respect to AWS.
`ecr-push` and `run` are Stage 2 operations and require billing acknowledgement.
`teardown` and `verify-cleanup` remain available without
`ALLOW_AWS_BILLING=1` so an emergency cleanup is never blocked.

Use `scripts/eks-benchmark.sh` as the small dispatcher,
`scripts/lib/eks-benchmark.sh` for strict-mode guards and lifecycle helpers, and
`scripts/eks-redact.py` only for structured sanitization. Add no Makefile and no
new Go production package.

## Ownership And Teardown

Before cluster creation, write a local owned-run sentinel under the raw artifact
directory. It records the exact account, region, generated cluster name, start
time, deadline, Git commit, and selected zones. Creation must fail if an existing
cluster or sentinel would be reused. Teardown refuses to delete a cluster when
the current identity, region, and name do not match a valid sentinel. The
rendered cluster also carries `load-tester.io/run-id` and
`load-tester.io/workflow-identity=eks-benchmark` tags; teardown verifies both
cloud tags before deleting the cluster. If a failed create leaves no live EKS API
object, the exact `eksctl-<cluster>-cluster` CloudFormation stack must prove the
same tags before cleanup or recovery can delete it.

`run` has no keep-cluster option. On success, failure, interruption, or deadline:

1. Stop scheduling new work.
2. Collect Kubernetes state, events, workload logs, Prometheus results, and AWS
   inventory diagnostics.
3. Delete exposed Kubernetes resources, if any.
4. Run `eksctl delete cluster --wait`.
5. Independently verify EKS, managed node groups, EC2 instances, Auto Scaling
   Groups, CloudFormation stacks, EBS volumes, load balancers, NAT gateways, and
   Elastic IPs are gone.

Cleanup failure overrides benchmark success. A local trap cannot survive laptop
power or network loss, so the documentation includes sentinel-protected emergency
`teardown` and `verify-cleanup` commands. The next-day Billing Console review is
a human attestation recorded with `acknowledge-billing`; the script cannot claim
to verify what Marcus saw.

Prometheus and Grafana port-forwards run in isolated process groups. The workflow
stops and reaps them before reporting cleanup success, including after an
interruption or deadline.

## Evidence And Redaction

Store full run data locally under:

```text
artifacts/eks/<run-id>/
```

The directory is gitignored and may contain raw logs, account IDs, ARNs, resource
names, and IP addresses. A run ID combines UTC time and the short Git SHA.

Only sanitized measured evidence may be copied to:

```text
docs/benchmarks/eks/<run-id>/report.md
docs/benchmarks/eks/<run-id>/metrics.json
docs/benchmarks/eks/<run-id>/dashboard.png
```

Redaction removes AWS account IDs, ARNs, public and private IP addresses, email
addresses, access-key-like values, session tokens, and credential fields. The
report records Git SHA, redacted ECR tags, Kubernetes/EKS versions, region,
instance types, node counts, exact scenarios, durations, measured worker and HPA
results, limitations, teardown verification, and billing attestation. The JSON
is the machine-readable source for Piece 24B README contract tests.

`run` writes only raw evidence. After cleanup passes and Marcus completes the
next-day billing review, `acknowledge-billing` records the human attestation and
invokes the redactor to publish the immutable curated bundle. The redactor binds
the input directory, raw ownership state, metrics, and cleanup identity to the
same run, Git SHA, and region, and rejects nonzero error rate, dropped samples,
or unmet demand.

Grafana screenshots are captured manually. A local `dashboard-review.json`
sidecar records human approval, reviewer, date, and the SHA-256 of the exact PNG;
the redactor requires that sidecar before publishing the dashboard and does not
copy the sidecar into the curated bundle.

No sanitized result directory is created during Stage 1 because no EKS
measurement exists yet.

## Offline Verification

Go tests place deterministic fake `aws`, `eksctl`, `kubectl`, `kustomize`,
`docker`, `curl`, and `git` commands first on a temporary `PATH`. The fakes record
arguments and return fixtures. Tests require:

- read-only preflight and render behavior;
- mutation blocked by missing acknowledgement, root identity, malformed names,
  existing clusters, or unsafe manifests;
- three SHA-tagged ECR pushes and lifecycle policies;
- correct fixed topology, dynamic zones, API CIDR, placement, resources, and
  ECR image rewrites;
- diagnostics before mandatory `eksctl delete cluster --wait` after both success
  and simulated failure;
- sentinel-protected teardown and leftover-resource detection;
- complete redaction and a schema-valid sanitized evidence bundle;
- valid Bash syntax and Kustomize rendering.

Tests may not require AWS credentials, network access, a Docker daemon, or a
Kubernetes cluster. Stage 1 verification runs only local static, unit, and
fake-command acceptance tests.

## Documentation Updates In Stage 1

Update `docs/design.md` with the approved EKS architecture, safety boundary,
benchmark isolation, and evidence policy. Refine Piece 23 in `docs/roadmap.md`
to distinguish Stage 1 automation from Stage 2 measured execution without
marking Piece 23 complete. Add `docs/eks-benchmark.md` for account prerequisites,
the reviewed command sequence, emergency cleanup, and the next-day billing
check.

## Out Of Scope

- Creating or mutating any AWS resource during Stage 1.
- Running the EKS benchmark or publishing estimated cloud results.
- Terraform, Helm, CI/CD, node autoscaling, multi-region or high-availability
  benchmarking, public dashboards, gRPC target load, or orchestrator HA.
- Changing scheduler, fleet-driver, histogram, or load-generation semantics.
- Storing AWS credentials, account identifiers, or unsanitized run artifacts in
  Git.
