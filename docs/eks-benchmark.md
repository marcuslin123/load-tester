# EKS Benchmark Runbook

This guide runs the one disposable AWS validation session for Piece 23. Stage 1
creates and tests the workflow locally. Stage 2 is still pending: it creates AWS
resources, records measured results, then removes them.

## Account Boundary

You own the AWS account, payment method, root MFA, IAM Identity Center setup,
and interactive SSO/MFA prompts. Keep the root user for account-only work; do
not create root access keys. Use an IAM Identity Center profile named
`load-tester-eks` for the benchmark. Do not share or commit passwords, access
keys, MFA codes, recovery codes, payment details, credentials, or account IDs.

Before Stage 2, create a `$10` monthly AWS Budget with actual and forecast
alerts, and enable Cost Anomaly Detection. Both are delayed notifications, not
hard caps. The practical limit is the workflow's 90-minute deadline and its
mandatory teardown.

See [root-user best practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/root-user-best-practices.html),
[AWS CLI authentication](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html),
[AWS Budgets](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-best-practices.html),
and [Cost Anomaly Detection](https://docs.aws.amazon.com/cost-management/latest/userguide/manage-ad.html).

## Prerequisites

Install and authenticate `aws`, `eksctl`, `kubectl`, Docker with Buildx,
Kustomize, Git, Bash, and Python 3. Complete SSO for the dedicated profile, then
set these values in the terminal that will run the benchmark:

```bash
export AWS_PROFILE=load-tester-eks
export AWS_REGION=us-west-2
export EKS_CLUSTER_NAME=load-tester-benchmark-<unique-suffix>
export ALLOW_AWS_BILLING=1
```

Use a unique lower-case suffix. The workflow rejects static AWS credentials,
root identities, malformed names, existing clusters, unsafe API CIDRs, and
keep-cluster flags.

## Topology

The disposable cluster has two fixed `c7i.large` load-generator nodes and two
fixed `m7i.large` platform/target nodes. It uses private nodes across two
selected availability zones, with one NAT gateway. There is no node autoscaler,
SSH, Ingress, or `LoadBalancer` Service. Prometheus and Grafana use local
`kubectl port-forward`; the Kubernetes API is restricted to the operator's
current public IPv4 `/32`.

## Commands

`preflight` and `render` are read-only AWS checks. `ecr-push` and `run` are
Stage 2 billable operations and require `ALLOW_AWS_BILLING=1`.

```bash
./scripts/eks-benchmark.sh preflight
./scripts/eks-benchmark.sh render
./scripts/eks-benchmark.sh ecr-push
./scripts/eks-benchmark.sh run
./scripts/eks-benchmark.sh verify-cleanup
./scripts/eks-benchmark.sh acknowledge-billing --checked-on YYYY-MM-DD
```

`run` performs a readiness check, a one-worker calibration, worker-fleet
autoscaling against `/echo`, then an isolated target-HPA experiment against
`/burn?ms=20`. It uses the open `constant-rate` model. Rates are calibrated from
the measured run rather than selected for a resume claim. A clean evidence run
has no request errors or dropped samples. The worker experiment reports missed
arrivals before recovery and requires zero unmet demand after recovery.
Calibration and the target HPA experiment require zero unmet demand.

## Dashboard Approval

During `run`, Grafana is available through a local port-forward. Capture the
dashboard manually and inspect it for account IDs, IPs, and sensitive details.
Set `EKS_DASHBOARD_PNG` to that screenshot and `EKS_DASHBOARD_REVIEW` to a local
`dashboard-review.json` sidecar. The sidecar must state `approved: true`, name
the reviewer and date, and contain the SHA-256 of the exact PNG. This binds human
approval to the screenshot bytes; it is not committed.

## Evidence And Publication

Raw logs, resource snapshots, screenshots, and identifiers remain gitignored in:

```text
artifacts/eks/<run-id>/
```

Only the redacted, measured bundle may be published here after cleanup and the
next-day billing check:

```text
docs/benchmarks/eks/<run-id>/
```

The redactor removes account IDs, ARNs, IPs, emails, and credential-like data.
`acknowledge-billing` records your human billing attestation exactly one calendar
day after the run and publishes the curated bundle. It rejects any nonzero error
rate, dropped sample, post-recovery unmet demand, or evidence whose run ID, Git SHA, region, or
cleanup identity does not exactly match the owned raw run. Do not update the
README or Piece 24B until that measured bundle exists.

## Teardown

There is no keep-cluster option. On success, failure, interruption, or the
deadline, the workflow collects diagnostics first, then deletes the EKS cluster
with `eksctl delete cluster --wait` and verifies cleanup. Cleanup fails closed:
an AWS query error or any remaining resource is a failure, not a pass.

The local sentinel and matching cloud ownership tags protect deletion. Emergency
commands do not require the billing acknowledgement, but `teardown` and
`verify-cleanup` require the local owned-run state:

```bash
./scripts/eks-benchmark.sh teardown
./scripts/eks-benchmark.sh verify-cleanup
```

After teardown, verify EKS, managed node groups, EC2 instances, Auto Scaling
Groups, CloudFormation stacks, EBS volumes, load balancers, NAT gateways, and
Elastic IPs. ECR images are lifecycle-managed separately. See [eksctl cluster
lifecycle](https://docs.aws.amazon.com/eks/latest/eksctl/creating-and-managing-clusters.html)
and [EKS deletion guidance](https://docs.aws.amazon.com/eks/latest/userguide/delete-cluster.html).

A laptop or network loss can prevent local traps from running. From another
authenticated terminal, set the original `AWS_PROFILE`, `AWS_REGION`, and exact
`EKS_CLUSTER_NAME`, then explicitly authorize recovery:

```bash
./scripts/eks-benchmark.sh recover-teardown --acknowledge-recovery
```

This command does not need the old sentinel or billing acknowledgement. It
authenticates the caller, rejects root identities and unsafe names, and refuses
deletion unless the live cluster or the exact `eksctl-<cluster>-cluster`
CloudFormation stack has matching `load-tester.io/workflow-identity=eks-benchmark`
and run-ID ownership tags. It writes a recovered local ownership record, then performs the same tag-protected
teardown and cleanup verification. It cannot recover or delete anything while
AWS is unreachable; restore AWS connectivity or use another authenticated
machine before resources can be verified or removed.
