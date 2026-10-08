#!/usr/bin/env bash
# Guarded helpers for the disposable EKS benchmark. This file performs no work
# when sourced; the public dispatcher chooses the lifecycle command.
set -euo pipefail

LIB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$LIB_DIR/../.." && pwd)
ARTIFACT_ROOT=${ARTIFACT_ROOT:-"$REPO_ROOT/artifacts/eks"}
NAMESPACE=${EKS_NAMESPACE:-load-tester}
EKS_REDACTOR=${EKS_REDACTOR:-"$REPO_ROOT/scripts/eks-redact.py"}
RUN_DIR=${EKS_RUN_DIR:-}
ACCOUNT_ID=''
CALLER_ARN=''
GIT_SHA=''
PUBLIC_API_CIDR=''
AZ_A=''
AZ_B=''
CLUSTER_CONFIG=''
CLUSTER_CREATE_ATTEMPTED=0
CLUSTER_DELETE_ATTEMPTED=0
RUN_FINALIZED=0
CURRENT_STACK=''
PROMETHEUS_PORT=''
GRAFANA_PORT=''
PROMETHEUS_PORT_FORWARD_PID=''
GRAFANA_PORT_FORWARD_PID=''
DEADLINE_WATCHDOG_PID=''
ACTIVE_COMMAND_PID=''
ACTIVE_COMMAND_CHILD_PID=''
ACTIVE_COMMAND_TIMER_PID=''
ACTIVE_COMMAND_TIMEOUT_FILE=''
CLEANUP_COMMAND_FAILED=0
EXIT_CLEANUP_RUNNING=0
ECR_PUSH_CLEANUP_RUNNING=0
WORKER_PEAK_RPS=''
WORKER_P50_MS=''
WORKER_P95_MS=''
WORKER_P99_MS=''
WORKER_PEAK_WORKERS=''
WORKER_ERROR_RATE=''
WORKER_DROPPED_SAMPLES=''
WORKER_UNMET_DEMAND=''
HPA_PEAK_RPS=''
HPA_P50_MS=''
HPA_P95_MS=''
HPA_P99_MS=''
HPA_PEAK_WORKERS=''
HPA_TARGET_REPLICAS=''
HPA_ERROR_RATE=''
HPA_DROPPED_SAMPLES=''
HPA_UNMET_DEMAND=''
KUBERNETES_VERSION=''
EKS_VERSION=''
LOADGEN_NODE_COUNT=''
PLATFORM_NODE_COUNT=''

die() {
  printf '%s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

reject_keep_alive_flags() {
  [[ -z "${KEEP_CLUSTER:-}" ]] || die "keep-cluster flags are not supported"
  [[ -z "${KEEP_EKS_CLUSTER:-}" ]] || die "keep-cluster flags are not supported"
}

require_common_environment() {
  reject_keep_alive_flags
  [[ "${AWS_PROFILE:-}" == "load-tester-eks" ]] || die "AWS_PROFILE must be load-tester-eks"
  [[ "${AWS_REGION:-}" == "us-west-2" ]] || die "AWS_REGION must be us-west-2"
  [[ "${EKS_CLUSTER_NAME:-}" =~ ^load-tester-benchmark-[a-z0-9][a-z0-9-]{5,30}$ ]] || die "invalid EKS_CLUSTER_NAME"
  [[ -z "${AWS_ACCESS_KEY_ID:-}" && -z "${AWS_SECRET_ACCESS_KEY:-}" ]] || die "static credentials are not supported; use AWS SSO"
  require_command aws
  require_command curl
  require_command git
  require_command kubectl
  require_command kustomize
  require_command eksctl
}

require_stage2_environment() {
  require_common_environment
  [[ "${ALLOW_AWS_BILLING:-}" == "1" ]] || die "ALLOW_AWS_BILLING=1 is required for Stage 2 mutation"
}

load_caller_identity() {
  local identity sso_session
  if [[ -n "$RUN_DIR" ]]; then
    identity=$(run_cleanup_command aws sts get-caller-identity --output json)
  else
    identity=$(run_external_command aws sts get-caller-identity --output json)
  fi
  ACCOUNT_ID=$(printf '%s' "$identity" | sed -nE 's/.*"Account"[[:space:]]*:[[:space:]]*"([0-9]{12})".*/\1/p')
  CALLER_ARN=$(printf '%s' "$identity" | sed -nE 's/.*"Arn"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p')
  [[ "$ACCOUNT_ID" =~ ^[0-9]{12}$ && -n "$CALLER_ARN" ]] || die "unable to parse AWS caller identity"
  [[ "$CALLER_ARN" != *":root" ]] || die "root identity is not allowed"
  if [[ -n "$RUN_DIR" ]]; then
    sso_session=$(run_cleanup_command aws configure get sso_session --profile "$AWS_PROFILE" 2>/dev/null || true)
  else
    sso_session=$(run_external_command aws configure get sso_session --profile "$AWS_PROFILE" 2>/dev/null || true)
  fi
  [[ -n "$sso_session" ]] || die "AWS profile must use an IAM Identity Center SSO session"
}

select_availability_zones() {
  local c7i m7i common=() zone
  c7i=$(run_external_command aws ec2 describe-instance-type-offerings --region "$AWS_REGION" --location-type availability-zone --filters Name=instance-type,Values=c7i.large --query 'InstanceTypeOfferings[].Location' --output text)
  m7i=$(run_external_command aws ec2 describe-instance-type-offerings --region "$AWS_REGION" --location-type availability-zone --filters Name=instance-type,Values=m7i.large --query 'InstanceTypeOfferings[].Location' --output text)
  for zone in $(printf '%s\n%s\n' "$c7i" "$m7i" | tr ',\t' '  ' | sort -u); do
    [[ " $c7i " == *"$zone"* && " $m7i " == *"$zone"* ]] && common+=("$zone")
  done
  [[ ${#common[@]} -ge 2 ]] || die "fewer than two common availability zones offer c7i.large and m7i.large"
  AZ_A=${common[0]}
  AZ_B=${common[1]}
  [[ "$AZ_A" != "$AZ_B" ]] || die "availability zones must be distinct"
}

discover_public_api_cidr() {
  local ip
  ip=$(run_external_command curl --fail --connect-timeout 5 --max-time 20 https://checkip.amazonaws.com)
  ip=${ip//$'\n'/}
  python3 - "$ip" <<'PY' || die "current public IPv4 address is invalid"
import ipaddress
import sys

try:
    address = ipaddress.ip_address(sys.argv[1])
except ValueError:
    raise SystemExit(1)
if address.version != 4 or any((
    address.is_private,
    address.is_loopback,
    address.is_link_local,
    address.is_multicast,
    address.is_unspecified,
)):
    raise SystemExit(1)
PY
  PUBLIC_API_CIDR="$ip/32"
  [[ "$PUBLIC_API_CIDR" != "0.0.0.0/0" ]] || die "unrestricted API CIDR is not allowed"
}

assert_cluster_absent() {
  local output
  if output=$(run_external_command aws eks describe-cluster --name "$EKS_CLUSTER_NAME" --region "$AWS_REGION" 2>&1); then
    die "EKS cluster already exists: $EKS_CLUSTER_NAME"
  fi
  [[ "$output" == *ResourceNotFoundException* ]] || die "unable to determine whether EKS cluster exists: $output"
}

load_git_sha() {
  GIT_SHA=$(run_external_command git rev-parse HEAD)
  [[ "$GIT_SHA" =~ ^[0-9a-f]{40}$ ]] || die "git rev-parse HEAD did not return a full commit SHA"
}

print_preflight_summary() {
  cat <<EOF
Account: $ACCOUNT_ID
Region: $AWS_REGION
Cluster: $EKS_CLUSTER_NAME
Zones: $AZ_A, $AZ_B
Topology: 2 c7i.large load-generation nodes; 2 m7i.large platform nodes
API CIDR: $PUBLIC_API_CIDR
Deadline: 90 minutes
EOF
}

validate_eksctl_template() {
  local temporary
  temporary=$(mktemp)
  render_cluster_config "$temporary"
  # eksctl validates the rendered ClusterConfig without creating AWS resources.
  run_external_command eksctl create cluster --config-file "$temporary" --dry-run >/dev/null
  rm -f "$temporary"
}

preflight() {
  require_common_environment
  load_caller_identity
  select_availability_zones
  discover_public_api_cidr
  load_git_sha
  assert_cluster_absent
  validate_eksctl_template
  print_preflight_summary
}

create_run_directory() {
  local run_id now short_sha
  now=$(date -u +%Y%m%dT%H%M%SZ)
  short_sha=${GIT_SHA:0:7}
  run_id="$now-$short_sha"
  RUN_DIR="$ARTIFACT_ROOT/$run_id"
  [[ ! -e "$RUN_DIR" ]] || die "raw artifact directory already exists: $RUN_DIR"
  mkdir -p "$RUN_DIR/scenarios"
}

render_cluster_config() {
  local output=$1 template run_id workflow_identity
  template="$REPO_ROOT/deploy/eks/cluster.yaml.tmpl"
  [[ -f "$template" ]] || die "cluster template is missing: $template"
  if [[ -n "$RUN_DIR" ]]; then
    run_id=$(basename "$RUN_DIR")
  else
    run_id="preflight-$EKS_CLUSTER_NAME"
  fi
  workflow_identity='eks-benchmark'
  [[ "$run_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$ ]] || die "unsafe EKS ownership run ID"
  sed \
    -e "s|__CLUSTER_NAME__|$EKS_CLUSTER_NAME|g" \
    -e "s|__AZ_A__|$AZ_A|g" \
    -e "s|__AZ_B__|$AZ_B|g" \
    -e "s|__PUBLIC_API_CIDR__|$PUBLIC_API_CIDR|g" \
    -e "s|__RUN_ID__|$run_id|g" \
    -e "s|__WORKFLOW_IDENTITY__|$workflow_identity|g" \
    "$template" >"$output"
  if grep -Eq '0.0.0.0/0|__[A-Z0-9_]+__' "$output"; then
    die "rendered cluster config contains an unrestricted API CIDR"
  fi
}

ecr_repository_uri() {
  printf '%s.dkr.ecr.%s.amazonaws.com/load-tester-%s\n' "$ACCOUNT_ID" "$AWS_REGION" "$1"
}

render_kubernetes_overlay() {
  local overlay=$1 output=$2 role uri work_tree
  work_tree=$(mktemp -d)
  cp -R "$REPO_ROOT/deploy/kubernetes/." "$work_tree/"
  for role in orchestrator worker target; do
    uri=$(ecr_repository_uri "$role")
    (
      cd "$work_tree/overlays/eks"
      run_external_command kustomize edit set image "load-tester-$role=$uri:sha-$GIT_SHA"
    )
  done
  run_external_command kustomize build "$work_tree/overlays/$overlay" >"$output"
  rm -rf "$work_tree"
}

assert_safe_rendered_manifests() {
  local manifest=$1
  if grep -Eq 'example\.invalid|sha-required|image:.*:latest|kind:[[:space:]]*Ingress|type:[[:space:]]*LoadBalancer|kubelet-insecure-tls|0\.0\.0\.0/0' "$manifest"; then
    die "rendered manifests contain an unsafe deployment value"
  fi
  grep -q 'nodeSelector:' "$manifest" || die "rendered manifests are missing node placement"
  grep -q 'image:.*sha-[0-9a-f]\{40\}' "$manifest" || die "rendered manifests are missing immutable image tags"
}

render_all() {
  CLUSTER_CONFIG="$RUN_DIR/cluster.yaml"
  render_cluster_config "$CLUSTER_CONFIG"
  render_kubernetes_overlay eks-worker "$RUN_DIR/eks-worker.yaml"
  render_kubernetes_overlay eks-hpa "$RUN_DIR/eks-hpa.yaml"
  assert_safe_rendered_manifests "$RUN_DIR/eks-worker.yaml"
  assert_safe_rendered_manifests "$RUN_DIR/eks-hpa.yaml"
}

render() {
  preflight
  create_run_directory
  render_all
  printf 'Rendered artifacts: %s\n' "$RUN_DIR"
}

require_clean_git_tree() {
  local status
  status=$(git status --porcelain --untracked-files=all)
  [[ -z "$status" ]] || die "working tree is dirty; immutable ECR tags require a clean source tree"
}

ensure_ecr_repositories() {
  local metadata role repository tags
  for role in orchestrator worker target; do
    repository="load-tester-$role"
    if ! metadata=$(run_external_command aws ecr describe-repositories --repository-names "$repository" --region "$AWS_REGION" --query 'repositories[0].{repositoryArn:repositoryArn,imageTagMutability:imageTagMutability}' --output json 2>&1); then
      if [[ "$metadata" == *RepositoryNotFoundException* ]]; then
        run_external_command aws ecr create-repository --repository-name "$repository" --image-tag-mutability IMMUTABLE --image-scanning-configuration scanOnPush=true --tags Key=load-tester.io/workflow-identity,Value=eks-benchmark --region "$AWS_REGION" >/dev/null
        metadata=$(run_external_command aws ecr describe-repositories --repository-names "$repository" --region "$AWS_REGION" --query 'repositories[0].{repositoryArn:repositoryArn,imageTagMutability:imageTagMutability}' --output json) || die "unable to verify newly created ECR repository: $repository"
      else
        die "unable to describe ECR repository $repository: $metadata"
      fi
    fi
    local repository_arn
    repository_arn=$(python3 - "$metadata" "$ACCOUNT_ID" "$AWS_REGION" "$repository" <<'PY'
import json
import re
import sys

value, account, region, repository = sys.argv[1:]
try:
    metadata = json.loads(value)
except ValueError as exc:
    raise SystemExit(f"invalid ECR repository metadata: {exc}")
arn = metadata.get("repositoryArn")
if not isinstance(arn, str) or arn != f"arn:aws:ecr:{region}:{account}:repository/{repository}":
    raise SystemExit("ECR repository ARN does not match this account, region, and name")
if metadata.get("imageTagMutability") != "IMMUTABLE":
    raise SystemExit("ECR repository must enforce immutable image tags")
print(arn)
PY
) || die "unsafe ECR repository configuration: $repository"
    tags=$(run_external_command aws ecr list-tags-for-resource --resource-arn "$repository_arn" --region "$AWS_REGION" --output json) || die "unable to read ECR ownership tags: $repository"
    python3 - "$tags" <<'PY' || die "ECR repository is not owned by this workflow: $repository"
import json
import sys

try:
    tags = json.loads(sys.argv[1]).get("tags", [])
except ValueError as exc:
    raise SystemExit(f"invalid ECR repository tags: {exc}")
if not isinstance(tags, list):
    raise SystemExit("ECR repository tags must be a list")
ownership = {item.get("Key"): item.get("Value") for item in tags if isinstance(item, dict)}
if ownership.get("load-tester.io/workflow-identity") != "eks-benchmark":
    raise SystemExit("workflow ownership tag is missing or mismatched")
PY
  done
}

apply_ecr_lifecycle_policy() {
  local policy role repository
  policy=$(mktemp)
  cat >"$policy" <<'EOF'
{"rules":[{"rulePriority":1,"description":"Expire untagged benchmark layers after one day","selection":{"tagStatus":"untagged","countType":"sinceImagePushed","countUnit":"days","countNumber":1},"action":{"type":"expire"}},{"rulePriority":2,"description":"Retain the three newest immutable benchmark images","selection":{"tagStatus":"tagged","tagPrefixList":["sha-"],"countType":"imageCountMoreThan","countNumber":3},"action":{"type":"expire"}}]}
EOF
  for role in orchestrator worker target; do
    repository="load-tester-$role"
    run_external_command aws ecr put-lifecycle-policy --repository-name "$repository" --lifecycle-policy-text "file://$policy" --region "$AWS_REGION" >/dev/null
  done
  rm -f "$policy"
}

push_project_images() {
  local role target uri
  for role in orchestrator worker target; do
    uri=$(ecr_repository_uri "$role")
    if [[ "$role" == "orchestrator" ]]; then
      target=orchestrator
    else
      target=$role
    fi
    run_external_command docker buildx build --platform linux/amd64 --push --target "$target" --build-arg "VERSION=sha-$GIT_SHA" --build-arg "VCS_REF=$GIT_SHA" --tag "$uri:sha-$GIT_SHA" "$REPO_ROOT"
  done
}

ecr_push() {
  require_stage2_environment
  trap 'run_ecr_push_exit_cleanup $?' EXIT
  trap 'exit 124' INT TERM
  load_caller_identity
  load_git_sha
  require_clean_git_tree
  ensure_ecr_repositories
  apply_ecr_lifecycle_policy
  local ecr_password
  ecr_password=$(run_external_command aws ecr get-login-password --region "$AWS_REGION")
  run_external_command docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com" <<<"$ecr_password"
  push_project_images
}

deadline_seconds() {
  local configured=${EKS_BENCHMARK_DEADLINE_SECONDS:-5400}
  if [[ "${EKS_BENCHMARK_TESTING:-0}" != "1" && "$configured" != "5400" ]]; then
    die "the EKS benchmark deadline is fixed at 5400 seconds"
  fi
  [[ "$configured" =~ ^[0-9]+$ ]] || die "deadline must be an integer number of seconds"
  printf '%s\n' "$configured"
}

write_owned_run_state() {
  local start deadline run_id seconds temporary
  start=$(date -u +%s)
  seconds=$(deadline_seconds)
  deadline=$((start + seconds))
  run_id=$(basename "$RUN_DIR")
  temporary="$RUN_DIR/.owned-run.json.tmp"
  python3 - "$temporary" "$ACCOUNT_ID" "$AWS_REGION" "$EKS_CLUSTER_NAME" "$run_id" "$GIT_SHA" "$AZ_A" "$AZ_B" "$start" "$deadline" "$(date -u +%F)" <<'PY'
import json
import pathlib
import sys

path, account, region, cluster, run_id, git_sha, az_a, az_b, start, deadline, run_date = sys.argv[1:]
value = {
    "schema_version": 1,
    "account": account,
    "region": region,
    "cluster": cluster,
    "run_id": run_id,
    "git_sha": git_sha,
    "zones": [az_a, az_b],
    "start_epoch": int(start),
    "deadline_epoch": int(deadline),
    "run_date": run_date,
}
pathlib.Path(path).write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
PY
  mv "$temporary" "$RUN_DIR/owned-run.json"
}

resolve_run_directory() {
  if [[ -n "${EKS_RUN_DIR:-}" ]]; then
    RUN_DIR=$EKS_RUN_DIR
  elif [[ -z "$RUN_DIR" ]]; then
    local states=()
    while IFS= read -r state; do states+=("$state"); done < <(find "$ARTIFACT_ROOT" -mindepth 2 -maxdepth 2 -name owned-run.json -print 2>/dev/null || true)
    [[ ${#states[@]} -eq 1 ]] || die "set EKS_RUN_DIR to exactly one owned raw run directory"
    RUN_DIR=$(dirname "${states[0]}")
  fi
  [[ -f "$RUN_DIR/owned-run.json" ]] || die "owned run state is missing"
}

load_owned_run_state() {
  resolve_run_directory
  local values
  values=$(python3 - "$RUN_DIR/owned-run.json" "$(basename "$RUN_DIR")" <<'PY'
from datetime import date
import json
import os
import pathlib
import re
import sys

try:
    data = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, ValueError) as exc:
    raise SystemExit(f"invalid owned run state: {exc}")
required = ("schema_version", "account", "region", "cluster", "run_id", "run_date")
if any(key not in data for key in required):
    raise SystemExit("invalid owned run state schema")
if not re.fullmatch(r"[0-9]{12}", str(data["account"])):
    raise SystemExit("invalid owned run state account")
if data["region"] != "us-west-2" or not re.fullmatch(r"load-tester-benchmark-[a-z0-9][a-z0-9-]{5,30}", data["cluster"]):
    raise SystemExit("invalid owned run state identity")
if not isinstance(data["run_id"], str) or data["run_id"] != sys.argv[2]:
    raise SystemExit("invalid owned run state run ID")
if data["schema_version"] == 1:
    required_v1 = ("git_sha", "zones", "start_epoch", "deadline_epoch")
    if any(key not in data for key in required_v1):
        raise SystemExit("invalid owned run state schema")
    if not isinstance(data["git_sha"], str) or not re.fullmatch(r"[0-9a-f]{40}", data["git_sha"]):
        raise SystemExit("invalid owned run state Git SHA")
    if (not isinstance(data["zones"], list) or len(data["zones"]) != 2 or
            len(set(data["zones"])) != 2 or
            any(not isinstance(zone, str) or not re.fullmatch(r"us-west-2[a-z]", zone) for zone in data["zones"])):
        raise SystemExit("invalid owned run state zones")
    if not isinstance(data["start_epoch"], int) or not isinstance(data["deadline_epoch"], int):
        raise SystemExit("invalid owned run state deadline")
    duration = data["deadline_epoch"] - data["start_epoch"]
    if duration != 5400 and os.environ.get("EKS_BENCHMARK_TESTING") != "1":
        raise SystemExit("invalid owned run state deadline")
    git_sha, start_epoch, deadline_epoch = data["git_sha"], data["start_epoch"], data["deadline_epoch"]
elif data["schema_version"] == 2:
    if data.get("recovered") is not True or not isinstance(data.get("recovered_at"), int):
        raise SystemExit("invalid recovered owned run state")
    git_sha, start_epoch, deadline_epoch = "recovered", data["recovered_at"], data["recovered_at"]
else:
    raise SystemExit("invalid owned run state schema")
try:
    date.fromisoformat(data["run_date"])
except (TypeError, ValueError) as exc:
    raise SystemExit(f"invalid owned run state date: {exc}")
print("\t".join(str(value) for value in (data["account"], data["region"], data["cluster"], data["run_id"], git_sha, start_epoch, deadline_epoch, data["run_date"])))
PY
) || die "owned run state is malformed or unsupported"
  IFS=$'\t' read -r STATE_ACCOUNT STATE_REGION STATE_CLUSTER STATE_RUN_ID STATE_GIT_SHA STATE_START_EPOCH STATE_DEADLINE_EPOCH STATE_RUN_DATE <<<"$values"
}

deadline_remaining() {
  local now
  now=$(date -u +%s)
  (( now < STATE_DEADLINE_EPOCH )) || return 1
}

start_deadline_watchdog() {
  local remaining parent_pid launcher_pid active_command_file grace
  deadline_remaining || die "benchmark deadline elapsed before starting the watchdog"
  remaining=$((STATE_DEADLINE_EPOCH - $(date -u +%s)))
  parent_pid=$$
  launcher_pid=$PPID
  active_command_file="$RUN_DIR/.active-command-pid"
  grace=$(process_termination_grace_seconds)
  python3 -c '
import os
import signal
import sys
import time

os.setsid()
deadline = time.monotonic() + int(sys.argv[1])
grace = int(sys.argv[5])
launcher = int(sys.argv[4])
while time.monotonic() < deadline:
    try:
        os.kill(launcher, 0)
    except ProcessLookupError:
        break
    time.sleep(min(1, deadline - time.monotonic()))
print("benchmark deadline reached", flush=True)
group = None
until = time.monotonic() + grace
while time.monotonic() < until:
    try:
        with open(sys.argv[2], encoding="utf-8") as active:
            fields = active.read().split()
        child = fields[1] if len(fields) > 1 else ""
        if child.isdigit():
            group = int(child)
            try:
                os.killpg(group, signal.SIGTERM)
                print("sent TERM to active command group", flush=True)
            except ProcessLookupError:
                os.kill(group, signal.SIGTERM)
                print("sent TERM to active command", flush=True)
            break
    except (FileNotFoundError, ProcessLookupError):
        pass
    time.sleep(0.05)
if group is not None:
    until = time.monotonic() + grace
    while time.monotonic() < until:
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            break
        time.sleep(0.05)
    else:
        try:
            os.killpg(group, signal.SIGKILL)
            print("sent KILL to active command group", flush=True)
        except ProcessLookupError:
            pass
try:
    os.kill(int(sys.argv[3]), signal.SIGTERM)
    print("sent TERM to benchmark shell", flush=True)
except ProcessLookupError:
    print("benchmark shell already exited", flush=True)
' "$remaining" "$active_command_file" "$parent_pid" "$launcher_pid" "$grace" </dev/null >"$RUN_DIR/deadline-watchdog.log" 2>&1 &
  DEADLINE_WATCHDOG_PID=$!
}

stop_deadline_watchdog() {
  [[ -n "$DEADLINE_WATCHDOG_PID" ]] || return 0
  kill "$DEADLINE_WATCHDOG_PID" 2>/dev/null || true
  wait "$DEADLINE_WATCHDOG_PID" 2>/dev/null || true
  DEADLINE_WATCHDOG_PID=''
}

process_termination_grace_seconds() {
  local seconds=${EKS_PROCESS_TERMINATION_GRACE_SECONDS:-5}
  [[ "$seconds" =~ ^[1-9][0-9]*$ ]] || die "process termination grace must be a positive integer"
  printf '%s\n' "$seconds"
}

emergency_diagnostics_budget_seconds() {
  local seconds=${EKS_EMERGENCY_DIAGNOSTICS_BUDGET_SECONDS:-15}
  [[ "$seconds" =~ ^[1-9][0-9]*$ ]] || die "emergency diagnostics budget must be a positive integer"
  printf '%s\n' "$seconds"
}

emergency_diagnostic_grace_seconds() {
  local seconds=${EKS_EMERGENCY_DIAGNOSTIC_GRACE_SECONDS:-1}
  [[ "$seconds" =~ ^[0-9]+$ ]] || die "emergency diagnostic grace must be a non-negative integer"
  printf '%s\n' "$seconds"
}

# Keep a small local audit trail for the deadline and teardown path. It is raw
# evidence, not curated benchmark output, and uses a monotonic clock so a wall
# clock change cannot make cleanup ordering ambiguous.
write_lifecycle_event() {
  [[ -n "$RUN_DIR" ]] || return 0
  mkdir -p "$RUN_DIR"
  printf '%s %s\n' "$(python3 -c 'import time; print(time.monotonic())')" "$*" >>"$RUN_DIR/lifecycle-events.log"
}

port_forward_stop_timeout_seconds() {
  local seconds=${EKS_PORT_FORWARD_STOP_TIMEOUT_SECONDS:-5}
  [[ "$seconds" =~ ^[1-9][0-9]*$ ]] || die "port-forward stop timeout must be a positive integer"
  printf '%s\n' "$seconds"
}

# TERM, then force-kill and reap an isolated process group before returning.
# The recorded PID is always the group leader created by start_background_process;
# this must never be the parent shell's process group.
terminate_and_reap_process() {
  local pid=$1 grace=${2:-} now
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 0
  [[ -n "$grace" ]] || grace=$(process_termination_grace_seconds)
  [[ "$grace" =~ ^[1-9][0-9]*$ ]] || return 1
  # A negative PID targets only the process group whose leader is $pid. The
  # Python launcher calls setsid(), so this cannot target this Bash process.
  if kill -0 -- "-$pid" 2>/dev/null; then
    write_lifecycle_event "active-command-termination-attempted pid=$pid"
    kill -TERM -- "-$pid" 2>/dev/null || true
    now=$((SECONDS + grace))
    while kill -0 -- "-$pid" 2>/dev/null && (( SECONDS < now )); do
      sleep 0.1
    done
    kill -0 -- "-$pid" 2>/dev/null && kill -KILL -- "-$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
  write_lifecycle_event "active-command-reaped pid=$pid"
  ! kill -0 -- "-$pid" 2>/dev/null
}

stop_active_session_command() {
  local timer=$ACTIVE_COMMAND_TIMER_PID command=$ACTIVE_COMMAND_PID active_child=$ACTIVE_COMMAND_CHILD_PID timeout_file=$ACTIVE_COMMAND_TIMEOUT_FILE owner state_file
  ACTIVE_COMMAND_TIMER_PID=''
  ACTIVE_COMMAND_PID=''
  ACTIVE_COMMAND_CHILD_PID=''
  ACTIVE_COMMAND_TIMEOUT_FILE=''
  state_file=''
  if [[ -n "$RUN_DIR" ]]; then
    state_file="$RUN_DIR/.active-command-pid"
  fi
  if [[ -n "$state_file" && -f "$state_file" ]]; then
    read -r owner command active_child timeout_file <"$state_file" || true
  fi
  # Command substitutions run in a different Bash process. Stop the command
  # supervisor first, then that shell, so neither can outlive this EXIT trap.
  # The supervisor and its command intentionally use different sessions. Stop
  # the command group first so a supervisor signal race cannot strand eksctl.
  [[ -n "$active_child" ]] && terminate_and_reap_process "$active_child" || true
  [[ -n "$command" ]] && terminate_and_reap_process "$command" || true
  [[ -n "$owner" && "$owner" != "$$" ]] && terminate_and_reap_process "$owner" || true
  [[ -n "$timer" ]] && terminate_and_reap_process "$timer" || true
  [[ -n "$timeout_file" ]] && rm -f "$timeout_file" "$timeout_file.wait"
  [[ -n "$state_file" ]] && rm -f "$state_file"
}

# Runs one command under a single supervisor. The supervisor and command each
# own an isolated session, so timeout cleanup can kill every command descendant
# without ever targeting this Bash process group.
run_bounded_command() {
  local seconds=$1 scope=$2
  shift 2
  run_bounded_command_with_grace "$seconds" "$(process_termination_grace_seconds)" "$scope" "$@"
}

# The caller supplies a grace period when several best-effort commands share a
# single budget. Their timeout plus termination grace can then never exceed it.
run_bounded_command_with_grace() {
  local seconds=$1 grace=$2 scope=$3 status child_pid command_pid owner_pid state_file state_tmp status_file ready_file wait_fifo
  shift 3
  [[ "$seconds" =~ ^[1-9][0-9]*$ ]] || die "command timeout must be a positive integer"
  [[ "$grace" =~ ^[0-9]+$ ]] || die "command termination grace must be a non-negative integer"
  owner_pid=${BASHPID:-$$}
  status_file="${TMPDIR:-/tmp}/eks-benchmark-status-$owner_pid-${RANDOM}"
  ready_file="$status_file.ready"
  wait_fifo="$status_file.wait"
  rm -f "$status_file" "$ready_file"
  mkfifo "$wait_fifo"
  exec 9<>"$wait_fifo"
	# Background jobs otherwise receive /dev/null as standard input in
	# non-interactive Bash. Preserve the caller's input for commands such as
	# docker login --password-stdin.
	exec 8<&0
  python3 -c '
import os
import signal
import subprocess
import sys
import time

timeout = int(sys.argv[1])
grace = int(sys.argv[2])
status_path = sys.argv[3]
notify_path = sys.argv[4]
command = sys.argv[5:]
ready_path = sys.argv[5]
command = sys.argv[6:]
os.setsid()
child = subprocess.Popen(command, start_new_session=True, stdin=8)

def report(status):
    temporary = status_path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as output:
        output.write(str(status))
    os.replace(temporary, status_path)
    try:
        descriptor = os.open(notify_path, os.O_WRONLY | os.O_NONBLOCK)
        os.write(descriptor, b"x")
        os.close(descriptor)
    except OSError:
        pass

def stop_child(signum):
    def signal_group(signal_number):
        try:
            os.killpg(child.pid, signal_number)
            return True
        except ProcessLookupError:
            return True
        except PermissionError:
            # Keep the supervisor reportable even if macOS rejects a process
            # group probe while the leader is already exiting.
            try:
                child.send_signal(signal_number)
            except ProcessLookupError:
                pass
            return False

    group_signaled = signal_group(signal.SIGTERM)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        time.sleep(0.05)
    group_signaled = signal_group(signal.SIGKILL) and group_signaled
    child.wait()
    return 128 + signum if group_signaled else 1

def interrupted(signum, _frame):
    status = stop_child(signum)
    report(status)
    raise SystemExit(status)

signal.signal(signal.SIGHUP, interrupted)
signal.signal(signal.SIGINT, interrupted)
signal.signal(signal.SIGTERM, interrupted)
# Publish the PID by renaming a complete file. The Bash parent watches for the
# ready path, so exposing it before the write is complete creates an empty-read
# race for commands that finish quickly.
ready_temporary = ready_path + ".tmp"
with open(ready_temporary, "w", encoding="utf-8") as ready:
    ready.write(str(child.pid))
os.replace(ready_temporary, ready_path)
try:
    status = child.wait(timeout=timeout)
except subprocess.TimeoutExpired:
    stop_child(signal.SIGTERM)
    status = 124
report(status)
raise SystemExit(status)
' "$seconds" "$grace" "$status_file" "$wait_fifo" "$ready_file" "$@" &
  child_pid=$!
  while [[ ! -f "$ready_file" ]]; do
    kill -0 "$child_pid" 2>/dev/null || die "command supervisor exited before establishing its process group: $*"
    sleep 0.01
  done
  command_pid=$(<"$ready_file")
  [[ "$command_pid" =~ ^[1-9][0-9]*$ ]] || die "command supervisor did not publish an active command PID: $*"
  if [[ "$scope" == session || "$scope" == external ]]; then
    ACTIVE_COMMAND_PID=$child_pid
    ACTIVE_COMMAND_CHILD_PID=$command_pid
    ACTIVE_COMMAND_TIMEOUT_FILE=$status_file
    if [[ "$scope" == session ]]; then
      state_file="$RUN_DIR/.active-command-pid"
      state_tmp="$state_file.$owner_pid.$RANDOM"
      printf '%s %s %s %s\n' "$owner_pid" "$child_pid" "$command_pid" "$status_file" >"$state_tmp"
      mv -f "$state_tmp" "$state_file"
    fi
  fi
  # read -t is a shell builtin, and the supervisor wakes it through the FIFO
  # as soon as the command ends, so TERM/INT traps run promptly without a
  # polling helper process.
  while [[ ! -f "$status_file" ]]; do
    read -r -t 1 -n 1 -u 9 _ || true
  done
  status=$(<"$status_file")
  wait "$child_pid" 2>/dev/null || true
  exec 9>&-
	exec 8<&-
  [[ "$status" =~ ^[0-9]+$ ]] || status=1
  rm -f "$status_file" "$ready_file" "$ready_file.tmp" "$wait_fifo"
  if [[ "$scope" == session || "$scope" == external ]]; then
    if [[ "$scope" == session && -f "$state_file" ]] && [[ "$(<"$state_file")" == "$owner_pid $child_pid $command_pid $status_file" ]]; then
      rm -f "$state_file"
    fi
    ACTIVE_COMMAND_PID=''
    ACTIVE_COMMAND_CHILD_PID=''
    ACTIVE_COMMAND_TIMEOUT_FILE=''
  fi
  return "$status"
}

# ecr-push has no run directory, but a buildx push still owns an isolated
# command group. Reap that group before the CLI exits on interruption.
run_ecr_push_exit_cleanup() {
  local status=$1
  trap - EXIT
  (( ECR_PUSH_CLEANUP_RUNNING == 0 )) || exit "$status"
  ECR_PUSH_CLEANUP_RUNNING=1
  trap '' HUP INT TERM
  set +e
  stop_active_session_command
  exit "$status"
}

run_managed_command_with_timeout() {
  local timeout=$1 remaining
  shift
  [[ "$timeout" =~ ^[1-9][0-9]*$ ]] || die "command timeout must be a positive integer"
  deadline_remaining || die "benchmark deadline elapsed before running: $*"
  remaining=$((STATE_DEADLINE_EPOCH - $(date -u +%s)))
  (( timeout <= remaining )) || timeout=$remaining
  run_bounded_command "$timeout" session "$@"
}

run_managed_command() {
  run_managed_command_with_timeout "${EKS_COMMAND_TIMEOUT_SECONDS:-600}" "$@"
}

# Cleanup commands may run after the benchmark deadline, but each is bounded.
run_cleanup_command_with_timeout() {
  local timeout=$1
  shift
  if run_bounded_command "$timeout" cleanup "$@"; then
    return 0
  fi
  CLEANUP_COMMAND_FAILED=1
  return 1
}

run_cleanup_command() {
  run_cleanup_command_with_timeout "${EKS_CLEANUP_COMMAND_TIMEOUT_SECONDS:-300}" "$@"
}

# Cleanup inventory treats a missing resource as data, not a deletion error.
# It remains bounded, while callers convert all unexpected errors to FAIL.
run_cleanup_probe_command() {
  run_bounded_command "${EKS_CLEANUP_COMMAND_TIMEOUT_SECONDS:-300}" cleanup-probe "$@"
}

# Preflight and ECR operations have no benchmark deadline, but cannot hang.
run_external_command() {
  run_bounded_command "${EKS_EXTERNAL_COMMAND_TIMEOUT_SECONDS:-120}" external "$@"
}

run_managed_sleep() {
  run_managed_command sleep "$1"
}

create_owned_cluster() {
  CLUSTER_CREATE_ATTEMPTED=1
  run_managed_command_with_timeout "${EKS_CLUSTER_CREATE_TIMEOUT_SECONDS:-1800}" eksctl create cluster --config-file "$CLUSTER_CONFIG"
}

wait_for_platform_readiness() {
  maybe_fail readiness
  deadline_remaining || die "benchmark deadline elapsed while waiting for the platform"
  run_managed_command kubectl wait --for=condition=Available deployment/autoscale-target --namespace "$NAMESPACE" --timeout=10m
  run_managed_command kubectl wait --for=condition=Available deployment/prometheus --namespace "$NAMESPACE" --timeout=10m
  run_managed_command kubectl wait --for=condition=Available deployment/grafana --namespace "$NAMESPACE" --timeout=10m
}

wait_for_hpa_readiness() {
  deadline_remaining || die "benchmark deadline elapsed while waiting for target HPA components"
  run_managed_command kubectl wait --for=condition=Available deployment/metrics-server --namespace kube-system --timeout=10m
  run_managed_command kubectl wait --for=condition=Available deployment/kube-state-metrics --namespace "$NAMESPACE" --timeout=10m
  run_managed_command kubectl wait --for=condition=Available deployment/autoscale-target --namespace "$NAMESPACE" --timeout=10m
  wait_for_prometheus_sample 'kube_horizontalpodautoscaler_status_current_replicas{namespace="load-tester",horizontalpodautoscaler="autoscale-target"}' 'target HPA metric'
}

install_benchmark_stack() {
  local stack=$1
  [[ -f "$stack" ]] || die "rendered benchmark stack is missing: $stack"
  deadline_remaining || die "benchmark deadline elapsed before installing a stack"
  CURRENT_STACK=$stack
  run_managed_command kubectl apply -f "$CURRENT_STACK"
}

install_scenario_config() {
  local source=$1 destination=$2 rate=${3:-} configmap_manifest
  [[ -f "$source" ]] || die "scenario is missing: $source"
  if [[ -n "$rate" ]]; then
    sed "s/__RATE__/$rate/g" "$source" >"$destination"
  else
    cp "$source" "$destination"
  fi
  # The orchestrator mounts this exact key as /app/config/demo.yaml.
  mkdir -p "$RUN_DIR/configmaps"
  configmap_manifest="$RUN_DIR/configmaps/$(basename "${destination%.yaml}").configmap.yaml"
  run_managed_command kubectl create configmap load-test-config \
    --namespace "$NAMESPACE" \
    --from-file="demo.yaml=$destination" \
    --dry-run=client \
    -o yaml >"$configmap_manifest"
  run_managed_command kubectl apply -f "$configmap_manifest"
}

run_orchestrator_job() {
  local phase=$1
  [[ -n "$CURRENT_STACK" ]] || die "a benchmark stack must be installed before running $phase"
  deadline_remaining || die "benchmark deadline elapsed before $phase"
  maybe_fail "$phase"
  # Jobs are single-use; recreate the suspended Job after each ConfigMap update.
  run_managed_command kubectl delete job/load-tester-orchestrator --namespace "$NAMESPACE" --ignore-not-found --wait=true
  run_managed_command kubectl apply -f "$CURRENT_STACK"
  run_managed_command kubectl patch job/load-tester-orchestrator --namespace "$NAMESPACE" --type merge -p '{"spec":{"suspend":false}}'
  run_managed_command kubectl wait --for=condition=complete job/load-tester-orchestrator --namespace "$NAMESPACE" --timeout=10m
  run_managed_command kubectl logs job/load-tester-orchestrator --namespace "$NAMESPACE"
}

query_prometheus() {
  local query=$1 response
  deadline_remaining || die "benchmark deadline elapsed while querying Prometheus"
  response=$(run_managed_command env "EKS_PROMETHEUS_PHASE=${PROMETHEUS_PHASE:-}" curl --fail --connect-timeout 5 --max-time 20 --get --data-urlencode "query=$query" "http://127.0.0.1:$PROMETHEUS_PORT/api/v1/query")
  [[ "$response" == *'"value"'* ]] || die "Prometheus query returned no sample: $query"
  printf '%s\n' "$response"
}

wait_for_prometheus_sample() {
  local query=$1 label=$2 response
  while true; do
    deadline_remaining || die "benchmark deadline elapsed while waiting for $label"
    response=$(run_managed_command env "EKS_PROMETHEUS_PHASE=${PROMETHEUS_PHASE:-}" curl --fail --connect-timeout 5 --max-time 20 --get --data-urlencode "query=$query" "http://127.0.0.1:$PROMETHEUS_PORT/api/v1/query" 2>/dev/null || true)
    [[ "$response" == *'"value"'* ]] && return 0
    run_managed_sleep 2
  done
}

prometheus_value() {
  local label=$1 query=$2 scale=${3:-1} response output
  response=$(query_prometheus "$query")
  printf '%s\n' "$response" >"$RUN_DIR/prometheus-$label.json"
  output=$(python3 -c '
import json
import math
import sys

scale = float(sys.argv[1])
payload = json.load(sys.stdin)
result = payload.get("data", {}).get("result", [])
if len(result) != 1:
    raise SystemExit("Prometheus query must return exactly one series")
try:
    value = float(result[0]["value"][1]) * scale
except (KeyError, IndexError, TypeError, ValueError) as exc:
    raise SystemExit(f"Prometheus result has no numeric vector sample: {exc}")
if not math.isfinite(value):
    raise SystemExit("Prometheus result is not finite")
print(format(value, ".12g"))
' "$scale" <<<"$response") || die "cannot parse Prometheus result for $label"
  printf '%s\n' "$output"
}

query_prometheus_range() {
  local query=$1 start=$2 end=$3 response
  deadline_remaining || die "benchmark deadline elapsed while querying Prometheus range"
  response=$(run_managed_command env "EKS_PROMETHEUS_PHASE=${PROMETHEUS_PHASE:-}" curl --fail --connect-timeout 5 --max-time 20 --get \
    --data-urlencode "query=$query" --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode 'step=15' \
    "http://127.0.0.1:$PROMETHEUS_PORT/api/v1/query_range")
  [[ "$response" == *'"values"'* ]] || die "Prometheus range query returned no samples: $query"
  printf '%s\n' "$response"
}

# Reduces samples across the recorded experiment window, never a final instant.
prometheus_range_value() {
  local label=$1 query=$2 start=$3 end=$4 reducer=${5:-max} scale=${6:-1} response output
  response=$(query_prometheus_range "$query" "$start" "$end")
  printf '%s\n' "$response" >"$RUN_DIR/prometheus-$label-range.json"
  output=$(python3 -c '
import json
import math
import sys

scale = float(sys.argv[2])
payload = json.load(sys.stdin)
result = payload.get("data", {}).get("result", [])
if len(result) != 1:
    raise SystemExit("Prometheus range query must return exactly one series")
try:
    values = [float(pair[1]) for pair in result[0]["values"]]
except (KeyError, IndexError, TypeError, ValueError) as exc:
    raise SystemExit(f"Prometheus range result has no numeric samples: {exc}")
if not values or not all(math.isfinite(value) for value in values):
    raise SystemExit("Prometheus range result is empty or non-finite")
reducers = {"max": max, "min": min}
if sys.argv[1] not in reducers:
    raise SystemExit("unsupported range reducer")
print(format(reducers[sys.argv[1]](values) * scale, ".12g"))
' "$reducer" "$scale" <<<"$response") || die "cannot parse Prometheus range result for $label"
  printf '%s\n' "$output"
}

begin_experiment_window() {
  EXPERIMENT_START_EPOCH=$(date -u +%s)
}

end_experiment_window() {
  EXPERIMENT_END_EPOCH=$(date -u +%s)
  (( EXPERIMENT_END_EPOCH > EXPERIMENT_START_EPOCH )) || EXPERIMENT_END_EPOCH=$((EXPERIMENT_START_EPOCH + 1))
}

assert_experiment_integrity() {
  local prefix=$1 error_rate=$2 dropped=$3 unmet=$4
  assert_metric "$error_rate" eq 0 "$prefix error rate"
  assert_metric "$dropped" eq 0 "$prefix dropped samples"
  assert_metric "$unmet" eq 0 "$prefix unmet demand"
}

assert_metric() {
  local value=$1 operator=$2 expected=$3 label=$4
  python3 - "$value" "$operator" "$expected" "$label" <<'PY'
import math
import operator
import sys

actual = float(sys.argv[1])
expected = float(sys.argv[3])
operators = {"eq": operator.eq, "ge": operator.ge, "gt": operator.gt, "lt": operator.lt, "le": operator.le}
if not math.isfinite(actual) or not operators[sys.argv[2]](actual, expected):
    raise SystemExit(f"metric invariant failed: {sys.argv[4]} ({actual} {sys.argv[2]} {expected})")
PY
}

run_worker_experiment() {
  local baseline="$RUN_DIR/scenarios/worker-baseline.yaml" evidence="$RUN_DIR/scenarios/worker-autoscale.yaml"
  local baseline_rps baseline_errors baseline_dropped baseline_unmet rate
  install_scenario_config "$REPO_ROOT/deploy/eks/scenarios/worker-baseline.yaml" "$baseline"
  run_orchestrator_job worker-calibration
  PROMETHEUS_PHASE=worker-calibration
  baseline_rps=$(prometheus_value worker-baseline-rps 'sum(rate(loadtester_requests_total[30s]))')
  baseline_errors=$(prometheus_value worker-baseline-errors 'sum(rate(loadtester_request_failures_total[30s])) / clamp_min(sum(rate(loadtester_requests_total[30s])), 1e-9)')
  baseline_dropped=$(prometheus_value worker-baseline-dropped 'max(max_over_time(loadtester_dropped_samples_total[5m]))')
  baseline_unmet=$(prometheus_value worker-baseline-unmet 'max(max_over_time(loadtester_unmet_demand_total[5m]))')
  unset PROMETHEUS_PHASE
  assert_metric "$baseline_rps" gt 0 'worker calibration throughput'
  assert_metric "$baseline_errors" eq 0 'worker calibration /echo error rate'
  assert_metric "$baseline_dropped" eq 0 'worker calibration dropped samples'
  assert_metric "$baseline_unmet" eq 0 'worker calibration unmet demand'
  # Integer RPS must remain inside the agreed band; fail rather than overstate load.
  rate=$(python3 - "$baseline_rps" <<'PY'
import math
import sys

baseline = float(sys.argv[1])
if not math.isfinite(baseline) or baseline <= 0:
    raise SystemExit("worker calibration rate must be a positive finite number")
floor = math.ceil(baseline * 1.25)
ceiling = math.floor(baseline * 1.5)
if floor > ceiling:
    raise SystemExit("calibration is too low to derive an integer challenge inside the approved 1.25x-1.5x band")
print(floor)
PY
  ) || die "cannot derive worker evidence rate from calibration"
  install_scenario_config "$REPO_ROOT/deploy/eks/scenarios/worker-autoscale.yaml.tmpl" "$evidence" "$rate"
  begin_experiment_window
  run_orchestrator_job worker-evidence
  end_experiment_window
  WORKER_PEAK_RPS=$(prometheus_range_value worker-rps 'sum(rate(loadtester_requests_total[30s]))' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
  WORKER_P50_MS=$(prometheus_range_value worker-p50 'histogram_quantile(0.50, sum by (le) (rate(loadtester_request_duration_seconds_bucket[30s])))' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH" max 1000)
  WORKER_P95_MS=$(prometheus_range_value worker-p95 'histogram_quantile(0.95, sum by (le) (rate(loadtester_request_duration_seconds_bucket[30s])))' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH" max 1000)
  WORKER_P99_MS=$(prometheus_range_value worker-p99 'histogram_quantile(0.99, sum by (le) (rate(loadtester_request_duration_seconds_bucket[30s])))' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH" max 1000)
  WORKER_PEAK_WORKERS=$(prometheus_range_value worker-peak-workers 'loadtester_active_workers' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
  WORKER_ERROR_RATE=$(prometheus_range_value worker-error-rate 'sum(rate(loadtester_request_failures_total[30s])) / clamp_min(sum(rate(loadtester_requests_total[30s])), 1e-9)' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
  WORKER_DROPPED_SAMPLES=$(prometheus_range_value worker-dropped-samples 'loadtester_dropped_samples_total' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
  WORKER_UNMET_DEMAND=$(prometheus_range_value worker-unmet-demand 'loadtester_unmet_demand_total' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
  local coverage
  coverage=$(prometheus_value worker-scheduler-coverage 'loadtester_autoscaler_scheduler_coverage_ratio')
  assert_metric "$WORKER_PEAK_RPS" gt 0 'worker peak RPS'
  assert_metric "$WORKER_PEAK_WORKERS" ge 2 'worker fleet scale-up'
  assert_metric "$WORKER_PEAK_WORKERS" le 4 'worker fleet upper bound'
  assert_metric "$coverage" ge 0.9 'worker scheduler coverage recovery'
  assert_experiment_integrity 'worker /echo' "$WORKER_ERROR_RATE" "$WORKER_DROPPED_SAMPLES" "$WORKER_UNMET_DEMAND"
}

run_hpa_experiment() {
  local baseline="$RUN_DIR/scenarios/target-baseline.yaml" evidence="$RUN_DIR/scenarios/target-hpa.yaml" rate=45 attempt=1
  install_benchmark_stack "$RUN_DIR/eks-hpa.yaml"
  wait_for_hpa_readiness
  install_scenario_config "$REPO_ROOT/deploy/eks/scenarios/target-baseline.yaml" "$baseline"
  run_orchestrator_job target-baseline
  local baseline_p95
  PROMETHEUS_PHASE=target-baseline
  baseline_p95=$(prometheus_value target-baseline-p95 'histogram_quantile(0.95, sum by (le) (rate(loadtester_request_duration_seconds_bucket[30s])))' 1000)
  unset PROMETHEUS_PHASE
  while true; do
    install_scenario_config "$REPO_ROOT/deploy/eks/scenarios/target-hpa.yaml.tmpl" "$evidence" "$rate"
    cp "$evidence" "$RUN_DIR/scenarios/target-hpa-attempt-$attempt.yaml"
    begin_experiment_window
    run_orchestrator_job target-hpa-evidence
    end_experiment_window
    PROMETHEUS_PHASE=target-hpa
    HPA_PEAK_RPS=$(prometheus_range_value hpa-rps 'sum(rate(loadtester_requests_total[30s]))' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
    HPA_P50_MS=$(prometheus_range_value hpa-p50 'histogram_quantile(0.50, sum by (le) (rate(loadtester_request_duration_seconds_bucket[30s])))' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH" max 1000)
    HPA_P95_MS=$(prometheus_range_value hpa-p95 'histogram_quantile(0.95, sum by (le) (rate(loadtester_request_duration_seconds_bucket[30s])))' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH" max 1000)
    HPA_P99_MS=$(prometheus_range_value hpa-p99 'histogram_quantile(0.99, sum by (le) (rate(loadtester_request_duration_seconds_bucket[30s])))' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH" max 1000)
    HPA_PEAK_WORKERS=$(prometheus_range_value hpa-peak-workers 'loadtester_active_workers' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
    HPA_TARGET_REPLICAS=$(prometheus_range_value hpa-target-replicas 'kube_horizontalpodautoscaler_status_current_replicas{namespace="load-tester",horizontalpodautoscaler="autoscale-target"}' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
    HPA_ERROR_RATE=$(prometheus_range_value hpa-error-rate 'sum(rate(loadtester_request_failures_total[30s])) / clamp_min(sum(rate(loadtester_requests_total[30s])), 1e-9)' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
    HPA_DROPPED_SAMPLES=$(prometheus_range_value hpa-dropped-samples 'loadtester_dropped_samples_total' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
    HPA_UNMET_DEMAND=$(prometheus_range_value hpa-unmet-demand 'loadtester_unmet_demand_total' "$EXPERIMENT_START_EPOCH" "$EXPERIMENT_END_EPOCH")
    unset PROMETHEUS_PHASE
    assert_experiment_integrity 'target HPA' "$HPA_ERROR_RATE" "$HPA_DROPPED_SAMPLES" "$HPA_UNMET_DEMAND"
    if python3 - "$HPA_TARGET_REPLICAS" <<'PY'
import sys
raise SystemExit(0 if float(sys.argv[1]) >= 3 else 1)
PY
    then
      break
    fi
    (( attempt < 4 )) || die "target HPA did not reach three replicas after approved rate escalations"
    rate=$(python3 - "$rate" <<'PY'
import math
import sys
print(math.ceil(float(sys.argv[1]) * 1.5))
PY
    ) || die "cannot derive the next target HPA rate"
    ((attempt += 1))
  done
  assert_metric "$HPA_PEAK_RPS" gt 0 'target HPA peak RPS'
  assert_metric "$HPA_PEAK_WORKERS" eq 2 'target HPA fixed workers'
  assert_metric "$HPA_TARGET_REPLICAS" ge 3 'target HPA scale-up'
  assert_metric "$HPA_TARGET_REPLICAS" lt 5 'target HPA terminal capacity'
  assert_metric "$HPA_P95_MS" lt "$baseline_p95" 'target HPA p95 recovery'
}

collect_diagnostics() {
  [[ -n "$RUN_DIR" ]] || return 0
  local budget grace deadline remaining timeout label output status
  budget=$(emergency_diagnostics_budget_seconds)
  grace=$(emergency_diagnostic_grace_seconds)
  deadline=$((SECONDS + budget))
  : >"$RUN_DIR/diagnostics-report.txt"
  collect_one_diagnostic() {
    label=$1
    output=$2
    shift 2
    remaining=$((deadline - SECONDS))
    if (( remaining <= grace )); then
      printf '%s skipped: shared budget exhausted\n' "$label" >>"$RUN_DIR/diagnostics-report.txt"
      write_lifecycle_event "diagnostic-skipped label=$label"
      return 0
    fi
    # Reserve the grace interval for TERM/KILL and reaping. This makes every
    # diagnostic's full lifecycle fit inside the shared budget.
    timeout=$((remaining - grace))
    write_lifecycle_event "diagnostic-attempted label=$label timeout=${timeout}s"
    if run_bounded_command_with_grace "$timeout" "$grace" cleanup-diagnostic "$@" >"$output" 2>&1; then
      status=success
    else
      status=failed
    fi
    printf '%s %s timeout=%ss remaining=%ss\n' "$label" "$status" "$timeout" "$((deadline - SECONDS))" >>"$RUN_DIR/diagnostics-report.txt"
    write_lifecycle_event "diagnostic-$status label=$label"
    return 0
  }
  collect_one_diagnostic kubernetes-all "$RUN_DIR/kubernetes-all.txt" kubectl get all --namespace "$NAMESPACE" -o wide
  collect_one_diagnostic kubernetes-events "$RUN_DIR/kubernetes-events.txt" kubectl get events --namespace "$NAMESPACE" --sort-by=.lastTimestamp
  collect_one_diagnostic orchestrator-log "$RUN_DIR/orchestrator.log" kubectl logs job/load-tester-orchestrator --namespace "$NAMESPACE" --all-containers
}

collect_human_reviewed_dashboard() {
  local image=${EKS_DASHBOARD_PNG:-} review=${EKS_DASHBOARD_REVIEW:-}
  [[ -n "$image" && -n "$review" ]] || die "while Grafana is available at http://127.0.0.1:$GRAFANA_PORT, set EKS_DASHBOARD_PNG and EKS_DASHBOARD_REVIEW to a human-reviewed screenshot and its SHA-bound review JSON"
  printf 'Grafana is available at http://127.0.0.1:%s. Waiting for the human-reviewed dashboard files.\n' "$GRAFANA_PORT"
  while [[ ! -f "$image" || ! -f "$review" ]]; do
    deadline_remaining || die "benchmark deadline elapsed while waiting for EKS_DASHBOARD_PNG and EKS_DASHBOARD_REVIEW"
    verify_grafana_port_forward
    run_managed_sleep 2
  done
  python3 - "$image" "$review" <<'PY' || die "dashboard-review.json must approve the exact EKS_DASHBOARD_PNG bytes"
import hashlib
import json
import pathlib
import re
import sys

image, review = map(pathlib.Path, sys.argv[1:])
try:
    value = json.loads(review.read_text(encoding="utf-8"))
except (OSError, ValueError) as exc:
    raise SystemExit(f"invalid dashboard review: {exc}")
digest = hashlib.sha256(image.read_bytes()).hexdigest()
if (not isinstance(value, dict) or value.get("approved") is not True or
        not isinstance(value.get("reviewed_by"), str) or not value["reviewed_by"].strip() or
        not isinstance(value.get("reviewed_on"), str) or
        not isinstance(value.get("dashboard_sha256"), str) or
        not re.fullmatch(r"[0-9a-fA-F]{64}", value["dashboard_sha256"]) or
        value["dashboard_sha256"].lower() != digest):
    raise SystemExit("review does not approve the supplied dashboard bytes")
PY
  cp "$image" "$RUN_DIR/dashboard.png"
  cp "$review" "$RUN_DIR/dashboard-review.json"
}

reserve_local_port() {
  if [[ "${EKS_BENCHMARK_TESTING:-0}" == "1" ]]; then
    printf '%s\n' "${1:?test port is required}"
    return 0
  fi
  python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

wait_for_local_endpoint() {
  local url=$1 label=$2 process_pid=$3
  while true; do
    deadline_remaining || die "benchmark deadline elapsed while waiting for $label"
    kill -0 "$process_pid" 2>/dev/null || die "$label process exited before becoming ready"
    if run_managed_command curl --fail --silent --connect-timeout 5 --max-time 20 "$url" >/dev/null 2>&1; then
      return 0
    fi
    run_managed_sleep 2
  done
}

verify_grafana_port_forward() {
  [[ "$GRAFANA_PORT_FORWARD_PID" =~ ^[1-9][0-9]*$ ]] && kill -0 "$GRAFANA_PORT_FORWARD_PID" 2>/dev/null || die "Grafana port-forward is unreachable while waiting for dashboard evidence"
  run_managed_command curl --fail --silent --connect-timeout 5 --max-time 20 "http://127.0.0.1:$GRAFANA_PORT/api/health" >/dev/null 2>&1 || die "Grafana port-forward is unreachable while waiting for dashboard evidence"
}

start_background_process() {
  local variable_name=$1 log_file=$2 pid saved_int saved_term deferred_signal=''
  shift 2
  saved_int=$(trap -p INT || true)
  saved_term=$(trap -p TERM || true)
  # Signals received during spawn are remembered, not acted on, until the new
  # group leader is registered for EXIT cleanup.
  trap 'deferred_signal=INT' INT
  trap 'deferred_signal=TERM' TERM
  python3 -c '
import os
import sys

os.setsid()
os.execvp(sys.argv[1], sys.argv[1:])
' "$@" >"$log_file" 2>&1 &
  pid=$!
  # Register the group leader before restoring signal handlers. It is the sole
  # identifier used by cleanup, so all descendants are covered as well.
  printf -v "$variable_name" '%s' "$pid"
  if [[ -n "$saved_int" ]]; then eval "$saved_int"; else trap - INT; fi
  if [[ -n "$saved_term" ]]; then eval "$saved_term"; else trap - TERM; fi
  if [[ -n "$deferred_signal" ]]; then
    kill -"$deferred_signal" "$$"
  fi
  kill -0 "$pid" 2>/dev/null || die "background process exited during startup: $*"
}

start_metric_port_forwards() {
  PROMETHEUS_PORT=$(reserve_local_port "${EKS_PROMETHEUS_PORT:-19090}")
  GRAFANA_PORT=$(reserve_local_port "${EKS_GRAFANA_PORT:-13000}")
  start_background_process PROMETHEUS_PORT_FORWARD_PID "$RUN_DIR/prometheus-port-forward.log" kubectl port-forward --address 127.0.0.1 --namespace "$NAMESPACE" service/prometheus "$PROMETHEUS_PORT:9090"
  start_background_process GRAFANA_PORT_FORWARD_PID "$RUN_DIR/grafana-port-forward.log" kubectl port-forward --address 127.0.0.1 --namespace "$NAMESPACE" service/grafana "$GRAFANA_PORT:3000"
  wait_for_local_endpoint "http://127.0.0.1:$PROMETHEUS_PORT/-/ready" "Prometheus port-forward" "$PROMETHEUS_PORT_FORWARD_PID"
  wait_for_local_endpoint "http://127.0.0.1:$GRAFANA_PORT/api/health" "Grafana port-forward" "$GRAFANA_PORT_FORWARD_PID"
}

stop_metric_port_forwards() {
  local pid failed=0 timeout deadline
  timeout=$(port_forward_stop_timeout_seconds)
  # Terminate both independent groups first, then give them one shared grace
  # window. Serial waits would make two stuck forwards consume 2x the contract.
  for pid in "$PROMETHEUS_PORT_FORWARD_PID" "$GRAFANA_PORT_FORWARD_PID"; do
    [[ -n "$pid" ]] || continue
    kill -0 -- "-$pid" 2>/dev/null && kill -TERM -- "-$pid" 2>/dev/null || true
  done
  deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    local running=0
    for pid in "$PROMETHEUS_PORT_FORWARD_PID" "$GRAFANA_PORT_FORWARD_PID"; do
      [[ -n "$pid" ]] && kill -0 -- "-$pid" 2>/dev/null && running=1
    done
    (( running == 0 )) && break
    sleep 0.1
  done
  for pid in "$PROMETHEUS_PORT_FORWARD_PID" "$GRAFANA_PORT_FORWARD_PID"; do
    [[ -n "$pid" ]] || continue
    kill -0 -- "-$pid" 2>/dev/null && kill -KILL -- "-$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    kill -0 -- "-$pid" 2>/dev/null && failed=1
  done
  PROMETHEUS_PORT_FORWARD_PID=''
  GRAFANA_PORT_FORWARD_PID=''
  (( failed == 0 )) || CLEANUP_COMMAND_FAILED=1
  return "$failed"
}

collect_raw_evidence() {
  maybe_fail collect
  [[ -n "$WORKER_PEAK_RPS" && -n "$HPA_PEAK_RPS" ]] || die "experiments did not produce measured Prometheus evidence"
  collect_runtime_topology
  cat >"$RUN_DIR/metrics.json" <<EOF
{"run_id":"$(basename "$RUN_DIR")","git_sha":"$GIT_SHA","region":"$AWS_REGION","topology":{"kubernetes_version":"$KUBERNETES_VERSION","eks_version":"$EKS_VERSION","instance_types":{"loadgen":"c7i.large","platform":"m7i.large"},"node_counts":{"loadgen":$LOADGEN_NODE_COUNT,"platform":$PLATFORM_NODE_COUNT}},"worker_experiment":{"scenario":{"target":"/echo","model":"constant-rate","duration":"3m"},"measured":{"peak_rps":$WORKER_PEAK_RPS,"p50_ms":$WORKER_P50_MS,"p95_ms":$WORKER_P95_MS,"p99_ms":$WORKER_P99_MS,"peak_workers":$WORKER_PEAK_WORKERS,"error_rate":$WORKER_ERROR_RATE,"dropped_samples":$WORKER_DROPPED_SAMPLES,"unmet_demand":$WORKER_UNMET_DEMAND}},"target_hpa_experiment":{"scenario":{"target":"/burn?ms=20","model":"constant-rate","duration":"2m"},"measured":{"peak_rps":$HPA_PEAK_RPS,"p50_ms":$HPA_P50_MS,"p95_ms":$HPA_P95_MS,"p99_ms":$HPA_P99_MS,"peak_workers":$HPA_PEAK_WORKERS,"target_replicas":$HPA_TARGET_REPLICAS,"error_rate":$HPA_ERROR_RATE,"dropped_samples":$HPA_DROPPED_SAMPLES,"unmet_demand":$HPA_UNMET_DEMAND}},"teardown":{"status":"PENDING","verified_clean":false},"billing_attestation":{"type":"pending","confirmed":false}}
EOF
  cp "$RUN_DIR/metrics.json" "$RUN_DIR/report.json"
  collect_human_reviewed_dashboard
}

collect_runtime_topology() {
  local kubernetes_json
  EKS_VERSION=$(run_managed_command aws eks describe-cluster --name "$EKS_CLUSTER_NAME" --region "$AWS_REGION" --query 'cluster.version' --output text)
  [[ "$EKS_VERSION" =~ ^[0-9]+\.[0-9]+$ ]] || die "EKS did not return a supported Kubernetes version"
  kubernetes_json=$(run_managed_command kubectl version --output json)
  KUBERNETES_VERSION=$(python3 -c '
import json
import sys

value = json.load(sys.stdin).get("serverVersion", {}).get("gitVersion", "")
if not isinstance(value, str) or not value.startswith("v"):
    raise SystemExit("kubectl version did not contain serverVersion.gitVersion")
print(value[1:])
' <<<"$kubernetes_json") || die "cannot parse Kubernetes server version"
  [[ "$KUBERNETES_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]] || die "Kubernetes server version is invalid"
  LOADGEN_NODE_COUNT=$(run_managed_command kubectl get nodes --selector load-tester.io/pool=loadgen --no-headers | awk 'NF { count += 1 } END { print count + 0 }')
  PLATFORM_NODE_COUNT=$(run_managed_command kubectl get nodes --selector load-tester.io/pool=platform --no-headers | awk 'NF { count += 1 } END { print count + 0 }')
  [[ "$LOADGEN_NODE_COUNT" == 2 && "$PLATFORM_NODE_COUNT" == 2 ]] || die "observed EKS node topology does not match the approved fixed two-node pools"
}

maybe_fail() {
  if [[ "${EKS_BENCHMARK_TESTING:-0}" == "1" && "${EKS_BENCHMARK_FAIL_PHASE:-}" == "$1" ]]; then
    die "simulated failure during $1"
  fi
  return 0
}

assert_owned_run() {
  require_common_environment
  load_caller_identity
  load_owned_run_state
  [[ "$ACCOUNT_ID" == "$STATE_ACCOUNT" && "$AWS_REGION" == "$STATE_REGION" && "$EKS_CLUSTER_NAME" == "$STATE_CLUSTER" ]] || die "owned run state does not match the current account, region, and cluster"
}

run_ownership_probe() {
  if [[ -n "$RUN_DIR" ]]; then
    run_cleanup_probe_command "$@"
  else
    run_external_command "$@"
  fi
}

validate_eks_ownership_tags() {
  local tags=$1 run_id=$2
  printf '%s' "$tags" | python3 -c '
import json
import sys

tags = json.load(sys.stdin).get("tags", {})
if tags.get("load-tester.io/workflow-identity") != "eks-benchmark":
    raise SystemExit(1)
if tags.get("load-tester.io/run-id") != sys.argv[1]:
    raise SystemExit(1)
' "$run_id" || die "EKS cloud ownership tags do not match this owned run"
}

# A failed eksctl create can leave its CloudFormation stack before an EKS API
# object exists. This accepts only the exact stack name and ownership tags.
owned_cloudformation_stack_run_id() {
  local account=$1 region=$2 cluster=$3 expected_run_id=${4:-} stack_name stack
  stack_name="eksctl-$cluster-cluster"
  stack=$(run_ownership_probe aws cloudformation describe-stacks --stack-name "$stack_name" --region "$region" --query 'Stacks[0].{StackId:StackId,Tags:Tags}' --output json 2>&1) || die "unable to read CloudFormation ownership stack: $stack"
  python3 - "$stack" "$account" "$region" "$stack_name" "$expected_run_id" <<'PY'
import json
import re
import sys

value, account, region, stack_name, expected_run_id = sys.argv[1:]
try:
    stack = json.loads(value)
except ValueError as exc:
    raise SystemExit(f"invalid CloudFormation ownership stack: {exc}")
stack_id = stack.get("StackId")
pattern = rf"arn:aws:cloudformation:{re.escape(region)}:{re.escape(account)}:stack/{re.escape(stack_name)}/[^/]+"
if not isinstance(stack_id, str) or not re.fullmatch(pattern, stack_id):
    raise SystemExit("CloudFormation stack identity does not match this account, region, and cluster")
tags = stack.get("Tags")
if not isinstance(tags, list):
    raise SystemExit("CloudFormation ownership tags are missing")
ownership = {item.get("Key"): item.get("Value") for item in tags if isinstance(item, dict)}
if ownership.get("load-tester.io/workflow-identity") != "eks-benchmark":
    raise SystemExit("CloudFormation workflow ownership tag is missing or mismatched")
run_id = ownership.get("load-tester.io/run-id")
if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}", run_id):
    raise SystemExit("CloudFormation run ID ownership tag is missing or unsafe")
if expected_run_id and run_id != expected_run_id:
    raise SystemExit("CloudFormation run ID ownership tag does not match this owned run")
print(run_id)
PY
}

assert_owned_cluster() {
  local arn output tags
  assert_owned_run
  if arn=$(run_ownership_probe aws eks describe-cluster --name "$STATE_CLUSTER" --region "$STATE_REGION" --query 'cluster.arn' --output text 2>&1); then
    [[ "$arn" == arn:aws:eks:* ]] || die "EKS cluster did not return an ARN for ownership verification"
    tags=$(run_ownership_probe aws eks list-tags-for-resource --resource-arn "$arn" --output json) || die "unable to read EKS cluster ownership tags"
    validate_eks_ownership_tags "$tags" "$STATE_RUN_ID"
    return 0
  fi
  output=$arn
  [[ "$output" == *ResourceNotFoundException* ]] || die "unable to read EKS cluster ownership tags: $output"
  owned_cloudformation_stack_run_id "$STATE_ACCOUNT" "$STATE_REGION" "$STATE_CLUSTER" "$STATE_RUN_ID" >/dev/null || die "CloudFormation cloud ownership tags do not match this owned run"
}

delete_exposed_kubernetes_resources() {
  local failed=0
  run_cleanup_command kubectl delete service --namespace "$NAMESPACE" --field-selector spec.type=LoadBalancer --ignore-not-found || failed=1
  run_cleanup_command kubectl delete ingress --namespace "$NAMESPACE" --all --ignore-not-found || failed=1
  return "$failed"
}

delete_owned_cluster() {
  CLUSTER_DELETE_ATTEMPTED=1
  run_cleanup_command_with_timeout "${EKS_CLUSTER_DELETE_TIMEOUT_SECONDS:-1200}" eksctl delete cluster --name "$STATE_CLUSTER" --region "$STATE_REGION" --wait
}

inventory_remaining_resources() {
  local cluster_absent=0 count failed=0 inventory='' output
  if output=$(run_cleanup_probe_command aws eks describe-cluster --name "$STATE_CLUSTER" --region "$STATE_REGION" 2>&1); then
    inventory+='eks=1'$'\n'
    failed=1
  elif [[ "$output" == *ResourceNotFoundException* ]]; then
    inventory+='eks=0'$'\n'
    cluster_absent=1
  else
    inventory+='eks=ERROR'$'\n'
    failed=1
  fi
  inventory_count() {
    local category=$1
    shift
    if count=$(run_cleanup_probe_command "$@"); then
      inventory+=$(inventory_line "$category" "$count")$'\n'
      [[ "$count" =~ ^0[[:space:]]*$ ]] || failed=1
    else
      inventory+="$category=ERROR"$'\n'
      failed=1
    fi
  }
  if (( cluster_absent == 1 )); then
    inventory+='nodegroups=0'$'\n'
  else
    inventory_count nodegroups aws eks list-nodegroups --cluster-name "$STATE_CLUSTER" --region "$STATE_REGION" --query 'length(nodegroups)' --output text
  fi
  inventory_count ec2 aws ec2 describe-instances --region "$STATE_REGION" --filters "Name=tag:alpha.eksctl.io/cluster-name,Values=$STATE_CLUSTER" --query "length(Reservations[].Instances[?State.Name != 'terminated'][])" --output text
  inventory_count asg aws autoscaling describe-auto-scaling-groups --region "$STATE_REGION" --query "length(AutoScalingGroups[?contains(AutoScalingGroupName, '$STATE_CLUSTER')])" --output text
  inventory_count cloudformation aws cloudformation list-stacks --region "$STATE_REGION" --query "length(StackSummaries[?contains(StackName, '$STATE_CLUSTER') && StackStatus != 'DELETE_COMPLETE'])" --output text
  inventory_count ebs aws ec2 describe-volumes --region "$STATE_REGION" --filters "Name=tag:alpha.eksctl.io/cluster-name,Values=$STATE_CLUSTER" --query 'length(Volumes)' --output text
  inventory_count elb aws elbv2 describe-load-balancers --region "$STATE_REGION" --query "length(LoadBalancers[?contains(LoadBalancerName, '$STATE_CLUSTER')])" --output text
  inventory_count nat aws ec2 describe-nat-gateways --region "$STATE_REGION" --filter "Name=tag:alpha.eksctl.io/cluster-name,Values=$STATE_CLUSTER" --query "length(NatGateways[?State != 'deleted'])" --output text
  inventory_count eip aws ec2 describe-addresses --region "$STATE_REGION" --filters "Name=tag:alpha.eksctl.io/cluster-name,Values=$STATE_CLUSTER" --query 'length(Addresses)' --output text
  inventory+='ecr=lifecycle-managed'$'\n'
  printf '%s' "$inventory"
  return "$failed"
}

inventory_line() {
  local category=$1 count=$2
  count=${count//$'\n'/}
  [[ "$count" =~ ^[0-9]+$ ]] || count=1
  printf '%s=%s' "$category" "$count"
}

write_cleanup_report() {
  local status=$1 inventory=$2 now
  now=$(date -u +%FT%TZ)
  python3 - "$RUN_DIR/cleanup-report.json" "$status" "$ACCOUNT_ID" "$STATE_REGION" "$STATE_CLUSTER" "$now" "$inventory" <<'PY'
import json
import pathlib
import sys

path, status, account, region, cluster, checked_at, inventory = sys.argv[1:]
categories = dict(line.split("=", 1) for line in inventory.splitlines() if "=" in line)
pathlib.Path(path).write_text(json.dumps({
    "status": status,
    "account": account,
    "region": region,
    "cluster": cluster,
    "checked_at": checked_at,
    "categories": categories,
}, sort_keys=True) + "\n", encoding="utf-8")
PY
}

update_metrics_teardown() {
  [[ -f "$RUN_DIR/metrics.json" ]] || return 0
  python3 - "$RUN_DIR/metrics.json" "$RUN_DIR/report.json" <<'PY'
import json
import pathlib
import sys

for raw_path in sys.argv[1:]:
    path = pathlib.Path(raw_path)
    value = json.loads(path.read_text(encoding="utf-8"))
    value["teardown"] = {"status": "PASS", "verified_clean": True}
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
PY
}

verify_cleanup() {
  assert_owned_run
  local inventory status
  if inventory=$(inventory_remaining_resources); then
    status=PASS
  else
    status=FAIL
  fi
  if (( CLEANUP_COMMAND_FAILED != 0 )); then
    status=FAIL
    inventory+=$'\ncleanup_commands=ERROR\n'
  fi
  write_cleanup_report "$status" "$inventory"
  [[ "$status" == PASS ]] || die "cleanup verification failed: $inventory"
  update_metrics_teardown
  printf 'Cleanup: PASS\n'
}

teardown() {
  assert_owned_cluster
  local exposed_status=0
  delete_exposed_kubernetes_resources || exposed_status=1
  delete_owned_cluster
  (( exposed_status == 0 )) || die "exposed Kubernetes resource cleanup failed; owned cluster deletion was still attempted"
}

run_exit_cleanup() {
  local status=$1
  trap - EXIT
  (( EXIT_CLEANUP_RUNNING == 0 )) || exit "$status"
  EXIT_CLEANUP_RUNNING=1
  trap '' HUP INT TERM
  set +e
  (( status == 0 )) || CLEANUP_COMMAND_FAILED=1
  stop_deadline_watchdog
  stop_active_session_command
  stop_metric_port_forwards || status=1
  if (( CLUSTER_CREATE_ATTEMPTED == 1 && RUN_FINALIZED == 0 )); then
    collect_diagnostics
    if (( CLUSTER_DELETE_ATTEMPTED == 0 )); then
      if assert_owned_cluster; then
        delete_exposed_kubernetes_resources || status=1
        delete_owned_cluster || status=1
      else
        status=1
      fi
    fi
    verify_cleanup || status=1
  fi
  exit "$status"
}

run_benchmark() {
  require_stage2_environment
  trap 'run_exit_cleanup $?' EXIT
  trap 'exit 124' INT TERM
  preflight
  create_run_directory
  render_all
  write_owned_run_state
  load_owned_run_state
  start_deadline_watchdog
  create_owned_cluster
  install_benchmark_stack "$RUN_DIR/eks-worker.yaml"
  wait_for_platform_readiness
  start_metric_port_forwards
  run_worker_experiment
  run_hpa_experiment
  collect_diagnostics
  collect_raw_evidence
  stop_metric_port_forwards
  teardown
  verify_cleanup
  RUN_FINALIZED=1
  stop_deadline_watchdog
  trap - INT TERM
  trap - EXIT
  printf 'Benchmark lifecycle completed with verified cleanup\n'
}

acknowledge_billing_check() {
  local checked_on=$1 today
  today=$(date -u +%F)
  python3 - "$STATE_RUN_DATE" "$checked_on" "$today" <<'PY' || die "billing check must be the day after the benchmark run and cannot be in the future"
from datetime import date
import sys

try:
    run_date, checked_on, today = (date.fromisoformat(value) for value in sys.argv[1:])
except ValueError as exc:
    raise SystemExit(f"invalid billing date: {exc}")
if (checked_on - run_date).days != 1:
    raise SystemExit("billing check is not exactly one calendar day after the benchmark run")
if checked_on > today:
    raise SystemExit("billing check is in the future")
PY
  python3 - "$RUN_DIR/metrics.json" "$RUN_DIR/report.json" "$checked_on" <<'PY'
import json
import pathlib
import sys

for raw_path in sys.argv[1:3]:
    path = pathlib.Path(raw_path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"cannot update raw report: {exc}")
    value["billing_attestation"] = {
        "type": "human",
        "confirmed": True,
        "checked_on": sys.argv[3],
        "statement": "Human billing-console review; not an API-verified cost result.",
    }
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
PY
}

acknowledge_billing() {
  [[ $# -eq 2 && "$1" == "--checked-on" ]] || die "usage: acknowledge-billing --checked-on YYYY-MM-DD"
  assert_owned_run
  assert_verified_cleanup_report
  acknowledge_billing_check "$2"
  [[ -f "$EKS_REDACTOR" ]] || die "EKS evidence redactor is missing: $EKS_REDACTOR"
  python3 "$EKS_REDACTOR" --input "$RUN_DIR" --output "$REPO_ROOT/docs/benchmarks/eks/$(basename "$RUN_DIR")" \
    --expected-run-id "$STATE_RUN_ID" --expected-git-sha "$STATE_GIT_SHA" --expected-region "$STATE_REGION" \
    --expected-cleanup-account "$STATE_ACCOUNT" --expected-cleanup-region "$STATE_REGION" --expected-cleanup-cluster "$STATE_CLUSTER"
}

# Billing publication follows verified cleanup; the cluster is expected to be gone.
assert_verified_cleanup_report() {
  [[ -f "$RUN_DIR/cleanup-report.json" ]] || die "cleanup PASS is required before billing acknowledgement"
  python3 - "$RUN_DIR/cleanup-report.json" "$STATE_ACCOUNT" "$STATE_REGION" "$STATE_CLUSTER" <<'PY' || die "verified cleanup PASS with zero cleanup errors is required before billing acknowledgement"
import json
import pathlib
import sys

try:
    report = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, ValueError) as exc:
    raise SystemExit(f"invalid cleanup report: {exc}")
if report.get("status") != "PASS":
    raise SystemExit("cleanup status is not PASS")
if (report.get("account"), report.get("region"), report.get("cluster")) != tuple(sys.argv[2:]):
    raise SystemExit("cleanup report identity does not match owned run")
categories = report.get("categories", {})
if not isinstance(categories, dict) or any(str(value).upper() == "ERROR" for value in categories.values()):
    raise SystemExit("cleanup report contains errors")
PY
}

# Rebuilds a minimal local ownership record only after the live cluster proves
# it belongs to this workflow. This is for a replacement machine after loss.
recover_owned_run_state() {
  local arn describe_output tags run_id cluster_account recovered_at temporary
  if arn=$(run_external_command aws eks describe-cluster --name "$EKS_CLUSTER_NAME" --region "$AWS_REGION" --query 'cluster.arn' --output text 2>&1); then
    cluster_account=$(python3 - "$arn" "$AWS_REGION" "$EKS_CLUSTER_NAME" <<'PY'
import re
import sys

match = re.fullmatch(r"arn:aws:eks:([^:]+):([0-9]{12}):cluster/([a-z0-9][a-z0-9-]+)", sys.argv[1])
if not match or match.group(1) != sys.argv[2] or match.group(3) != sys.argv[3]:
    raise SystemExit("unexpected EKS cluster ARN")
print(match.group(2))
PY
) || die "live EKS cluster ARN is invalid for recovery"
    [[ "$cluster_account" == "$ACCOUNT_ID" ]] || die "live EKS cluster account does not match the authenticated caller"
    tags=$(run_external_command aws eks list-tags-for-resource --resource-arn "$arn" --output json) || die "unable to read live EKS ownership tags for recovery"
    run_id=$(python3 - "$tags" <<'PY'
import json
import re
import sys

tags = json.loads(sys.argv[1]).get("tags", {})
if tags.get("load-tester.io/workflow-identity") != "eks-benchmark":
    raise SystemExit("workflow ownership tag is missing or mismatched")
run_id = tags.get("load-tester.io/run-id")
if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}", run_id):
    raise SystemExit("run ID ownership tag is missing or unsafe")
print(run_id)
PY
) || die "live EKS cloud ownership tags are not safe for recovery"
  else
    describe_output=$arn
    [[ "$describe_output" == *ResourceNotFoundException* ]] || die "unable to read live EKS cluster for recovery: $describe_output"
    run_id=$(owned_cloudformation_stack_run_id "$ACCOUNT_ID" "$AWS_REGION" "$EKS_CLUSTER_NAME") || die "CloudFormation cloud ownership tags are not safe for recovery"
  fi
  RUN_DIR="$ARTIFACT_ROOT/$run_id"
  [[ ! -e "$RUN_DIR/owned-run.json" ]] || die "owned run state already exists; use teardown instead of recovery"
  mkdir -p "$RUN_DIR"
  recovered_at=$(date -u +%s)
  temporary="$RUN_DIR/.owned-run.json.tmp"
  python3 - "$temporary" "$ACCOUNT_ID" "$AWS_REGION" "$EKS_CLUSTER_NAME" "$run_id" "$recovered_at" "$(date -u +%F)" <<'PY'
import json
import pathlib
import sys

path, account, region, cluster, run_id, recovered_at, run_date = sys.argv[1:]
pathlib.Path(path).write_text(json.dumps({
    "schema_version": 2,
    "recovered": True,
    "account": account,
    "region": region,
    "cluster": cluster,
    "run_id": run_id,
    "recovered_at": int(recovered_at),
    "run_date": run_date,
}, sort_keys=True) + "\n", encoding="utf-8")
PY
  mv "$temporary" "$RUN_DIR/owned-run.json"
}

recover_teardown() {
  [[ $# -eq 1 && "$1" == "--acknowledge-recovery" ]] || die "usage: recover-teardown --acknowledge-recovery"
  require_common_environment
  load_caller_identity
  recover_owned_run_state
  local teardown_status=0
  teardown || teardown_status=1
  verify_cleanup
  (( teardown_status == 0 )) || die "recovery teardown completed with exposed Kubernetes cleanup errors"
}
