#!/usr/bin/env bash
set -euo pipefail

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Required command is missing: %s\n' "$1" >&2
    exit 1
  fi
}

require_executable() {
  if [[ "$1" == */* ]]; then
    if [[ ! -x "$1" ]]; then
      printf 'Required executable is unavailable: %s\n' "$1" >&2
      exit 1
    fi
    return
  fi
  require_command "$1"
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
NAMESPACE=load-tester
CLUSTER_NAME=${KIND_CLUSTER_NAME:-load-tester-piece22-$$}
KUBE_CONTEXT="kind-$CLUSTER_NAME"
NODE_IMAGE=${KIND_NODE_IMAGE:-kindest/node:v1.34.8@sha256:02722c2dedddcfc00febf5d27fbeb9b7b2c14294c82109ff4a85d89ac9ba3256}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-240}
KEEP_KIND_CLUSTER=${KEEP_KIND_CLUSTER:-0}
EXPECTED_WORKERS=3
EXPECTED_RPS=90
MIN_THROUGHPUT_RATIO=0.90
RECOVERY_WINDOW_SECONDS=10
DIP_TIMEOUT_SECONDS=10
MIN_STEADY_RPS=81
ORCHESTRATOR_IMAGE=load-tester-orchestrator:kind
WORKER_IMAGE=load-tester-worker:kind
TARGET_IMAGE=load-tester-target:kind
EXTERNAL_IMAGES=(
  prom/prometheus:v3.13.1
  grafana/grafana:13.1.0
  busybox:1.37.0
)
TMP_DIR=''
PROMETHEUS_PORT_FORWARD_PID=''
GRAFANA_PORT_FORWARD_PID=''
CLUSTER_OWNED=0
CLEANUP_FAILED=0
LIFECYCLE_PROVEN=0
FAULT_NODE_NAME=''
DIP_ACTIVE_WORKERS=''
DIP_EPOCH=0

kubectl_kind() {
  kubectl --context "$KUBE_CONTEXT" "$@"
}

cleanup_failure() {
  printf 'CLEANUP FAILED: %s\n' "$1" >&2
  CLEANUP_FAILED=1
}

cluster_presence() {
  local clusters
  clusters=$("$KIND_BIN" get clusters 2>/dev/null) || return 2
  if grep -Fxq "$CLUSTER_NAME" <<<"$clusters"; then
    return 0
  fi
  return 1
}

delete_owned_cluster() {
  local delete_status=0
  local lookup_status=0

  if [[ "$KEEP_KIND_CLUSTER" == 1 ]]; then
    printf 'Cluster preserved for debugging: %s (context %s)\n' "$CLUSTER_NAME" "$KUBE_CONTEXT"
    return 0
  fi

  "$KIND_BIN" delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || delete_status=$?
  if (( delete_status != 0 )); then
    cleanup_failure "could not delete kind cluster $CLUSTER_NAME"
    return 1
  fi

  cluster_presence || lookup_status=$?
  case "$lookup_status" in
    0)
      cleanup_failure "kind cluster $CLUSTER_NAME still exists"
      return 1
      ;;
    1)
      ;;
    *)
      cleanup_failure "could not verify deletion of kind cluster $CLUSTER_NAME"
      return 1
      ;;
  esac

  if (( LIFECYCLE_PROVEN != 1 )); then
    cleanup_failure 'worker teardown was not proven before cluster deletion'
    return 1
  fi
  printf '%s\n' 'Cleanup: PASS (workers=0 cluster=deleted)'
}

prometheus_value() {
  local query=$1
  local response
  response=$(curl --silent --fail --get --data-urlencode "query=$query" "http://127.0.0.1:$PROMETHEUS_PORT/api/v1/query" 2>/dev/null || true)
  python3 -c 'import json,math,sys
try:
 data=json.loads(sys.argv[1]); result=data["data"]["result"]
 value=result[0]["value"][1] if result else ""
 print(value if value not in ("NaN", "+Inf", "-Inf") and (not value or math.isfinite(float(value))) else "")
except Exception:
 print("")' "$response"
}

metric_is_at_least() {
  local value=$1
  local minimum=$2
  [[ -n "$value" ]] && awk -v value="$value" -v minimum="$minimum" 'BEGIN { exit !(value >= minimum) }'
}

current_ready_worker_pods() {
  local response
  response=$(kubectl_kind get pods --namespace "$NAMESPACE" --selector app.kubernetes.io/name=load-tester-worker -o json 2>/dev/null || true)
  python3 -c 'import json,sys; pods=json.loads(sys.argv[1]).get("items", []); [print("{}\t{}".format(pod["metadata"]["name"], pod["metadata"]["uid"])) for pod in sorted(pods, key=lambda item: item["metadata"]["name"]) if pod.get("status", {}).get("phase") == "Running" and {condition["type"]: condition.get("status") for condition in pod.get("status", {}).get("conditions", [])}.get("Ready") == "True"]' "$response" 2>/dev/null || true
}

deployment_has_replica_count() {
  local deployment=$1
  local want=$2
  local desired
  local ready
  desired=$(kubectl_kind get "deployment/$deployment" --namespace "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)
  ready=$(kubectl_kind get "deployment/$deployment" --namespace "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
  ready=${ready:-0}
  [[ "$desired" == "$want" && "$ready" == "$want" ]]
}

worker_pod_count() {
  local response
  response=$(kubectl_kind get pods --namespace "$NAMESPACE" --selector app.kubernetes.io/name=load-tester-worker -o json 2>/dev/null) || return 2
  [[ -n "$response" ]] || return 2
  python3 -c 'import json,sys
try:
 data=json.loads(sys.argv[1])
 items=data["items"]
 if not isinstance(data, dict) or not isinstance(items, list): raise ValueError
 print(len(items))
except Exception:
 sys.exit(2)' "$response" 2>/dev/null
}

worker_teardown_observed() {
  local desired
  local remaining
  desired=$(kubectl_kind get deployment/load-tester-worker --namespace "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null) || return 2
  [[ "$desired" =~ ^[0-9]+$ ]] || return 2
  [[ "$desired" == 0 ]] || return 1
  remaining=$(worker_pod_count) || return $?
  [[ "$remaining" == 0 ]]
}

wait_for_worker_teardown() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local teardown_status=0
  while true; do
    teardown_status=0
    worker_teardown_observed || teardown_status=$?
    case "$teardown_status" in
      0)
        return 0
        ;;
      1)
        if (( SECONDS >= deadline )); then
          printf '%s\n' 'Worker teardown timed out before desired replicas and pods reached zero' >&2
          return 1
        fi
        sleep 1
        ;;
      *)
        printf '%s\n' 'Could not observe worker teardown state' >&2
        return 1
        ;;
    esac
  done
}

validate_orchestrator_result() {
  local orchestrator_exit=$1
  local orchestrator_log=$2
  local expected_failure
  local summary_status=0
  expected_failure="worker $VICTIM_NAME failed: stream closed; unreported metric interval lost"

  if [[ "$orchestrator_exit" != 1 ]]; then
    printf 'Unexpected orchestrator exit: got=%s want=1\n' "$orchestrator_exit" >&2
    return 1
  fi

  printf '%s\n' "$orchestrator_log" | python3 -c 'import re,sys
victim_name=sys.argv[1]
lines=sys.stdin.read().splitlines()
while lines and not lines[-1].strip():
 lines.pop()
starts=[index for index,line in enumerate(lines) if line.startswith("Test:")]
if not starts:
 sys.exit(1)
summary=lines[starts[-1]:]
expected_fields=[
 "Test: kubernetes-worker-fault-injection",
 "Model: constant-rate",
 "Result: FAIL",
]
for index,expected in enumerate(expected_fields):
 if index >= len(summary) or summary[index] != expected:
  sys.exit(1)
patterns=[
 r"Duration: .+",
 r"Requests: [0-9]+",
 r"Succeeded: [0-9]+",
 r"Failed: [0-9]+",
 r"Transport errors: [0-9]+",
 r"Client errors: [0-9]+",
 r"Server errors: [0-9]+",
 r"Unmet demand: [0-9]+",
 r"Dropped samples: 0",
 r"Bytes read: [0-9]+",
 r"Throughput: .+ req/s",
 r"Error rate: .+%",
 r"Latency: p50=.+ p95=.+ p99=.+",
]
for offset,pattern in enumerate(patterns, start=3):
 if offset >= len(summary) or not re.fullmatch(pattern, summary[offset]):
  sys.exit(1)
unmet_demand=int(summary[10].split(": ", 1)[1])
violations_offset=3+len(patterns)
if violations_offset >= len(summary) or summary[violations_offset] != "Violations:":
 sys.exit(1)
violations=summary[violations_offset+1:]
expected_failure="worker {} failed: stream closed; unreported metric interval lost".format(victim_name)
failure_violation_count=violations.count("- "+expected_failure)
if failure_violation_count != 1:
 sys.exit(1)
unmet_violation_count=0
for violation in violations:
 if violation == "- "+expected_failure:
  continue
 unmet_match=re.fullmatch(r"- unmet demand: ([1-9][0-9]*)", violation)
 if unmet_match and unmet_demand > 0 and int(unmet_match.group(1)) == unmet_demand:
  unmet_violation_count+=1
  continue
 sys.exit(1)
if unmet_violation_count > 1:
 sys.exit(1)
sys.exit(0)' "$VICTIM_NAME" || summary_status=$?

  if (( summary_status != 0 )); then
    printf '%s\n' 'Orchestrator result did not match the required final summary contract' >&2
    return 1
  fi
}

wait_for_orchestrator_completion() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local succeeded
  local failed
  while true; do
    succeeded=$(kubectl_kind get job/load-tester-orchestrator --namespace "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
    failed=$(kubectl_kind get job/load-tester-orchestrator --namespace "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null || true)
    if [[ "$succeeded" == 1 || "$failed" == 1 ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      printf '%s\n' 'Orchestrator Job did not terminate before timeout' >&2
      return 1
    fi
    sleep 1
  done
}

worker_is_contributing() {
  local worker_name=$1
  local active
  local requests
  active=$(prometheus_value "loadtester_worker_active{worker_id=\"$worker_name\"}")
  requests=$(prometheus_value "loadtester_worker_requests_total{worker_id=\"$worker_name\"}")
  [[ "$active" == 1 ]] && metric_is_at_least "$requests" 0.0000001
}

capture_steady_workers() {
  local -a workers=()
  local worker
  local worker_name
  local worker_uid
  while IFS= read -r worker; do
    [[ -n "$worker" ]] || continue
    workers[${#workers[@]}]=$worker
  done < <(current_ready_worker_pods)
  (( ${#workers[@]} == EXPECTED_WORKERS )) || return 1
  deployment_has_replica_count load-tester-worker "$EXPECTED_WORKERS" || return 1
  [[ "$(prometheus_value 'loadtester_active_workers')" == "$EXPECTED_WORKERS" ]] || return 1
  for worker in "${workers[@]}"; do
    IFS=$'\t' read -r worker_name worker_uid <<<"$worker"
    [[ -n "$worker_name" && -n "$worker_uid" ]] || return 1
    worker_is_contributing "$worker_name" || return 1
  done
  STEADY_WORKERS=("${workers[@]}")
}

wait_for_steady_workers() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  until capture_steady_workers; do
    if (( SECONDS >= deadline )); then
      printf '%s\n' 'Steady worker fleet did not reach three Ready, active, contributing workers' >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_steady_throughput() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local steady_since=0
  local now
  local rps
  while true; do
    rps=$(prometheus_value 'rate(loadtester_requests_total[10s])')
    now=$(date +%s)
    if metric_is_at_least "$rps" "$MIN_STEADY_RPS"; then
      if (( steady_since == 0 )); then
        steady_since=$now
      fi
      if (( now - steady_since >= 20 )); then
        printf 'Steady throughput: PASS (rps=%s, seconds=%d)\n' "$rps" "$((now - steady_since))"
        return 0
      fi
    else
      steady_since=0
    fi
    if (( SECONDS >= deadline )); then
      printf 'Steady throughput did not remain at or above %s RPS for 20 seconds\n' "$MIN_STEADY_RPS" >&2
      return 1
    fi
    sleep 1
  done
}

initial_worker_uid_exists() {
  local uid=$1
  local worker
  local worker_name
  local worker_uid
  for worker in "${STEADY_WORKERS[@]}"; do
    IFS=$'\t' read -r worker_name worker_uid <<<"$worker"
    [[ "$worker_uid" == "$uid" ]] && return 0
  done
  return 1
}

wait_for_replacement() {
  local deadline=$((SECONDS + 30))
  local -a workers=()
  local worker
  local worker_name
  local worker_uid
  while true; do
    workers=()
    while IFS= read -r worker; do
      [[ -n "$worker" ]] || continue
      workers[${#workers[@]}]=$worker
    done < <(current_ready_worker_pods)
    if deployment_has_replica_count load-tester-worker "$EXPECTED_WORKERS" && \
      [[ "$(prometheus_value 'loadtester_active_workers')" == "$EXPECTED_WORKERS" ]]; then
      for worker in "${workers[@]}"; do
        IFS=$'\t' read -r worker_name worker_uid <<<"$worker"
        if [[ "$worker_name" != "$VICTIM_NAME" && "$worker_uid" != "$VICTIM_UID" ]] && \
          ! initial_worker_uid_exists "$worker_uid" && worker_is_contributing "$worker_name"; then
          REPLACEMENT_NAME=$worker_name
          REPLACEMENT_UID=$worker_uid
          RECOVERY_EPOCH=$(date +%s)
          return 0
        fi
      done
    fi
    if (( SECONDS >= deadline )); then
      printf '%s\n' 'replacement worker did not contribute requests within 30 seconds' >&2
      return 1
    fi
    sleep 1
  done
}

recovery_rate_window_is_complete() {
  local now
  now=$(date +%s)
  (( now > RECOVERY_EPOCH + RECOVERY_WINDOW_SECONDS ))
}

wait_for_recovery_throughput() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local rps
  while true; do
    if recovery_rate_window_is_complete; then
      rps=$(prometheus_value 'rate(loadtester_requests_total[10s])')
      if metric_is_at_least "$rps" "$MIN_STEADY_RPS"; then
        printf 'Recovery throughput: PASS (rps=%s, window=%ss)\n' "$rps" "$RECOVERY_WINDOW_SECONDS"
        return 0
      fi
    fi
    if (( SECONDS >= deadline )); then
      printf 'Recovery throughput did not reach %s RPS over a %ss window\n' "$MIN_STEADY_RPS" "$RECOVERY_WINDOW_SECONDS" >&2
      return 1
    fi
    sleep 1
  done
}

# wait_for_active_worker_dip holds replacement scheduling until Prometheus has
# observed the orchestrator's reduced active-worker membership.
wait_for_active_worker_dip() {
  local deadline=$((SECONDS + DIP_TIMEOUT_SECONDS))
  local active_workers
  while true; do
    active_workers=$(prometheus_value 'loadtester_active_workers')
    if [[ -n "$active_workers" ]] && awk -v active="$active_workers" -v expected="$EXPECTED_WORKERS" 'BEGIN { exit !(active < expected) }'; then
      DIP_ACTIVE_WORKERS=$active_workers
      DIP_EPOCH=$(date +%s)
      printf 'Active worker dip: PASS (active_workers=%s epoch=%s)\n' "$DIP_ACTIVE_WORKERS" "$DIP_EPOCH"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      printf 'Prometheus did not observe active workers below %s within %ss\n' "$EXPECTED_WORKERS" "$DIP_TIMEOUT_SECONDS" >&2
      return 1
    fi
    sleep 1
  done
}

uncordon_fault_node() {
  local node_name=$FAULT_NODE_NAME
  if [[ -z "$node_name" ]]; then
    return 0
  fi
  kubectl_kind uncordon "$node_name" >/dev/null || return 1
  FAULT_NODE_NAME=''
}

pod_uid_exists() {
  local uid=$1
  local response
  response=$(kubectl_kind get pods --namespace "$NAMESPACE" -o json 2>/dev/null) || return 2
  [[ -n "$response" ]] || return 2
  python3 -c 'import json,sys
try:
 data=json.loads(sys.argv[1])
 items=data["items"]
 if not isinstance(data, dict) or not isinstance(items, list): raise ValueError
 present=False
 for pod in items:
  metadata=pod["metadata"]
  if not isinstance(pod, dict) or not isinstance(metadata, dict): raise ValueError
  present=present or metadata.get("uid") == sys.argv[2]
except Exception:
 sys.exit(2)
sys.exit(0 if present else 1)' "$response" "$uid"
}

verify_victim_uid_absent() {
  local pod_uid_status=0
  pod_uid_exists "$VICTIM_UID" || pod_uid_status=$?
  case "$pod_uid_status" in
    0)
      printf 'Deleted worker UID remains present: %s\n' "$VICTIM_UID" >&2
      return 1
      ;;
    1)
      return 0
      ;;
    *)
      printf 'Could not verify deletion of worker UID: %s\n' "$VICTIM_UID" >&2
      return 1
      ;;
  esac
}

refresh_and_select_victim() {
  wait_for_steady_workers
  IFS=$'\t' read -r VICTIM_NAME VICTIM_UID <<<"${STEADY_WORKERS[0]}"
  VICTIM_REQUESTS_BEFORE=$(prometheus_value "loadtester_worker_requests_total{worker_id=\"$VICTIM_NAME\"}")
  if ! metric_is_at_least "$VICTIM_REQUESTS_BEFORE" 0.0000001; then
    printf 'Selected victim %s has no positive request count: %s\n' "$VICTIM_NAME" "$VICTIM_REQUESTS_BEFORE" >&2
    return 1
  fi
}

reach_steady_state_and_select_victim() {
  wait_for_steady_workers
  wait_for_steady_throughput
  refresh_and_select_victim
}

if [[ "${TEST_KIND_FAULT_SOURCE_ONLY:-0}" == 1 ]]; then
  return 0 2>/dev/null || exit 0
fi

TMP_DIR=$(mktemp -d)

# Diagnostics retain the exact state needed when later fault assertions fail.
diagnostics() {
  printf '%s\n' '--- Kubernetes fault-injection diagnostics ---' >&2
  kubectl_kind get deployments,pods --namespace "$NAMESPACE" -o custom-columns='KIND:.kind,NAME:.metadata.name,UID:.metadata.uid,READY:.status.readyReplicas' >&2 || true
  kubectl_kind get events --namespace "$NAMESPACE" --sort-by=.lastTimestamp >&2 || true
  kubectl_kind logs --namespace "$NAMESPACE" job/load-tester-orchestrator --all-containers >&2 || true

  local worker_pod
  while IFS= read -r worker_pod; do
    [[ -n "$worker_pod" ]] || continue
    printf '%s\n' "--- Worker log: $worker_pod ---" >&2
    kubectl_kind logs --namespace "$NAMESPACE" "$worker_pod" --all-containers >&2 || true
  done < <(kubectl_kind get pods --namespace "$NAMESPACE" --selector app.kubernetes.io/name=load-tester-worker -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)

  if [[ -n "${PROMETHEUS_PORT:-}" ]]; then
    printf 'Final Prometheus values: active_workers=%s worker_active=%s worker_requests_total=%s\n' \
      "$(prometheus_value 'loadtester_active_workers')" \
      "$(prometheus_value 'sum(loadtester_worker_active)')" \
      "$(prometheus_value 'sum(loadtester_worker_requests_total)')" >&2
  fi
}

cleanup() {
  local exit_status=$?
  local cluster_status=0
  trap - EXIT
  set +e

  if ! uncordon_fault_node; then
    cleanup_failure "could not uncordon fault node $FAULT_NODE_NAME"
  fi

  for pid in "$PROMETHEUS_PORT_FORWARD_PID" "$GRAFANA_PORT_FORWARD_PID"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done

  if (( exit_status != 0 )) && (( CLUSTER_OWNED )); then
    cluster_status=0
    cluster_presence || cluster_status=$?
  fi
  if (( exit_status != 0 )) && (( CLUSTER_OWNED )) && (( cluster_status == 0 )); then
    diagnostics
  fi

  if (( CLUSTER_OWNED )); then
    delete_owned_cluster || true
  fi
  rm -rf "$TMP_DIR" || cleanup_failure 'could not remove temporary files'

  if (( CLEANUP_FAILED )); then
    exit_status=1
  fi
  exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in curl docker kubectl python3; do
  require_command "$command"
done

KIND_BIN=${KIND_BIN:-kind}
require_executable "$KIND_BIN"

if ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'Docker is installed but the daemon is unavailable' >&2
  exit 1
fi

reserve_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_port_forward() {
  local url=$1
  local pid=$2
  local log_path=$3
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  until curl --silent --fail "$url" >/dev/null 2>&1; do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      printf 'Port-forward exited before %s became ready:\n' "$url" >&2
      cat "$log_path" >&2 || true
      return 1
    fi
    if (( SECONDS >= deadline )); then
      printf 'Port-forwarded endpoint did not become ready: %s\n' "$url" >&2
      cat "$log_path" >&2 || true
      return 1
    fi
    sleep 1
  done
}

wait_for_replica_count() {
  local deployment=$1
  local want=$2
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local desired=''
  local ready=''
  while true; do
    desired=$(kubectl_kind get "deployment/$deployment" --namespace "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)
    ready=$(kubectl_kind get "deployment/$deployment" --namespace "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
    ready=${ready:-0}
    if [[ "$desired" == "$want" && "$ready" == "$want" ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      printf 'Replica wait timed out for %s: desired=%s ready=%s want=%s\n' "$deployment" "$desired" "$ready" "$want" >&2
      return 1
    fi
    sleep 2
  done
}

printf '%s\n' 'Building local Kubernetes images...'
docker build --quiet --target orchestrator --tag "$ORCHESTRATOR_IMAGE" "$REPO_ROOT" >/dev/null
docker build --quiet --target worker --tag "$WORKER_IMAGE" "$REPO_ROOT" >/dev/null
docker build --quiet --target target --tag "$TARGET_IMAGE" "$REPO_ROOT" >/dev/null

printf 'Creating kind cluster %s with Kubernetes 1.34.8...\n' "$CLUSTER_NAME"
cluster_status=0
cluster_presence || cluster_status=$?
case "$cluster_status" in
  0)
    printf 'Refusing to reuse existing kind cluster: %s\n' "$CLUSTER_NAME" >&2
    exit 1
    ;;
  1)
    ;;
  *)
    printf 'Could not list kind clusters before creation: %s\n' "$CLUSTER_NAME" >&2
    exit 1
    ;;
esac
"$KIND_BIN" create cluster --name "$CLUSTER_NAME" --image "$NODE_IMAGE" --wait "${TIMEOUT_SECONDS}s"
CLUSTER_OWNED=1
"$KIND_BIN" load docker-image --name "$CLUSTER_NAME" "$ORCHESTRATOR_IMAGE" "$WORKER_IMAGE" "$TARGET_IMAGE"

# Pull sequentially inside the node to avoid competing registry downloads.
printf '%s\n' 'Caching external Kubernetes images in the kind node...'
for image in "${EXTERNAL_IMAGES[@]}"; do
  docker exec "$CLUSTER_NAME-control-plane" crictl pull "$image" >/dev/null
done

kubectl_kind apply -k "$REPO_ROOT/deploy/kubernetes/overlays/kind-fault" >/dev/null
for deployment in autoscale-target prometheus grafana; do
  kubectl_kind rollout status "deployment/$deployment" --namespace "$NAMESPACE" --timeout "${TIMEOUT_SECONDS}s"
done
wait_for_replica_count autoscale-target 1

PROMETHEUS_PORT=$(reserve_port)
GRAFANA_PORT=$(reserve_port)
kubectl_kind port-forward --address 127.0.0.1 --namespace "$NAMESPACE" service/prometheus "$PROMETHEUS_PORT:9090" >"$TMP_DIR/prometheus-port-forward.log" 2>&1 &
PROMETHEUS_PORT_FORWARD_PID=$!
kubectl_kind port-forward --address 127.0.0.1 --namespace "$NAMESPACE" service/grafana "$GRAFANA_PORT:3000" >"$TMP_DIR/grafana-port-forward.log" 2>&1 &
GRAFANA_PORT_FORWARD_PID=$!
wait_port_forward "http://127.0.0.1:$PROMETHEUS_PORT/-/ready" "$PROMETHEUS_PORT_FORWARD_PID" "$TMP_DIR/prometheus-port-forward.log"
wait_port_forward "http://127.0.0.1:$GRAFANA_PORT/api/health" "$GRAFANA_PORT_FORWARD_PID" "$TMP_DIR/grafana-port-forward.log"

dashboard_search=$(curl --silent --fail --user admin:admin "http://127.0.0.1:$GRAFANA_PORT/api/search?query=Load%20Tester")
if [[ "$dashboard_search" != *'"uid":"load-tester-overview"'* ]]; then
  printf 'Grafana Load Tester dashboard was not provisioned: %s\n' "$dashboard_search" >&2
  exit 1
fi

kubectl_kind patch job/load-tester-orchestrator --namespace "$NAMESPACE" --type merge --patch '{"spec":{"suspend":false}}' >/dev/null
deadline=$((SECONDS + TIMEOUT_SECONDS))
until [[ "$(prometheus_value 'up{job="orchestrator"}')" == 1 ]]; do
  if (( SECONDS >= deadline )); then
    printf '%s\n' 'Prometheus did not scrape the orchestrator before timeout' >&2
    exit 1
  fi
  sleep 2
done

wait_for_replica_count autoscale-target 1
reach_steady_state_and_select_victim

FAULT_NODE_NAME=$(kubectl_kind get nodes -o jsonpath='{.items[0].metadata.name}')
if [[ -z "$FAULT_NODE_NAME" ]]; then
  printf '%s\n' 'Could not identify the kind node for the fault gate' >&2
  exit 1
fi
kubectl_kind cordon "$FAULT_NODE_NAME" >/dev/null
DELETE_EPOCH=$(date +%s)
printf 'Injecting active worker failure: name=%s uid=%s requests_before=%s epoch=%s\n' \
  "$VICTIM_NAME" "$VICTIM_UID" "$VICTIM_REQUESTS_BEFORE" "$DELETE_EPOCH"
kubectl_kind delete pod "$VICTIM_NAME" --namespace "$NAMESPACE" --wait=false >/dev/null
wait_for_active_worker_dip
uncordon_fault_node

REPLACEMENT_NAME=''
REPLACEMENT_UID=''
RECOVERY_EPOCH=0
wait_for_replacement
printf 'Replacement registration: PASS (name=%s uid=%s epoch=%s)\n' \
  "$REPLACEMENT_NAME" "$REPLACEMENT_UID" "$RECOVERY_EPOCH"

if (( DIP_EPOCH < DELETE_EPOCH || DIP_EPOCH > RECOVERY_EPOCH )); then
  printf 'Active worker dip epoch %s was outside deletion/recovery bounds %s..%s\n' \
    "$DIP_EPOCH" "$DELETE_EPOCH" "$RECOVERY_EPOCH" >&2
  exit 1
fi

verify_victim_uid_absent

wait_for_recovery_throughput
VICTIM_REQUESTS_AFTER=$(prometheus_value "loadtester_worker_requests_total{worker_id=\"$VICTIM_NAME\"}")
if ! metric_is_at_least "$VICTIM_REQUESTS_AFTER" "$VICTIM_REQUESTS_BEFORE"; then
  printf 'deleted worker request history regressed: before=%s after=%s\n' \
    "$VICTIM_REQUESTS_BEFORE" "$VICTIM_REQUESTS_AFTER" >&2
  exit 1
fi

printf '%s\n' 'Fault recovery: PASS (active-worker dip, replacement registration, throughput, victim history)'

wait_for_orchestrator_completion
orchestrator_pod=$(kubectl_kind get pods --namespace "$NAMESPACE" --selector job-name=load-tester-orchestrator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -z "$orchestrator_pod" ]]; then
  printf '%s\n' 'Could not identify the completed orchestrator pod' >&2
  exit 1
fi
orchestrator_exit=$(kubectl_kind get pod "$orchestrator_pod" --namespace "$NAMESPACE" -o jsonpath='{.status.containerStatuses[?(@.name=="orchestrator")].state.terminated.exitCode}' 2>/dev/null || true)
orchestrator_log=$(kubectl_kind logs "$orchestrator_pod" --namespace "$NAMESPACE" --container orchestrator 2>/dev/null || true)
if ! validate_orchestrator_result "$orchestrator_exit" "$orchestrator_log"; then
  printf 'Completed orchestrator pod=%s exit=%s\n%s\n' "$orchestrator_pod" "$orchestrator_exit" "$orchestrator_log" >&2
  exit 1
fi

wait_for_worker_teardown
LIFECYCLE_PROVEN=1
printf '%s\n' 'Fault result: PASS (exit=1 result=FAIL dropped_samples=0 worker_loss=1 workers=0)'
