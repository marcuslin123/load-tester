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

for command in awk curl docker kubectl python3; do
  require_command "$command"
done

KIND_BIN=${KIND_BIN:-kind}
require_executable "$KIND_BIN"

if ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'Docker is installed but the daemon is unavailable' >&2
  exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
NAMESPACE=load-tester
CLUSTER_NAME=${KIND_CLUSTER_NAME:-load-tester-piece21-$$}
KUBE_CONTEXT="kind-$CLUSTER_NAME"
NODE_IMAGE=${KIND_NODE_IMAGE:-kindest/node:v1.34.8@sha256:02722c2dedddcfc00febf5d27fbeb9b7b2c14294c82109ff4a85d89ac9ba3256}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-240}
SCALE_DOWN_TIMEOUT_SECONDS=${SCALE_DOWN_TIMEOUT_SECONDS:-300}
KEEP_KIND_CLUSTER=${KEEP_KIND_CLUSTER:-0}
MIN_TARGET_REPLICAS=3
MAX_TARGET_REPLICAS=5
MIN_THROUGHPUT_RATIO=0.90
MIN_P95_IMPROVEMENT=0.20
MAX_CAPACITY_LIMITED_SECONDS=30
EXPECTED_RPS=30
ORCHESTRATOR_IMAGE=load-tester-orchestrator:kind
WORKER_IMAGE=load-tester-worker:kind
TARGET_IMAGE=load-tester-target:kind
EXTERNAL_IMAGES=(
  registry.k8s.io/metrics-server/metrics-server:v0.9.0
  registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.19.1
  prom/prometheus:v3.13.1
  grafana/grafana:13.1.0
  busybox:1.37.0
)
TMP_DIR=$(mktemp -d)
SAMPLES_FILE="$TMP_DIR/full-load-samples.tsv"
: >"$SAMPLES_FILE"
PROMETHEUS_PORT_FORWARD_PID=''
GRAFANA_PORT_FORWARD_PID=''
CLUSTER_OWNED=0
CLEANUP_FAILED=0
LIFECYCLE_PROVEN=0

kubectl_kind() {
  kubectl --context "$KUBE_CONTEXT" "$@"
}

cleanup_failure() {
  printf 'CLEANUP FAILED: %s\n' "$1" >&2
  CLEANUP_FAILED=1
}

# Diagnostics collect both autoscaling control-plane state and load-generator state.
diagnostics() {
  printf '%s\n' '--- Kubernetes HPA diagnostics ---' >&2
  kubectl_kind get horizontalpodautoscaler/autoscale-target --namespace "$NAMESPACE" -o wide >&2 || true
  kubectl_kind describe horizontalpodautoscaler/autoscale-target --namespace "$NAMESPACE" >&2 || true
  kubectl_kind get deployments,pods --namespace "$NAMESPACE" -o wide >&2 || true
  kubectl_kind top pods --namespace "$NAMESPACE" >&2 || true
  kubectl_kind get events --namespace "$NAMESPACE" --sort-by=.lastTimestamp >&2 || true
  kubectl_kind get apiservice v1beta1.metrics.k8s.io -o wide >&2 || true
  kubectl_kind logs --namespace kube-system deployment/metrics-server --tail=100 >&2 || true
  kubectl_kind logs --namespace "$NAMESPACE" deployment/kube-state-metrics --tail=100 >&2 || true
  kubectl_kind logs --namespace "$NAMESPACE" job/load-tester-orchestrator --all-containers >&2 || true
}

cluster_exists() {
  "$KIND_BIN" get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"
}

cleanup() {
  local exit_status=$?
  trap - EXIT
  set +e

  for pid in "$PROMETHEUS_PORT_FORWARD_PID" "$GRAFANA_PORT_FORWARD_PID"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done

  if (( exit_status != 0 )) && (( CLUSTER_OWNED )) && cluster_exists; then
    diagnostics
  fi

  if (( CLUSTER_OWNED )); then
    if [[ "$KEEP_KIND_CLUSTER" == 1 ]]; then
      printf 'Cluster preserved for debugging: %s (context %s)\n' "$CLUSTER_NAME" "$KUBE_CONTEXT"
    else
      "$KIND_BIN" delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || cleanup_failure "could not delete kind cluster $CLUSTER_NAME"
      if cluster_exists; then
        cleanup_failure "kind cluster $CLUSTER_NAME still exists"
      elif (( LIFECYCLE_PROVEN )); then
        printf '%s\n' 'Cleanup: PASS (workers=0 target=1 cluster=deleted)'
      else
        printf '%s\n' 'Cluster cleanup: PASS (cluster=deleted)'
      fi
    fi
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

wait_for_metrics_api() {
  kubectl_kind wait --for=condition=Available apiservice/v1beta1.metrics.k8s.io --timeout "${TIMEOUT_SECONDS}s" >/dev/null
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  until kubectl_kind get --raw /apis/metrics.k8s.io/v1beta1 >/dev/null 2>&1 && \
    kubectl_kind top pods --namespace "$NAMESPACE" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      printf '%s\n' 'Metrics API or kubectl top pods did not become ready' >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_replica_count() {
  local deployment=$1
  local want=$2
  local timeout=$3
  local deadline=$((SECONDS + timeout))
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

wait_for_workers_zero() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS))
  local desired=''
  local remaining=''
  while true; do
    desired=$(kubectl_kind get deployment/load-tester-worker --namespace "$NAMESPACE" -o jsonpath='{.spec.replicas}')
    remaining=$(kubectl_kind get pods --namespace "$NAMESPACE" --selector app.kubernetes.io/name=load-tester-worker --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$desired" == 0 && "$remaining" == 0 ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      printf 'Worker cleanup timed out: desired=%s remaining=%s\n' "$desired" "$remaining" >&2
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
if cluster_exists; then
  printf 'Refusing to reuse existing kind cluster: %s\n' "$CLUSTER_NAME" >&2
  exit 1
fi
CLUSTER_OWNED=1
"$KIND_BIN" create cluster --name "$CLUSTER_NAME" --image "$NODE_IMAGE" --wait "${TIMEOUT_SECONDS}s"
"$KIND_BIN" load docker-image --name "$CLUSTER_NAME" "$ORCHESTRATOR_IMAGE" "$WORKER_IMAGE" "$TARGET_IMAGE"

# Pull sequentially inside the node to avoid competing registry downloads.
printf '%s\n' 'Caching external Kubernetes images in the kind node...'
for image in "${EXTERNAL_IMAGES[@]}"; do
  docker exec "$CLUSTER_NAME-control-plane" crictl pull "$image" >/dev/null
done

kubectl_kind apply -f "$REPO_ROOT/deploy/kubernetes/base/namespace.yaml" >/dev/null
kubectl_kind create configmap load-test-config \
  --namespace "$NAMESPACE" \
  --from-file="demo.yaml=$REPO_ROOT/deploy/kubernetes/target-hpa/demo.yaml" \
  --dry-run=client \
  -o yaml >"$TMP_DIR/load-test-config.yaml"
kubectl_kind apply -f "$TMP_DIR/load-test-config.yaml" >/dev/null
kubectl_kind apply -k "$REPO_ROOT/deploy/kubernetes/overlays/kind-hpa" >/dev/null

kubectl_kind rollout status deployment/metrics-server --namespace kube-system --timeout "${TIMEOUT_SECONDS}s"
for deployment in autoscale-target kube-state-metrics prometheus grafana; do
  kubectl_kind rollout status "deployment/$deployment" --namespace "$NAMESPACE" --timeout "${TIMEOUT_SECONDS}s"
done
wait_for_metrics_api
wait_for_replica_count autoscale-target 1 "$TIMEOUT_SECONDS"
printf '%s\n' 'Metrics API: PASS (APIService available, kubectl top pods works, target starts at 1)'

PROMETHEUS_PORT=$(reserve_port)
GRAFANA_PORT=$(reserve_port)
kubectl_kind port-forward --address 127.0.0.1 --namespace "$NAMESPACE" service/prometheus "$PROMETHEUS_PORT:9090" >"$TMP_DIR/prometheus-port-forward.log" 2>&1 &
PROMETHEUS_PORT_FORWARD_PID=$!
kubectl_kind port-forward --address 127.0.0.1 --namespace "$NAMESPACE" service/grafana "$GRAFANA_PORT:3000" >"$TMP_DIR/grafana-port-forward.log" 2>&1 &
GRAFANA_PORT_FORWARD_PID=$!
wait_port_forward "http://127.0.0.1:$PROMETHEUS_PORT/-/ready" "$PROMETHEUS_PORT_FORWARD_PID" "$TMP_DIR/prometheus-port-forward.log"
wait_port_forward "http://127.0.0.1:$GRAFANA_PORT/api/health" "$GRAFANA_PORT_FORWARD_PID" "$TMP_DIR/grafana-port-forward.log"

dashboard_search=$(curl --silent --fail --user admin:admin "http://127.0.0.1:$GRAFANA_PORT/api/search?query=Target%20Autoscaling")
if [[ "$dashboard_search" != *'"uid":"target-autoscaling"'* || "$dashboard_search" != *'"title":"Target Autoscaling"'* ]]; then
  printf 'Grafana Target Autoscaling dashboard was not provisioned: %s\n' "$dashboard_search" >&2
  exit 1
fi

deadline=$((SECONDS + TIMEOUT_SECONDS))
until [[ "$(prometheus_value 'up{job="kube-state-metrics"}')" == 1 ]]; do
  if (( SECONDS >= deadline )); then
    printf '%s\n' 'Prometheus did not scrape kube-state-metrics before timeout' >&2
    exit 1
  fi
  sleep 2
done
printf '%s\n' 'Observability: PASS (kube-state-metrics scraped, Target Autoscaling dashboard loaded)'

kubectl_kind patch job/load-tester-orchestrator --namespace "$NAMESPACE" --type merge --patch '{"spec":{"suspend":false}}' >/dev/null

deadline=$((SECONDS + TIMEOUT_SECONDS))
max_replicas=1
max_worker_desired=0
max_worker_ready=0
peak_saturated_p95=0
capacity_limited_since=0
max_capacity_limited_seconds=0
last_sample_seconds=0
while true; do
  current=$(kubectl_kind get deployment/autoscale-target --namespace "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
  current=${current:-0}
  if (( current > max_replicas )); then
    max_replicas=$current
  fi

  worker_desired=$(kubectl_kind get deployment/load-tester-worker --namespace "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)
  worker_ready=$(kubectl_kind get deployment/load-tester-worker --namespace "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
  worker_desired=${worker_desired:-0}
  worker_ready=${worker_ready:-0}
  if (( worker_desired > max_worker_desired )); then
    max_worker_desired=$worker_desired
  fi
  if (( worker_ready > max_worker_ready )); then
    max_worker_ready=$worker_ready
  fi

  p95=$(prometheus_value 'histogram_quantile(0.95, sum(rate(loadtester_request_duration_seconds_bucket[15s])) by (le))')
  rps=$(prometheus_value 'sum(rate(loadtester_requests_total[15s]))')
  if [[ -n "$p95" ]]; then
    if (( current < MIN_TARGET_REPLICAS )) && awk -v value="$p95" -v peak="$peak_saturated_p95" 'BEGIN { exit !(value > peak) }'; then
      peak_saturated_p95=$p95
    fi
    if (( current >= MIN_TARGET_REPLICAS && SECONDS - last_sample_seconds >= 5 )) && [[ -n "$rps" ]]; then
      printf '%s\t%s\n' "$p95" "$rps" >>"$SAMPLES_FILE"
      last_sample_seconds=$SECONDS
    fi
  fi

  scaling_limited=$(kubectl_kind get horizontalpodautoscaler/autoscale-target --namespace "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="ScalingLimited")].status}' 2>/dev/null || true)
  desired=$(kubectl_kind get horizontalpodautoscaler/autoscale-target --namespace "$NAMESPACE" -o jsonpath='{.status.desiredReplicas}' 2>/dev/null || true)
  if (( current >= MAX_TARGET_REPLICAS )) || [[ "$desired" == "$MAX_TARGET_REPLICAS" && "$scaling_limited" == True ]]; then
    if (( capacity_limited_since == 0 )); then
      capacity_limited_since=$SECONDS
    fi
    capacity_limited_seconds=$((SECONDS - capacity_limited_since))
    if (( capacity_limited_seconds > max_capacity_limited_seconds )); then
      max_capacity_limited_seconds=$capacity_limited_seconds
    fi
  else
    capacity_limited_since=0
  fi

  succeeded=$(kubectl_kind get job/load-tester-orchestrator --namespace "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
  failed=$(kubectl_kind get job/load-tester-orchestrator --namespace "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null || true)
  if [[ "$succeeded" == 1 || "$failed" == 1 ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    printf '%s\n' 'Target autoscaling proof timed out' >&2
    exit 1
  fi
  sleep 2
done

orchestrator_pod=$(kubectl_kind get pods --namespace "$NAMESPACE" --selector job-name=load-tester-orchestrator -o jsonpath='{.items[0].metadata.name}')
orchestrator_exit=$(kubectl_kind get pod "$orchestrator_pod" --namespace "$NAMESPACE" -o jsonpath='{.status.containerStatuses[?(@.name=="orchestrator")].state.terminated.exitCode}')
orchestrator_log=$(kubectl_kind logs "$orchestrator_pod" --namespace "$NAMESPACE" --container orchestrator)
if [[ "$orchestrator_exit" != 0 || "$orchestrator_log" != *'Result: PASS'* || "$orchestrator_log" != *'Unmet demand: 0'* || "$orchestrator_log" != *'Dropped samples: 0'* ]]; then
  printf 'Load-test integrity failed (exit=%s):\n%s\n' "$orchestrator_exit" "$orchestrator_log" >&2
  exit 1
fi
if (( max_replicas < MIN_TARGET_REPLICAS )); then
  printf 'Target only reached %d replicas; want at least %d\n' "$max_replicas" "$MIN_TARGET_REPLICAS" >&2
  exit 1
fi
if (( max_worker_desired != 2 || max_worker_ready != 2 )); then
  printf 'Fixed worker proof failed: max_desired=%d max_ready=%d want exactly 2\n' "$max_worker_desired" "$max_worker_ready" >&2
  exit 1
fi
if (( max_capacity_limited_seconds >= MAX_CAPACITY_LIMITED_SECONDS )); then
  printf 'HPA remained capacity-limited at %d replicas for %ds\n' "$MAX_TARGET_REPLICAS" "$max_capacity_limited_seconds" >&2
  exit 1
fi

read -r final_p95 final_rps sample_count < <(python3 -c 'import statistics,sys
rows=[]
with open(sys.argv[1], encoding="utf-8") as stream:
 for line in stream:
  fields=line.split()
  if len(fields)==2:
   rows.append((float(fields[0]), float(fields[1])))
tail=rows[-6:]
if not tail:
 print("0 0 0")
else:
 print(statistics.median(x[0] for x in tail), statistics.median(x[1] for x in tail), len(tail))' "$SAMPLES_FILE")

if (( sample_count < 3 )); then
  printf 'Only %d full-load samples were collected after scale-up; want at least 3\n' "$sample_count" >&2
  exit 1
fi
if ! awk -v rps="$final_rps" -v expected="$EXPECTED_RPS" -v ratio="$MIN_THROUGHPUT_RATIO" 'BEGIN { exit !(rps >= expected * ratio) }'; then
  printf 'Final full-load throughput %.2f RPS is below %.0f%% of %d RPS\n' "$final_rps" "$(awk -v ratio="$MIN_THROUGHPUT_RATIO" 'BEGIN { print ratio * 100 }')" "$EXPECTED_RPS" >&2
  exit 1
fi
if ! awk -v final="$final_p95" -v peak="$peak_saturated_p95" -v improvement="$MIN_P95_IMPROVEMENT" 'BEGIN { exit !(peak > 0 && final <= peak * (1 - improvement)) }'; then
  printf 'Latency recovery insufficient: saturated_peak_p95=%ss final_full_load_p95=%ss required_improvement=%.0f%%\n' \
    "$peak_saturated_p95" "$final_p95" "$(awk -v improvement="$MIN_P95_IMPROVEMENT" 'BEGIN { print improvement * 100 }')" >&2
  exit 1
fi
printf 'Target autoscaling: PASS (max_replicas=%d final_rps=%.2f saturated_peak_p95=%ss final_p95=%ss)\n' \
  "$max_replicas" "$final_rps" "$peak_saturated_p95" "$final_p95"

wait_for_workers_zero
wait_for_replica_count autoscale-target 1 "$SCALE_DOWN_TIMEOUT_SECONDS"
LIFECYCLE_PROVEN=1
printf '%s\n' 'Scale-down: PASS (workers=0 target=1)'
