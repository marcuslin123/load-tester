#!/usr/bin/env bash
set -euo pipefail

require_command() {
  local command_name=$1

  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'Required command is missing: %s\n' "$command_name" >&2
    exit 1
  fi
}

require_command docker
require_command curl
require_command jq

if ! docker compose version >/dev/null 2>&1; then
  printf '%s\n' 'Docker Compose is unavailable' >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'Docker is installed but the daemon is unavailable' >&2
  exit 1
fi

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
COMPOSE_PROJECT_NAME="loadtester-smoke-$(date +%s)-$$-$RANDOM"
export COMPOSE_PROJECT_NAME
COMPOSE=(docker compose --project-name "$COMPOSE_PROJECT_NAME" --file "$REPO_ROOT/compose.yaml")
PROMETHEUS_URL=${PROMETHEUS_URL:-http://localhost:9090}
GRAFANA_URL=${GRAFANA_URL:-http://localhost:3000}
GRAFANA_USER=${GRAFANA_USER:-admin}
GRAFANA_PASSWORD=${GRAFANA_PASSWORD:-admin}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-240}
STACK_STARTED=0
CLEANED_UP=0

READY_RESPONSE='not queried'
TARGETS_RESPONSE='not queried'
REQUESTS_RESPONSE='not queried'
ACTIVE_WORKERS_RESPONSE='not queried'
WORKER_A_RESPONSE='not queried'
WORKER_B_RESPONSE='not queried'
GRAFANA_HEALTH_RESPONSE='not queried'
GRAFANA_DATASOURCE_RESPONSE='not queried'
GRAFANA_DASHBOARD_RESPONSE='not queried'
GRAFANA_RPS_RESPONSE='not queried'
GRAFANA_LATENCY_RESPONSE='not queried'
GRAFANA_ERROR_RESPONSE='not queried'
GRAFANA_STATUS_RESPONSE='not queried'
GRAFANA_WORKER_RESPONSE='not queried'

print_diagnostics() {
  printf '\nCompose smoke diagnostics (project=%s)\n' "$COMPOSE_PROJECT_NAME" >&2
  printf '%s\n' '--- docker compose ps ---' >&2
  "${COMPOSE[@]}" ps >&2 || true
  printf '%s\n' '--- Prometheus /-/ready ---' >&2
  printf '%s\n' "$READY_RESPONSE" >&2
  printf '%s\n' '--- Prometheus /api/v1/targets ---' >&2
  printf '%s\n' "$TARGETS_RESPONSE" >&2
  printf '%s\n' '--- Prometheus query: loadtester_requests_total > 0 ---' >&2
  printf '%s\n' "$REQUESTS_RESPONSE" >&2
  printf '%s\n' '--- Prometheus query: loadtester_active_workers == 2 ---' >&2
  printf '%s\n' "$ACTIVE_WORKERS_RESPONSE" >&2
  printf '%s\n' '--- Prometheus query: worker-a requests > 0 ---' >&2
  printf '%s\n' "$WORKER_A_RESPONSE" >&2
  printf '%s\n' '--- Prometheus query: worker-b requests > 0 ---' >&2
  printf '%s\n' "$WORKER_B_RESPONSE" >&2
  printf '%s\n' '--- Grafana /api/health ---' >&2
  printf '%s\n' "$GRAFANA_HEALTH_RESPONSE" >&2
  printf '%s\n' '--- Grafana Prometheus datasource health ---' >&2
  printf '%s\n' "$GRAFANA_DATASOURCE_RESPONSE" >&2
  printf '%s\n' '--- Grafana provisioned dashboard ---' >&2
  printf '%s\n' "$GRAFANA_DASHBOARD_RESPONSE" >&2
  printf '%s\n' '--- Grafana proxy query: requests per second ---' >&2
  printf '%s\n' "$GRAFANA_RPS_RESPONSE" >&2
  printf '%s\n' '--- Grafana proxy query: p99 latency ---' >&2
  printf '%s\n' "$GRAFANA_LATENCY_RESPONSE" >&2
  printf '%s\n' '--- Grafana proxy query: error rate by kind ---' >&2
  printf '%s\n' "$GRAFANA_ERROR_RESPONSE" >&2
  printf '%s\n' '--- Grafana proxy query: status codes ---' >&2
  printf '%s\n' "$GRAFANA_STATUS_RESPONSE" >&2
  printf '%s\n' '--- Grafana proxy query: per-worker RPS ---' >&2
  printf '%s\n' "$GRAFANA_WORKER_RESPONSE" >&2
  printf '%s\n' '--- docker compose logs (last 200 lines) ---' >&2
  "${COMPOSE[@]}" logs --no-color --tail 200 >&2 || true
}

cleanup() {
  local exit_status=$?
  local cleanup_status=0

  if (( CLEANED_UP )); then
    return
  fi
  CLEANED_UP=1
  trap - EXIT
  set +e

  if (( exit_status != 0 && STACK_STARTED )); then
    print_diagnostics
  fi

  "${COMPOSE[@]}" down --volumes --remove-orphans
  cleanup_status=$?
  if (( cleanup_status != 0 )); then
    printf 'Cleanup failed for Compose project %s\n' "$COMPOSE_PROJECT_NAME" >&2
    exit_status=1
  else
    printf 'Cleanup: PASS (project=%s volumes=removed orphans=removed)\n' "$COMPOSE_PROJECT_NAME"
  fi

  exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

service_status() {
  local service=$1
  local container_id

  container_id=$("${COMPOSE[@]}" ps --quiet "$service" 2>/dev/null || true)
  if [[ -z "$container_id" ]]; then
    printf 'missing'
    return
  fi
  docker container inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
    "$container_id" 2>/dev/null || printf 'missing'
}

curl_snapshot() {
  curl --silent --show-error --fail --max-time 2 "$@" 2>&1 || true
}

query_prometheus() {
  local expression=$1

  curl_snapshot --get \
    --data-urlencode "query=$expression" \
    "$PROMETHEUS_URL/api/v1/query"
}

query_grafana_prometheus() {
  local expression=$1

  curl_snapshot --user "$GRAFANA_USER:$GRAFANA_PASSWORD" --get \
    --data-urlencode "query=$expression" \
    "$GRAFANA_URL/api/datasources/proxy/uid/prometheus/api/v1/query"
}

query_has_result() {
  local response=$1

  printf '%s\n' "$response" | jq -e '
    .status == "success" and
    .data.resultType == "vector" and
    (.data.result | type) == "array" and
    (.data.result | length) > 0
  ' >/dev/null 2>&1
}

query_has_positive_finite_result() {
  local response=$1

  printf '%s\n' "$response" | jq -e '
    .status == "success" and
    .data.resultType == "vector" and
    any(.data.result[]?;
      (.value[1] != "NaN") and
      (.value[1] != "+Inf") and
      (.value[1] != "Inf") and
      (.value[1] != "-Inf") and
      ((.value[1] | tonumber) > 0)
    )
  ' >/dev/null 2>&1
}

query_has_positive_label() {
  local response=$1
  local label=$2
  local value=$3

  printf '%s\n' "$response" | jq -e --arg label "$label" --arg value "$value" '
    .status == "success" and
    .data.resultType == "vector" and
    any(.data.result[]?;
      (.metric[$label] == $value) and
      (.value[1] != "NaN") and
      (.value[1] != "+Inf") and
      (.value[1] != "Inf") and
      (.value[1] != "-Inf") and
      ((.value[1] | tonumber) > 0)
    )
  ' >/dev/null 2>&1
}

target_is_up() {
  local response=$1

  printf '%s\n' "$response" | jq -e '
    .status == "success" and
    (.data.activeTargets | type) == "array" and
    any(.data.activeTargets[]?;
      .scrapeUrl == "http://orchestrator:9091/metrics" and
      .health == "up"
    )
  ' >/dev/null 2>&1
}

grafana_is_ready() {
  local response=$1

  printf '%s\n' "$response" | jq -e '
    .database == "ok" and
    .version == "13.1.0"
  ' >/dev/null 2>&1
}

grafana_datasource_is_healthy() {
  local response=$1

  printf '%s\n' "$response" | jq -e '
    .status == "OK"
  ' >/dev/null 2>&1
}

grafana_dashboard_is_provisioned() {
  local response=$1

  printf '%s\n' "$response" | jq -e '
    .dashboard.uid == "load-tester-overview" and
    .dashboard.title == "Load Tester Overview" and
    (.dashboard.panels | length) == 12 and
    .meta.provisioned == true
  ' >/dev/null 2>&1
}

cd "$REPO_ROOT"
printf 'Compose project: %s\n' "$COMPOSE_PROJECT_NAME"
STACK_STARTED=1
"${COMPOSE[@]}" up --build --detach

deadline=$((SECONDS + TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  target_status=$(service_status target)
  orchestrator_status=$(service_status orchestrator)
  worker_a_status=$(service_status worker-a)
  worker_b_status=$(service_status worker-b)
  prometheus_status=$(service_status prometheus)
  grafana_status=$(service_status grafana)

  for service_state in \
    "$target_status" "$orchestrator_status" "$worker_a_status" \
    "$worker_b_status" "$prometheus_status" "$grafana_status"; do
    if [[ "$service_state" == exited || "$service_state" == dead ]]; then
      printf 'A Compose service exited before the required metrics were proven\n' >&2
      exit 1
    fi
  done

  containers_ready=0
  if [[ "$target_status" == healthy && "$orchestrator_status" == healthy && \
        "$worker_a_status" == running && "$worker_b_status" == running && \
        "$prometheus_status" == healthy && "$grafana_status" == healthy ]]; then
    containers_ready=1
  fi

  READY_RESPONSE=$(curl_snapshot "$PROMETHEUS_URL/-/ready")
  TARGETS_RESPONSE=$(curl_snapshot "$PROMETHEUS_URL/api/v1/targets")
  REQUESTS_RESPONSE=$(query_prometheus 'loadtester_requests_total > 0')
  ACTIVE_WORKERS_RESPONSE=$(query_prometheus 'loadtester_active_workers == 2')
  WORKER_A_RESPONSE=$(query_prometheus 'loadtester_worker_requests_total{worker_id="worker-a"} > 0')
  WORKER_B_RESPONSE=$(query_prometheus 'loadtester_worker_requests_total{worker_id="worker-b"} > 0')
  GRAFANA_HEALTH_RESPONSE=$(curl_snapshot "$GRAFANA_URL/api/health")
  GRAFANA_DATASOURCE_RESPONSE=$(curl_snapshot --user "$GRAFANA_USER:$GRAFANA_PASSWORD" \
    "$GRAFANA_URL/api/datasources/uid/prometheus/health")
  GRAFANA_DASHBOARD_RESPONSE=$(curl_snapshot --user "$GRAFANA_USER:$GRAFANA_PASSWORD" \
    "$GRAFANA_URL/api/dashboards/uid/load-tester-overview")
  GRAFANA_RPS_RESPONSE=$(query_grafana_prometheus 'rate(loadtester_requests_total[10s])')
  GRAFANA_LATENCY_RESPONSE=$(query_grafana_prometheus \
    'histogram_quantile(0.99, sum by (le) (rate(loadtester_request_duration_seconds_bucket[10s])))')
  GRAFANA_ERROR_RESPONSE=$(query_grafana_prometheus \
    'sum by (kind) (rate(loadtester_request_failures_total[10s])) / ignoring(kind) group_left clamp_min(sum(rate(loadtester_requests_total[10s])), 1e-9)')
  GRAFANA_STATUS_RESPONSE=$(query_grafana_prometheus \
    'sum by (code) (rate(loadtester_http_responses_total[10s]))')
  GRAFANA_WORKER_RESPONSE=$(query_grafana_prometheus \
    'sum by (worker_id) (rate(loadtester_worker_requests_total[10s]))')

  if (( containers_ready )) && \
     [[ "$READY_RESPONSE" == 'Prometheus Server is Ready.' ]] && \
     target_is_up "$TARGETS_RESPONSE" && \
     query_has_result "$REQUESTS_RESPONSE" && \
     query_has_result "$ACTIVE_WORKERS_RESPONSE" && \
     query_has_result "$WORKER_A_RESPONSE" && \
     query_has_result "$WORKER_B_RESPONSE" && \
     grafana_is_ready "$GRAFANA_HEALTH_RESPONSE" && \
     grafana_datasource_is_healthy "$GRAFANA_DATASOURCE_RESPONSE" && \
     grafana_dashboard_is_provisioned "$GRAFANA_DASHBOARD_RESPONSE" && \
     query_has_positive_finite_result "$GRAFANA_RPS_RESPONSE" && \
     query_has_positive_finite_result "$GRAFANA_LATENCY_RESPONSE" && \
     query_has_positive_label "$GRAFANA_ERROR_RESPONSE" kind server && \
     query_has_positive_label "$GRAFANA_STATUS_RESPONSE" code 200 && \
     query_has_positive_label "$GRAFANA_WORKER_RESPONSE" worker_id worker-a && \
     query_has_positive_label "$GRAFANA_WORKER_RESPONSE" worker_id worker-b; then
    printf 'Compose containers: PASS (target=%s orchestrator=%s worker-a=%s worker-b=%s prometheus=%s grafana=%s)\n' \
      "$target_status" "$orchestrator_status" "$worker_a_status" "$worker_b_status" \
      "$prometheus_status" "$grafana_status"
    printf 'Prometheus ready: PASS (%s)\n' "$READY_RESPONSE"
    printf 'Prometheus target: PASS (scrape_url=http://orchestrator:9091/metrics health=up)\n'
    printf 'Prometheus requests: PASS (%s)\n' "$REQUESTS_RESPONSE"
    printf 'Prometheus active workers: PASS (%s)\n' "$ACTIVE_WORKERS_RESPONSE"
    printf 'Prometheus worker-a series: PASS (%s)\n' "$WORKER_A_RESPONSE"
    printf 'Prometheus worker-b series: PASS (%s)\n' "$WORKER_B_RESPONSE"
    printf 'Grafana health: PASS (%s)\n' "$GRAFANA_HEALTH_RESPONSE"
    printf 'Grafana datasource: PASS (%s)\n' "$GRAFANA_DATASOURCE_RESPONSE"
    printf 'Grafana dashboard: PASS (uid=load-tester-overview panels=12 provisioned=true)\n'
    printf 'Grafana dashboard queries: PASS (rps=p99=positive server-error=positive status-200=positive worker-a=positive worker-b=positive)\n'
    exit 0
  fi

  sleep 0.5
done

printf 'Timed out after %ss waiting for Compose and Prometheus evidence\n' "$TIMEOUT_SECONDS" >&2
exit 1
