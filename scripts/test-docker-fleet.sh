#!/usr/bin/env bash
set -euo pipefail

IMAGE=${LOAD_TESTER_WORKER_IMAGE:-load-tester-piece16-worker:integration-$$}
BUILT_IMAGE=0

cleanup() {
  if (( BUILT_IMAGE )); then
    docker image rm --force "$IMAGE" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! docker info >/dev/null 2>&1; then
  printf '%s\n' 'Docker is installed but the daemon is unavailable' >&2
  exit 1
fi

if [[ -z "${LOAD_TESTER_WORKER_IMAGE:-}" ]]; then
  docker build \
    --target worker \
    --build-arg VERSION=piece16-integration \
    --tag "$IMAGE" \
    .
  BUILT_IMAGE=1
fi

LOAD_TESTER_DOCKER_INTEGRATION=1 \
LOAD_TESTER_WORKER_IMAGE="$IMAGE" \
go test -count=1 -run '^TestDockerDriverLifecycle$' -v ./internal/fleet/docker
