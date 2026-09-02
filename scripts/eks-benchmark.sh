#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib/eks-benchmark.sh
source "$SCRIPT_DIR/lib/eks-benchmark.sh"

if [[ "${EKS_BENCHMARK_LIBRARY_ONLY:-0}" == "1" ]]; then
  return 0 2>/dev/null || exit 0
fi

command_name=${1:-}
shift || true

case "$command_name" in
  preflight)
    [[ $# -eq 0 ]] || die "preflight does not accept arguments"
    preflight
    ;;
  ecr-push)
    [[ $# -eq 0 ]] || die "ecr-push does not accept arguments"
    ecr_push
    ;;
  render)
    [[ $# -eq 0 ]] || die "render does not accept arguments"
    render
    ;;
  run)
    [[ $# -eq 0 ]] || die "run does not accept arguments"
    run_benchmark
    ;;
  teardown)
    [[ $# -eq 0 ]] || die "teardown does not accept arguments"
    teardown
    ;;
  recover-teardown)
    recover_teardown "$@"
    ;;
  verify-cleanup)
    [[ $# -eq 0 ]] || die "verify-cleanup does not accept arguments"
    verify_cleanup
    ;;
  acknowledge-billing)
    acknowledge_billing "$@"
    ;;
  *)
    die "unknown command: ${command_name:-<missing>}"
    ;;
esac
