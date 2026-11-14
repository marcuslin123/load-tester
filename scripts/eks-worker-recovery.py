"""Reconcile complete scheduler windows with the worker experiment's final report."""

import json
import re
import sys


WINDOW = re.compile(
    r"autoscale evaluation: expected=(\d+) admitted=(\d+) unmet=(\d+) "
    r"coverage=[\d.]+ complete=(true|false) deficits=\d+ "
    r"desired=(\d+) registered=(\d+) scale=(true|false) reason="
)


def recovery_evidence(log: str) -> dict[str, int]:
    parts = re.split(r"^Test: eks-worker-autoscale\n", log, flags=re.MULTILINE)
    if len(parts) != 2:
        raise ValueError("missing final worker experiment report")
    windows, summary = parts

    def counter(label: str) -> int:
        matches = re.findall(rf"^{label}: (\d+)$", summary, re.MULTILINE)
        if len(matches) != 1:
            raise ValueError(f"expected one final {label} counter")
        return int(matches[0])

    total = counter("Unmet demand")
    if counter("Failed") or counter("Dropped samples"):
        raise ValueError("worker experiment reported failed requests or dropped samples")
    violations = [line for line in summary.splitlines() if line.startswith("- ")]
    results = re.findall(r"^Result: (\w+)$", summary, re.MULTILINE)
    expected_result = ["FAIL"] if total else ["PASS"]
    expected_violations = [f"- unmet demand: {total}"] if total else []
    if results != expected_result or violations != expected_violations:
        raise ValueError("worker experiment has a failure other than unmet demand")

    scaled = recovered = False
    before = after = after_rejections = 0
    for match in WINDOW.finditer(windows):
        expected, admitted, rejected = map(int, match.group(1, 2, 3))
        complete, desired, registered, applied = match.group(4, 5, 6, 7)
        if complete != "true":
            if recovered:
                raise ValueError("incomplete scheduler window after recovery")
            continue
        deficit = expected - admitted
        if scaled and int(registered) >= 2 and registered == desired and deficit <= 0 and rejected == 0:
            recovered = True
        if recovered:
            after += deficit
            after_rejections += rejected
        else:
            before += deficit
        scaled = scaled or applied == "true"

    if recovered and not 0 <= before <= total:
        raise ValueError("pre-recovery scheduler deficit does not reconcile with the final report")
    return {
        "scaled": int(scaled),
        "recovered": int(recovered),
        "missed_arrivals_total": total,
        "missed_arrivals_during_scale_up": before if recovered else 0,
        "unmet_demand": max(total - before, after, after_rejections) if recovered else total,
    }


if __name__ == "__main__":
    try:
        print(json.dumps(recovery_evidence(sys.stdin.read())))
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
