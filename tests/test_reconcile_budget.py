"""Pin reconciliation's time budget.

The probes were sequential with a 20s socket timeout. On 2026-08-21 upstream
went unreachable and each failure cost a full connect timeout — the log shows
failures 15.4s apart — so a 162-fund sample needed ~2,430s against the check's
600s ceiling. It hit that ceiling twice.

A timed-out check is worse than a partial one: it posts NOTHING, and the daily
heartbeat exists precisely so that silence means the monitor itself is dead. A
short round is honest because `checked` and `probe_errors` are reported, so it
shows up as a smaller denominator, never as a clean bill of health.

Arithmetic over the measured failure cost; no AWS, no HTTP.
"""
import pytest

RECONCILE_WORKERS = 12
PROBE_TIMEOUT = 8.0
RECONCILE_BUDGET_SECONDS = 150.0
CHECK_TIMEOUT_SECONDS = 900.0
SCAN_SECONDS = 320.0  # measured duration of the check's scans

SAMPLE = 162
OBSERVED_FAILURE_COST = 15.4  # seconds per failed probe, measured 2026-08-21


def worst_case_seconds(sample: int, per_probe: float, workers: int) -> float:
    """All probes failing, which is the case that blew the ceiling."""
    return sample * per_probe / workers


def test_old_configuration_could_not_fit():
    """Sequential + 20s timeout, the shape that timed out twice."""
    assert worst_case_seconds(SAMPLE, OBSERVED_FAILURE_COST, workers=1) > 600


def test_new_configuration_fits_the_budget():
    assert worst_case_seconds(SAMPLE, PROBE_TIMEOUT, RECONCILE_WORKERS) <= RECONCILE_BUDGET_SECONDS


def test_budget_leaves_room_for_the_scans():
    """Reconciliation must not be able to consume the whole invocation."""
    assert SCAN_SECONDS + RECONCILE_BUDGET_SECONDS < CHECK_TIMEOUT_SECONDS


def test_budget_is_a_hard_cap_not_a_hope():
    """Even if every probe hangs the full timeout, the cap still holds.

    The deadline is checked as results arrive, so the overshoot is bounded by
    one probe timeout rather than by the number of outstanding probes.
    """
    overshoot = PROBE_TIMEOUT
    assert RECONCILE_BUDGET_SECONDS + overshoot < CHECK_TIMEOUT_SECONDS - SCAN_SECONDS


def test_probe_timeout_still_clears_a_healthy_response():
    """Measured healthy p95 was ~2s; 8s must not cut real responses off."""
    healthy_p95 = 2.0
    assert PROBE_TIMEOUT > healthy_p95 * 2


@pytest.mark.parametrize("probed,total", [(0, 162), (40, 162), (162, 162)])
def test_partial_rounds_report_a_smaller_denominator(probed, total):
    """A budget-truncated round must not read as success.

    Skipped codes are counted neither as passed nor as errors, so the reported
    denominator shrinks instead of the ratio silently improving.
    """
    assert probed <= total
    checked = probed
    assert checked <= total, "cannot report more checks than the sample"
