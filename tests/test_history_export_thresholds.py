"""Pin the history-export check to the defects it exists to catch.

Nothing watched the three flat files under ``fund/_history/`` until
2026-08-10, and both defects found that day were invisible for exactly that
reason — every other check stayed green while:

- ``fund_scale_history`` stopped updating after 2026-07-04, so consumers
  screening liquidation risk (net assets < 2亿) read 3/31 figures for over a
  month.
- ``fund_manager_history`` silently shed ~1,000 rate-limited funds on every
  weekly run, because the merge overwrote the canonical file with only what
  that run happened to collect.

A first pass at (40 days, 24,000 funds) would have missed BOTH: scale was 37
days old, and manager still held 26,299 funds. These tests pin the calibrated
values so that can't regress. Pure arithmetic over observed numbers; no AWS.

Thresholds must match ``_HISTORY_FILES`` in lambda/freshness-check/handler.py.
"""
import pytest

# (max age in days, minimum distinct funds)
HISTORY_FILES = {
    "fund_manager_history": (10, 26_500),
    "fund_scale_history": (33, 26_000),
    "fund_portfolio_hold_history": (100, 14_000),
}


def _problems(base: str, n_funds: int, age_days: int) -> list[str]:
    max_age, min_funds = HISTORY_FILES[base]
    found = []
    if age_days > max_age:
        found.append("stale")
    if n_funds < min_funds:
        found.append("partial")
    return found


# --- the two real defects must both alarm ---


def test_scale_stall_alarms():
    """Measured 2026-08-10: last written 2026-07-04, 37 days stale."""
    assert _problems("fund_scale_history", 26_646, 37) == ["stale"]


def test_manager_partial_overwrite_alarms():
    """Measured 2026-08-09: 26,299 funds after 988 were dropped."""
    assert _problems("fund_manager_history", 26_299, 0) == ["partial"]


def test_first_pass_thresholds_would_have_missed_both():
    """Guards the calibration itself, not just the current values.

    If someone loosens these back toward round numbers, this test explains
    why that hides the exact defects the check was built for.
    """
    loose = {"fund_manager_history": (10, 24_000),
             "fund_scale_history": (40, 24_000)}
    for base, n, age in (("fund_scale_history", 26_646, 37),
                         ("fund_manager_history", 26_299, 0)):
        max_age, min_funds = loose[base]
        assert not (age > max_age or n < min_funds), (
            f"{base} passes the loose thresholds — that is the bug"
        )
        assert _problems(base, n, age), f"{base} must fail the tightened ones"


# --- today's healthy state must not alarm ---


@pytest.mark.parametrize("base,n_funds,age_days", [
    ("fund_manager_history", 27_287, 0),
    ("fund_scale_history", 26_984, 0),
    # Holdings cover equity-like funds only: money-market and pure-bond
    # products have no 股票投资明细, so ~15.3k is complete, not partial. A
    # shared 20k floor alarmed on this every day.
    ("fund_portfolio_hold_history", 15_289, 6),
])
def test_healthy_state_is_quiet(base, n_funds, age_days):
    assert _problems(base, n_funds, age_days) == []


def test_portfolio_floor_sits_below_its_real_population():
    """A shared floor would make the quarterly file a permanent false alarm."""
    _, portfolio_floor = HISTORY_FILES["fund_portfolio_hold_history"]
    _, manager_floor = HISTORY_FILES["fund_manager_history"]
    assert portfolio_floor < 15_289 < manager_floor
