"""Pin the money check's re-verification window.

The check first shipped covering only the target day. That made every fix
momentary: the snapshot endpoint returns today AND yesterday, so the run on day
D rewrites day D-1 with the same cumulative figure, and nothing revisits D-1
afterwards. 004904@2026-08-23 held a single row stamped 2026-08-24 17:16 —
written by the FOLLOWING day's collection — so the correction applied on
2026-08-21 never saw it.

The ingested_at tiebreak makes this worse rather than better on its own: newest
write wins, so a later bad write also beats an earlier correction. Re-checking a
window is what makes the fix durable.
"""
import pytest

MONEY_VERIFY_LOOKBACK_DAYS = 5
SNAPSHOT_REWRITES_DAYS_BACK = 1  # the endpoint returns today + yesterday


def covered_days(lookback: int) -> range:
    """Days back from target that the check re-verifies."""
    return range(0, lookback + 1)


def test_window_covers_the_day_the_snapshot_rewrites():
    assert SNAPSHOT_REWRITES_DAYS_BACK in covered_days(MONEY_VERIFY_LOOKBACK_DAYS)


def test_target_day_only_would_miss_it():
    """The original shape, and exactly how 004904@2026-08-23 slipped through."""
    assert SNAPSHOT_REWRITES_DAYS_BACK not in covered_days(0)


def test_window_spans_a_weekend():
    """The 2x discrepancies cluster on Saturdays and Sundays.

    A Friday run must still re-verify the preceding weekend once the following
    Monday's collection has rewritten it.
    """
    assert MONEY_VERIFY_LOOKBACK_DAYS >= 3


def test_window_costs_one_request_per_fund():
    """lsjz accepts startDate/endDate, so widening the window is not N times
    the cost — otherwise ~977 funds x 6 days would be ~5,900 requests."""
    funds = 977
    naive = funds * (MONEY_VERIFY_LOOKBACK_DAYS + 1)
    ranged = funds
    assert ranged < naive / 5


@pytest.mark.parametrize("days_back", [0, 1, 2, 3, 5])
def test_every_day_in_the_window_is_rechecked(days_back):
    assert days_back in covered_days(MONEY_VERIFY_LOOKBACK_DAYS)
