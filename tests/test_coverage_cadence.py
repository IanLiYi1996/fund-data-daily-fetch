"""Pin the cadence-scoped coverage metric to the false alarm that motivated it.

On 2026-08-11 the heartbeat fired: "coverage 95.8% below 97.0% — 1,136 active
funds missing data", while upstream reconciliation simultaneously reported
164/164 agreement. Both could not be right.

Measurement settled it. Of the 1,762 codes absent on the target day:

    disclosure rate over 30 trading days     codes
    <= 20%  (low frequency / stopped)          675
    20-60%  (weekly publishers)              1,035
    60-90%                                      47
    >  90%  (genuinely daily)                    5

and upstream had no value for the sampled ones either (0 of 8). The alert
asked ~1,700 funds to do something they never do — unactionable by
construction, the same class of defect as reconcile's wall-clock window.

Scoping the denominator to daily-cadence funds takes that day from 95.8% to
99.85%. The metric then measured 99.81-99.93% across 12 trading days (median
99.86%, no weekday effect), which is tight enough to alarm at 99% — and that
catches a ~200-fund regression, where the old 97% floor on the noisy
denominator needed 2,000.

Pure arithmetic over measured numbers; no AWS. Values must match
lambda/freshness-check/handler.py.
"""
import pytest

MIN_COVERAGE_PCT = 99.0
DAILY_CADENCE_MIN_PCT = 80.0

# Measured 2026-08-11 on the settled target day 2026-08-10.
DAILY_POPULATION = 21_540
BASELINE_MISSING = 32


def coverage_pct(missing: int, population: int = DAILY_POPULATION) -> float:
    return 100.0 * (population - missing) / population


def alarms(missing: int, population: int = DAILY_POPULATION) -> bool:
    return coverage_pct(missing, population) < MIN_COVERAGE_PCT


def is_daily_cadence(days_seen: int, trading_days: int) -> bool:
    return days_seen >= DAILY_CADENCE_MIN_PCT / 100.0 * trading_days


# --- the false alarm must not recur ---


def test_the_2026_08_11_false_alarm_now_passes():
    assert coverage_pct(BASELINE_MISSING) == pytest.approx(99.85, abs=0.01)
    assert not alarms(BASELINE_MISSING)


def test_old_denominator_would_still_have_alarmed():
    """Pins WHY the denominator changed, not just that it did.

    Same day, whole-universe denominator: 1,136 of 26,768 missing = 95.8%.
    """
    assert alarms(1_136, population=26_768)
    assert coverage_pct(1_136, 26_768) == pytest.approx(95.76, abs=0.05)


# --- cadence classification ---


@pytest.mark.parametrize("days_seen,expected", [
    (26, True),    # every day
    (21, True),    # 80.8% — missed a week's worth, still daily
    (20, False),   # 76.9%
    (13, False),   # weekly publisher: measured 20-60% band
    (5, False),    # low frequency / stopped
])
def test_cadence_threshold(days_seen, expected):
    assert is_daily_cadence(days_seen, trading_days=26) is expected


def test_weekly_publishers_are_excluded_from_scoring():
    """A Friday-only bond fund is healthy but absent 4 days in 5.

    Scoring it daily is what produced 1,035 phantom "missing" funds.
    """
    # ~1 day in 5 over 26 trading days
    assert not is_daily_cadence(5, trading_days=26)


# --- sensitivity: the check must still catch real regressions ---


@pytest.mark.parametrize("dropped,should_alarm", [
    (100, False),   # 99.39% — inside the observed 99.81-99.93% noise band
    (200, True),    # 98.92%
    (500, True),    # 97.53%
    (2_000, True),  # 90.57%
])
def test_injection_sensitivity(dropped, should_alarm):
    assert alarms(BASELINE_MISSING + dropped) is should_alarm


def test_more_sensitive_than_the_old_floor():
    """The old 97% floor on the whole universe needed a 2,000-fund loss.

    This is the guarantee the rescope had to preserve while removing noise:
    tightening the denominator must not cost sensitivity.
    """
    old_threshold, old_population = 97.0, 26_768
    lost = 500
    old_pct = 100.0 * (old_population - lost) / old_population
    assert old_pct >= old_threshold, "old check tolerated a 500-fund loss"
    assert alarms(BASELINE_MISSING + lost), "new check must catch it"


def test_threshold_leaves_headroom_over_observed_minimum():
    """Observed range was 99.81-99.93% over 12 trading days."""
    assert MIN_COVERAGE_PCT < 99.81
    assert 99.81 - MIN_COVERAGE_PCT < 1.0, "headroom this wide dulls the check"
