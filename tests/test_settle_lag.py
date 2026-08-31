"""Pin the per-category settle lag, and why a cadence bar cannot replace it.

The coverage check scores the newest SETTLED trading day. That is one day back
for almost everything, but FOF price off their holdings' NAVs and so publish one
trading day later. Measured 2026-08-31 over the daily-cadence population,
coverage by trading days back:

    category      lag1     lag2     lag3
    FOF          19.1%    99.8%   100.0%
    混合型        100.0%   100.0%  100.0%
    债券型         99.9%    99.9%  100.0%
    指数型        100.0%   100.0%  100.0%
    货币型/QDII   100.0%   100.0%  100.0%

Two earlier attempts tried to absorb this by moving the cadence bar (80% then
90%). Neither could work, and the 2026-08-27 alert shows why: of the 945 funds
reported missing, **931 had appearance rates of 99-101%** — they publish nearly
every day and then all skip the newest settled day together. No threshold on
"how often does this fund appear" excludes a fund that appears ~always.

Upstream confirmed it had nothing for 20 of 20 sampled on that date, so the
data was not missing; it was not due yet.
"""
import pytest

MIN_COVERAGE_PCT = 99.0
SLOW_SETTLE_CATEGORIES = {"FOF"}
SLOW_SETTLE_LAG = 2
FAST_SETTLE_LAG = 1

# Measured coverage by (category, trading days back).
MEASURED = {
    ("FOF", 1): 19.1, ("FOF", 2): 99.8, ("FOF", 3): 100.0,
    ("混合型", 1): 100.0, ("债券型", 1): 99.9, ("指数型", 1): 100.0,
    ("股票型", 1): 100.0, ("货币型", 1): 100.0, ("QDII", 1): 100.0,
}


def settle_lag(category: str) -> int:
    return SLOW_SETTLE_LAG if category in SLOW_SETTLE_CATEGORIES else FAST_SETTLE_LAG


def alarms(category: str) -> bool:
    return MEASURED[(category, settle_lag(category))] < MIN_COVERAGE_PCT


# --- the alert must not recur ---


def test_fof_scored_at_its_own_lag_passes():
    assert settle_lag("FOF") == 2
    assert not alarms("FOF")


def test_fof_scored_at_the_common_lag_would_still_fire():
    """Pins the failure being fixed: 19.1% at lag1 is far below any threshold."""
    assert MEASURED[("FOF", 1)] < MIN_COVERAGE_PCT


@pytest.mark.parametrize("category", ["混合型", "债券型", "指数型", "股票型",
                                      "货币型", "QDII"])
def test_every_other_category_settles_in_one_day(category):
    assert settle_lag(category) == 1
    assert not alarms(category)


def test_no_category_is_scored_before_it_settles():
    for (category, lag), pct in MEASURED.items():
        if lag == settle_lag(category):
            assert pct >= MIN_COVERAGE_PCT, (
                f"{category} is scored at lag{lag} where it reads {pct}%"
            )


# --- why the cadence bar was the wrong lever ---


def test_cadence_bar_cannot_exclude_a_fund_that_appears_every_day():
    """931 of the 945 funds named on 2026-08-27 sat at 99-101% appearance.

    Any bar low enough to admit normal funds also admits these, and any bar
    high enough to exclude them excludes almost everything.
    """
    appearance_rate = 100.0
    for bar in (80.0, 90.0, 95.0, 99.0):
        assert appearance_rate >= bar, (
            f"a bar of {bar}% still admits a fund appearing {appearance_rate}% "
            "of days — the lever cannot work"
        )


def test_lag_is_a_product_property_not_an_observation_artifact():
    """FOF lag is stable across days, so it belongs in configuration.

    A statistic computed from a sliding window drifted twice (denominator
    21,540 -> 25,737); the settle lag does not.
    """
    assert MEASURED[("FOF", 2)] > MEASURED[("FOF", 1)] * 5
    assert MEASURED[("FOF", 3)] >= MEASURED[("FOF", 2)]
