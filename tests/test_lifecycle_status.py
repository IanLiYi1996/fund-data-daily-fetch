"""Pin the fund lifecycle classification consumers screen on.

Consumers asked (2026-08-10) for a field distinguishing 停更 from 已终止,
because ``fund_daily`` alone cannot answer "will this fund report again?" —
and that question decides whether a code stays in a screening universe.

The discriminator is upstream presence, not elapsed time: 天天基金 drops the
NAV series for liquidated and delisted products, while a 3-month-hold FOF can
be legitimately silent for a quarter. Ageing alone would evict live funds.

One tier exists purely because measurement contradicted the first design: a
plain stalled/terminated split labelled 31 codes "may resume" that had been
silent for over a year (max 3,603 days ≈ 10 years), because the per-run probe
cap of 60 meant they were never checked. Telling a consumer to keep those is
worse than useless, but we haven't confirmed them dead either — hence
``presumed_terminated``.
"""
import pytest

STATUS_ACTIVE = "active"
STATUS_STALLED = "stalled"
STATUS_TERMINATED = "terminated"
STATUS_NEVER = "never_reported"
STATUS_PRESUMED_DEAD = "presumed_terminated"

INACTIVE_AFTER_DAYS = 30
PRESUMED_DEAD_AFTER_DAYS = 365


def classify(lag_days, *, probed_dead: bool = False) -> str:
    """Mirrors _status() in lambda/freshness-check/handler.py."""
    if lag_days is None:
        return STATUS_TERMINATED if probed_dead else STATUS_NEVER
    if lag_days <= INACTIVE_AFTER_DAYS:
        return STATUS_ACTIVE
    if probed_dead:
        return STATUS_TERMINATED
    if lag_days > PRESUMED_DEAD_AFTER_DAYS:
        return STATUS_PRESUMED_DEAD
    return STATUS_STALLED


def test_recent_disclosure_is_active():
    assert classify(0) == STATUS_ACTIVE
    assert classify(INACTIVE_AFTER_DAYS) == STATUS_ACTIVE


@pytest.mark.parametrize("lag", [31, 200, 216, 326, 365])
def test_hold_period_gaps_stay_stalled_not_terminated(lag):
    """Real 定开/持有期 lags measured 2026-08-10 (200-326 days).

    These must never be labelled terminated: they are expected to report
    again, and evicting them would silently shrink the consumer's universe.
    """
    assert classify(lag) == STATUS_STALLED


@pytest.mark.parametrize("lag", [366, 1345, 3603])
def test_year_plus_silence_is_not_called_recoverable(lag):
    """Measured: 31 of 52 unprobed silent codes exceeded a year.

    A plain stalled/terminated split called all of them "may resume".
    """
    assert classify(lag) == STATUS_PRESUMED_DEAD


def test_probe_confirming_death_overrides_short_lag():
    """An upstream probe is stronger evidence than elapsed time."""
    assert classify(45, probed_dead=True) == STATUS_TERMINATED


def test_never_reported_vs_terminated_are_distinct():
    """Absence from fund_daily and a confirmed death are different facts."""
    assert classify(None) == STATUS_NEVER
    assert classify(None, probed_dead=True) == STATUS_TERMINATED


def test_no_fund_is_unclassified():
    """Every code gets a verdict — "absent from the file" is not an answer a
    consumer can screen on, which is why the export covers the whole universe.
    """
    valid = {STATUS_ACTIVE, STATUS_STALLED, STATUS_TERMINATED,
             STATUS_NEVER, STATUS_PRESUMED_DEAD}
    for lag in (None, 0, 30, 31, 365, 366, 3603):
        for probed in (True, False):
            assert classify(lag, probed_dead=probed) in valid
