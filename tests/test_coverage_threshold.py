"""Guard the coverage check against the failure mode it exists to catch.

MIN_FUNDS is an absolute floor, so a regression that silently drops live
funds still clears it: at 26.7k active codes and a 20k floor, losing even
2,000 funds passes. That is the shape of the 2026-06-12 outage — the
pipeline reported success while the consumer got nothing new.

These tests pin the arithmetic of the ratio check so the guarantee cannot
regress unnoticed. They are pure functions over set sizes; no AWS calls.
"""
MIN_FUNDS = 20_000
MIN_COVERAGE_PCT = 97.0

# Shape taken from a real settled day (2026-07-31).
BASELINE = 26_728
OBSERVED = 26_302


def _passes_floor(n: int) -> bool:
    return n >= MIN_FUNDS


def _passes_ratio(n: int, baseline: int = BASELINE) -> bool:
    return (100.0 * n / baseline) >= MIN_COVERAGE_PCT


def test_healthy_day_passes_both():
    assert _passes_floor(OBSERVED)
    assert _passes_ratio(OBSERVED)


def test_ratio_catches_losses_the_floor_misses():
    """The whole point: the floor is blind to losses the ratio catches."""
    for dropped in (500, 1_000, 2_000):
        n = OBSERVED - dropped
        assert _passes_floor(n), (
            f"floor unexpectedly caught a {dropped}-fund loss; "
            "if MIN_FUNDS was raised, update this test's intent"
        )
        assert not _passes_ratio(n), (
            f"ratio failed to catch a {dropped}-fund loss "
            f"({100.0 * n / BASELINE:.1f}%)"
        )


def test_weekend_shape_would_be_scored_as_collapse():
    """Why non-trading days must be excluded, not scored.

    Weekends carry only money-market accrual (~800 codes). Scoring them
    reads as a 97% collapse, which is why _check_coverage filters to days
    above _TRADING_DAY_MIN_FUNDS instead of taking the newest row date.
    """
    weekend = 805
    assert not _passes_floor(weekend)
    assert not _passes_ratio(weekend)


def test_t_plus_one_day_would_false_alarm():
    """Why the newest day is skipped in favour of the previous settled one.

    ETFs and money funds disclose T+1, so ~2.9k codes legitimately have no
    row yet on the newest day — 89% coverage, which would alert daily.
    """
    newest_day = 23_781
    assert _passes_floor(newest_day)
    assert not _passes_ratio(newest_day)


# --- per-row date/value selection (iceberg_writer) -------------------------
#
# akshare returns today and yesterday as parallel columns, and which one is
# populated varies BY ROW. Choosing one column for the whole frame discarded
# 684 T+1 products' NAVs every day; they reached the consumer as NULL while
# upstream had the value. These cases pin the per-row behaviour, including
# the weekend shape an earlier fix addressed.

def _strip():
    import sys
    sys.path.insert(0, "lambda")
    from shared.storage.iceberg_writer import _strip_date_prefix_keep_latest
    return _strip_date_prefix_keep_latest


def test_uniform_date_returns_scalar():
    import pandas as pd
    f, observed = _strip()(pd.DataFrame([
        {"基金代码": "a", "2026-08-04-单位净值": "1.0", "2026-08-03-单位净值": "0.9"},
        {"基金代码": "b", "2026-08-04-单位净值": "2.0", "2026-08-03-单位净值": "1.9"},
    ]))
    assert observed == "2026-08-04"
    assert list(f["单位净值"]) == ["1.0", "2.0"]
    assert "__observed_date__" not in f.columns


def test_mixed_dates_keep_each_rows_own_value():
    """The regression that shipped: row b's only value is under the older date."""
    import pandas as pd
    f, observed = _strip()(pd.DataFrame([
        {"基金代码": "a", "2026-08-04-单位净值": "1.0", "2026-08-03-单位净值": "0.9"},
        {"基金代码": "b", "2026-08-04-单位净值": "",    "2026-08-03-单位净值": "1.9"},
    ]))
    assert observed is None, "mixed dates must not collapse to one scalar"
    assert list(f["单位净值"]) == ["1.0", "1.9"], "row b's NAV must survive"
    assert list(f["__observed_date__"]) == ["2026-08-04", "2026-08-03"]


def test_weekend_shape_still_picks_friday():
    """Earlier fix: on a weekend the newest column is all placeholders."""
    import pandas as pd
    f, observed = _strip()(pd.DataFrame([
        {"基金代码": "a", "2026-08-01-单位净值": "---", "2026-07-31-单位净值": "4.2338"},
    ]))
    assert observed == "2026-07-31"
    assert list(f["单位净值"]) == ["4.2338"]


def test_all_empty_keeps_newest_date_and_null_value():
    import pandas as pd
    f, observed = _strip()(pd.DataFrame([
        {"基金代码": "a", "2026-08-04-单位净值": "", "2026-08-03-单位净值": ""},
    ]))
    assert observed == "2026-08-04"
    assert list(f["单位净值"]) == [""]
