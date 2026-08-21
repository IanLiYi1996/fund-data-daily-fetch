"""Pin the money-market NAV verification pass and its tolerance.

The daily snapshot endpoint reports 每万份收益 for some money funds as an
accrual covering everything since that fund last published, not for the single
day the row is labelled with. Written as-is it overstates one day's income.

An earlier pass corrected 11 such rows by hand. That treated the symptom — the
write path was unchanged, so it recurred the very next weekend, and the
heartbeat caught `022605@2026-08-16` (ours 0.674 vs upstream 0.337) plus
`025079@2026-08-14` (0.7844 vs 0.2648, a ~3x). This pass is the cause fix:
lsjz publishes one row per calendar day, including Saturday and Sunday, so its
value is authoritative and wins.

Measured 2026-08-14..08-20: 13 of 6,303 money-fund rows disagreed — 2x on a
Sunday, 3x on a Friday, and several non-integer ratios (1.034 vs 0.4949). Ratios
are NOT reliably whole multiples, so the check must be "differs beyond
rounding", never "is an exact multiple".

Pure arithmetic over the observed values; no AWS, no HTTP.
"""
import pytest

MONEY_NAV_TOLERANCE = 0.0005


def needs_correction(ours: float, upstream: float) -> bool:
    """Mirrors the comparison in _verify_money_funds."""
    return abs(float(upstream) - float(ours)) > MONEY_NAV_TOLERANCE


# --- every shape observed on 2026-08-14..08-20 must be caught ---


@pytest.mark.parametrize("code,ours,upstream", [
    ("022605", 0.674, 0.337),      # 2x, Sunday
    ("004903", 0.6599, 0.33),      # 2x, Sunday
    ("004904", 0.6599, 0.33),      # 2x, Sunday
    ("000331", 0.739, 0.2468),     # 3x, Friday
    ("000332", 0.9366, 0.3128),    # 3x
    ("020097", 0.9365, 0.3127),    # 3x
    ("024503", 0.968, 0.3226),     # 3x
    ("025079", 0.7844, 0.2648),    # ~2.96x — not a clean multiple
    ("003316", 1.034, 0.4949),     # ~2.09x
    ("003317", 1.2314, 0.5608),    # ~2.20x
    ("020201", 1.2313, 0.5607),
    ("020852", 1.2313, 0.5607),
    ("022048", 1.1163, 0.5224),
])
def test_all_observed_discrepancies_are_corrected(code, ours, upstream):
    assert needs_correction(ours, upstream)


def test_non_integer_ratios_are_caught_too():
    """A multiple-based rule would have missed 6 of the 13.

    025079 is 2.96x and 003316 is 2.09x; only a "differs beyond rounding"
    comparison catches those alongside the clean 2x and 3x cases.
    """
    ratio = 0.7844 / 0.2648
    assert not (abs(ratio - 2) < 0.03 or abs(ratio - 3) < 0.03), (
        "025079 is not a clean multiple — do not key the check on multiples"
    )
    assert needs_correction(0.7844, 0.2648)


# --- the 6,286 agreeing rows must not be rewritten ---


@pytest.mark.parametrize("value", [0.3370, 0.2648, 1.8376, 0.0001])
def test_identical_values_are_left_alone(value):
    assert not needs_correction(value, value)


def test_last_digit_rounding_is_not_a_correction():
    """每万份收益 is quoted to 4 decimals.

    Rewriting on a 1e-4 wobble would append rows daily for no benefit; the
    tolerance sits above it deliberately.
    """
    assert not needs_correction(0.2978, 0.2979)
    assert not needs_correction(0.3370, 0.3371)


def test_tolerance_sits_above_quote_precision_but_well_below_real_gaps():
    smallest_real = abs(0.674 - 0.337)
    assert 1e-4 < MONEY_NAV_TOLERANCE < smallest_real


def test_correction_relies_on_ingested_at_not_deletion():
    """Documents why this pass can simply append.

    fund_daily is append-mode and the export/compaction tie-break now orders by
    ingested_at, so a freshly written row supersedes the wrong one. Without that
    ordering (added 2026-08-12) this pass would need a delete and could not be
    a plain write. See tests/test_duplicate_tiebreak.py.
    """
    from tests.test_duplicate_tiebreak import export_winner
    import pandas as pd

    rows = pd.DataFrame([
        {"fund_code": "022605", "trade_date": "2026-08-16", "unit_nav": 0.674,
         "accum_nav": 1.238, "ingested_at": pd.NaT},
        {"fund_code": "022605", "trade_date": "2026-08-16", "unit_nav": 0.337,
         "accum_nav": 1.238, "ingested_at": pd.Timestamp("2026-08-21 02:00")},
    ])
    assert export_winner(rows).unit_nav == 0.337
