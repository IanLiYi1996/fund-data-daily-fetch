"""Pin the duplicate tie-break that makes a correction publishable.

fund_daily is append-mode, so one (fund_code, trade_date) can carry several
rows: the fallback adds a value the main snapshot missed, and a correction may
supersede a wrong value. Exactly one row per pair reaches the consumer, and
both places that choose used to choose arbitrarily:

- ``export-fund-history`` ordered only by "non-null before null", so between
  two NON-null rows the winner was undefined.
- ``iceberg-maintenance`` compaction sorted on the identifier columns alone
  and kept ``last``, i.e. whatever order the files happened to be read in.

That combination made corrections unshippable: on 2026-08-12, eleven weekend
money-market rows disagreed with 天天基金 by exactly 2x (some low, some high),
and writing the right value would not reliably have replaced the wrong one —
weekly compaction could even have thrown the fix away.

``ingested_at`` gives both a direction: newest write wins, and rows predating
the column (~30M of them) are NULL and always lose.

Pure ordering logic; no AWS, no DuckDB.
"""
import pandas as pd
import pytest

ID_COLS = ["fund_code", "trade_date"]


def export_winner(rows: pd.DataFrame) -> pd.Series:
    """Mirrors the export's row_number() ORDER BY."""
    df = rows.copy()
    df["_nav_null"] = df.unit_nav.isna().astype(int)
    df["_acc_null"] = df.accum_nav.isna().astype(int)
    # ingested_at DESC NULLS LAST
    df["_ts"] = df.ingested_at.fillna(pd.Timestamp.min)
    df = df.sort_values(
        ["_nav_null", "_acc_null", "_ts"], ascending=[True, True, False],
    )
    return df.iloc[0]


def compaction_survivor(rows: pd.DataFrame) -> pd.Series:
    """Mirrors compact_table's sort + drop_duplicates(keep='last')."""
    df = rows.sort_values(
        ID_COLS + ["ingested_at"], kind="mergesort", na_position="first",
    )
    return df.drop_duplicates(subset=ID_COLS, keep="last").iloc[0]


def _pair(old_nav, new_nav, old_ts, new_ts):
    return pd.DataFrame([
        {"fund_code": "000576", "trade_date": "2026-08-09",
         "unit_nav": old_nav, "accum_nav": 0.955, "ingested_at": old_ts},
        {"fund_code": "000576", "trade_date": "2026-08-09",
         "unit_nav": new_nav, "accum_nav": 0.955, "ingested_at": new_ts},
    ])


def test_export_prefers_the_newer_write():
    """The real 000576 case: 0.2607 was wrong, 0.5213 is upstream's value."""
    rows = _pair(0.2607, 0.5213,
                 pd.Timestamp("2026-08-10 03:00"), pd.Timestamp("2026-08-12 02:50"))
    assert export_winner(rows).unit_nav == 0.5213


def test_export_correction_beats_a_row_written_before_the_column_existed():
    """A pre-existing row has NULL ingested_at and must lose."""
    rows = _pair(0.2607, 0.5213, pd.NaT, pd.Timestamp("2026-08-12 02:50"))
    assert export_winner(rows).unit_nav == 0.5213


def test_export_still_prefers_non_null_nav_over_a_newer_null():
    """Recency must not outrank having a value at all.

    The fallback appends a real NAV next to an existing NULL row; that NULL
    could be the newer of the two.
    """
    rows = _pair(None, 1.0107, pd.Timestamp("2026-08-12 03:00"), pd.Timestamp("2026-08-10 03:00"))
    # old (null nav, newer ts) vs new (real nav, older ts) -> real nav wins
    assert export_winner(rows).unit_nav == 1.0107


def test_compaction_keeps_the_newer_write():
    rows = _pair(0.2607, 0.5213,
                 pd.Timestamp("2026-08-10 03:00"), pd.Timestamp("2026-08-12 02:50"))
    assert compaction_survivor(rows).unit_nav == 0.5213


def test_compaction_keeps_correction_over_null_timestamp():
    """Weekly compaction must not throw the fix away."""
    rows = _pair(0.2607, 0.5213, pd.NaT, pd.Timestamp("2026-08-12 02:50"))
    assert compaction_survivor(rows).unit_nav == 0.5213


def test_export_and_compaction_agree():
    """Divergence would make the exported value flip after compaction."""
    rows = _pair(0.2607, 0.5213, pd.NaT, pd.Timestamp("2026-08-12 02:50"))
    assert export_winner(rows).unit_nav == compaction_survivor(rows).unit_nav


@pytest.mark.parametrize("kind,ours,upstream", [
    ("half", 0.2607, 0.5213),    # 000576@2026-08-09
    ("double", 0.6664, 0.3332),  # 004903@2026-08-09
    ("rounding", 0.297, 0.2978), # 000837@2026-08-08
])
def test_all_three_observed_discrepancy_shapes_are_correctable(kind, ours, upstream):
    """The 11 corrected rows spanned all three shapes, in both directions."""
    rows = _pair(ours, upstream, pd.NaT, pd.Timestamp("2026-08-12 02:50"))
    assert export_winner(rows).unit_nav == upstream
    assert compaction_survivor(rows).unit_nav == upstream


# --- reconcile is the THIRD place that must apply the same rule ---
#
# Export and compaction were fixed on 2026-08-12. Reconciliation was not, and
# it reads raw Iceberg rows: on 2026-08-21 it reported
# `003317@2026-08-14 ours=1.2314 vs upstream 0.5608` while the corrected
# 0.5608 was already stored AND already in the consumer export. A checker that
# disagrees with what it ships raises false alarms about values nobody sees.

_MISSING = object()


def reconcile_pick(rows: pd.DataFrame) -> float:
    """Mirrors the newest-write selection in shared/quality/reconcile.py."""
    value = None
    stamp = _MISSING
    for _, r in rows.iterrows():
        ts = None if pd.isna(r.ingested_at) else r.ingested_at
        if stamp is not _MISSING:
            if ts is None:
                continue
            if stamp is not None and stamp >= ts:
                continue
        value, stamp = r.unit_nav, ts
    return value


def test_reconcile_prefers_the_correction():
    """The real 003317 case, in both row orders."""
    old = {"fund_code": "003317", "trade_date": "2026-08-14",
           "unit_nav": 1.2314, "ingested_at": pd.NaT}
    new = {"fund_code": "003317", "trade_date": "2026-08-14",
           "unit_nav": 0.5608, "ingested_at": pd.Timestamp("2026-08-21 02:30")}
    assert reconcile_pick(pd.DataFrame([old, new])) == 0.5608
    assert reconcile_pick(pd.DataFrame([new, old])) == 0.5608, (
        "must not depend on the order rows come back in"
    )


def test_reconcile_agrees_with_export():
    """Divergence here is what produced the false alarm."""
    rows = pd.DataFrame([
        {"fund_code": "003317", "trade_date": "2026-08-14",
         "unit_nav": 1.2314, "accum_nav": 1.0, "ingested_at": pd.NaT},
        {"fund_code": "003317", "trade_date": "2026-08-14",
         "unit_nav": 0.5608, "accum_nav": 1.0,
         "ingested_at": pd.Timestamp("2026-08-21 02:30")},
    ])
    assert reconcile_pick(rows) == export_winner(rows).unit_nav


def test_reconcile_handles_two_timestamped_rows():
    rows = pd.DataFrame([
        {"fund_code": "022605", "trade_date": "2026-08-16", "unit_nav": 0.674,
         "ingested_at": pd.Timestamp("2026-08-17 03:00")},
        {"fund_code": "022605", "trade_date": "2026-08-16", "unit_nav": 0.337,
         "ingested_at": pd.Timestamp("2026-08-21 02:30")},
    ])
    assert reconcile_pick(rows) == 0.337
