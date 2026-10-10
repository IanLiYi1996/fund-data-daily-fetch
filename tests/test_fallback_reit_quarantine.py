"""Pin the one-day quarantine for REIT rows in the fallback fetcher.

天天基金's lsjz endpoint sometimes publishes, on the evening of a trading
day, a row for a public REIT dated THAT day and carrying the fund's last
disclosed NAV — then removes it hours or days later. REITs disclose NAV
quarterly, so a same-day row with an unchanged value is almost never a real
disclosure. Measured 2026-10-09: 12 such phantom rows across 6 REITs
(508600, 508602, 508603, 508093, 180307, 180505), all written by the
fallback at ~17:20 UTC with trade_date == the run's target day, none of
them present upstream any more.

The fix is a quarantine rather than a blanket skip: a REIT row dated the
target day is dropped on the run that first sees it; the 14-day fetch
window re-fetches the fund the next night, and if upstream still has the
row it is written then. Real quarterly disclosures therefore land one day
late; transient rows never land.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest


def _load_fallback_module():
    os.environ.setdefault("WAREHOUSE_PATH", "s3://unit-test/warehouse/")
    script = (Path(__file__).resolve().parent.parent
              / "lambda" / "fund-fallback-fetcher" / "handler.py")
    spec = importlib.util.spec_from_file_location("fund_fallback_handler", script)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["fund_fallback_handler"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def fb():
    return _load_fallback_module()


TARGET = date(2026, 9, 30)


def _rows(*items):
    return pd.DataFrame(
        [{"fund_code": c, "trade_date": d, "unit_nav": v} for c, d, v in items]
    )


def test_reit_row_dated_target_day_is_dropped(fb):
    merged = _rows(("508600", TARGET, 4.092))
    out = fb._quarantine_reit_rows(merged, reit_codes={"508600"}, target=TARGET)
    assert out.empty


def test_reit_row_dated_earlier_is_kept(fb):
    # Seen on a later run: upstream kept it for a day, so it is real.
    merged = _rows(("508600", date(2026, 9, 29), 4.092))
    out = fb._quarantine_reit_rows(merged, reit_codes={"508600"}, target=TARGET)
    assert len(out) == 1


def test_non_reit_row_dated_target_day_is_kept(fb):
    # Same-day rows for ordinary funds are the whole point of the fallback.
    merged = _rows(("000001", TARGET, 1.234))
    out = fb._quarantine_reit_rows(merged, reit_codes={"508600"}, target=TARGET)
    assert len(out) == 1


def test_mixed_frame_only_loses_the_reit_target_row(fb):
    merged = _rows(
        ("508600", TARGET, 4.092),
        ("508600", date(2026, 9, 29), 4.092),
        ("000001", TARGET, 1.234),
    )
    out = fb._quarantine_reit_rows(merged, reit_codes={"508600"}, target=TARGET)
    kept = {(r.fund_code, r.trade_date) for r in out.itertuples()}
    assert kept == {("508600", date(2026, 9, 29)), ("000001", TARGET)}


def test_empty_frame_passes_through(fb):
    merged = _rows()
    out = fb._quarantine_reit_rows(merged, reit_codes={"508600"}, target=TARGET)
    assert out.empty


@pytest.mark.parametrize("ftype,expected", [
    ("Reits", True),
    ("QDII-REITs", False),   # open-end QDII with daily NAV; not quarantined
    ("指数型-其他", False),
    (None, False),
    ("", False),
])
def test_reit_type_predicate(fb, ftype, expected):
    assert fb._is_public_reit(ftype) is expected
