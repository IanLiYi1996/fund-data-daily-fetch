"""Pin the fallback's re-check of overseas funds that publish NAV late.

2026-10-22 alert: 024149@2026-10-19 missing (upstream lsjz had 0.7213).
QDII NAVs land a day or more late. On the 10-19 run the fund was in the gap
but lsjz didn't have 10-19 yet; by the 10-20 run the snapshot had written
10-20, the fund was no longer missing ON the target day, and 10-19 was never
asked for again. A manual 10-19 re-run recovered 12 such rows, all QDII
(6 A/C pairs) — 4 of them typed 指数型-海外股票, not QDII-*.

Fix: overseas funds missing a NAV on any of the last few market trading days
before the target join the gap. The 14-day fetch window and _existing_pairs
already confine the write to the holes.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest


def _load():
    os.environ.setdefault("WAREHOUSE_PATH", "s3://unit-test/warehouse/")
    script = (Path(__file__).resolve().parent.parent
              / "lambda" / "fund-fallback-fetcher" / "handler.py")
    spec = importlib.util.spec_from_file_location("fund_fallback_late", script)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["fund_fallback_late"] = mod
    spec.loader.exec_module(mod)
    return mod


fb = _load()

UNIVERSE = pd.DataFrame({
    "fund_code": ["024149", "026704", "000001", "006283", "999999"],
    "fund_name": ["长城恒生科技指数(QDII)C", "长城港股价值优选股票(QDII)C",
                  "华夏成长混合", "鹏华美国房地产美元现汇", "某某海外"],
    "fund_type": ["指数型-海外股票", "QDII-普通股票", "混合型-偏股",
                  "QDII-REITs", "债券型-长债"],
})

# Mon 10-19 .. Wed 10-21; target is Wed 10-21
D = {n: date(2026, 10, n) for n in (14, 15, 16, 19, 20, 21)}
FILLER = {f"F{i:05d}" for i in range(fb.LATE_MARKET_MIN_FUNDS + 1)}


def _pairs(holes=()):
    """Every fund (plus a market's worth of filler) has every day except holes."""
    codes = set(UNIVERSE.fund_code) | FILLER
    return {(c, d) for c in codes for d in D.values()} - set(holes)


def test_overseas_detection():
    is_ov = [fb._is_overseas(n, t) for n, t in
             zip(UNIVERSE.fund_name, UNIVERSE.fund_type)]
    # name-only QDII, QDII-* type, 海外 type; not 混合型 / not a 债券 fund
    # whose name merely says 海外 without QDII
    assert is_ov == [True, True, False, True, False]


def test_flags_overseas_hole_on_recent_trading_day():
    pairs = _pairs(holes=[("024149", D[19])])
    assert fb._late_overseas_codes(UNIVERSE, pairs, D[21]) == {"024149"}


def test_domestic_hole_not_flagged():
    pairs = _pairs(holes=[("000001", D[19])])
    assert fb._late_overseas_codes(UNIVERSE, pairs, D[21]) == set()


def test_hole_older_than_window_not_flagged():
    # 3 trading days before Wed 10-21 are 10-20, 10-19, 10-16
    pairs = _pairs(holes=[("026704", D[15])])
    assert fb._late_overseas_codes(UNIVERSE, pairs, D[21]) == set()
    pairs = _pairs(holes=[("026704", D[16])])
    assert fb._late_overseas_codes(UNIVERSE, pairs, D[21]) == {"026704"}


def test_target_day_itself_not_counted():
    # Missing ON the target is the ordinary gap's job, not this one's
    pairs = _pairs(holes=[("024149", D[21])])
    assert fb._late_overseas_codes(UNIVERSE, pairs, D[21]) == set()


def test_market_holiday_is_not_a_hole():
    # Nobody has 10-20 (e.g. an exchange holiday): not a trading day
    pairs = {p for p in _pairs() if p[1] != D[20]}
    assert fb._late_overseas_codes(UNIVERSE, pairs, D[21]) == set()


def test_fund_with_no_recent_history_not_flagged():
    """A long-dormant or brand-new overseas fund isn't a late publisher."""
    pairs = {p for p in _pairs() if p[0] != "006283"}
    assert fb._late_overseas_codes(UNIVERSE, pairs, D[21]) == set()
