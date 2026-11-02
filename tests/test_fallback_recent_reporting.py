"""A new/resumed domestic fund's target-day NAV must not hide earlier holes."""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest


def _load():
    os.environ.setdefault("WAREHOUSE_PATH", "s3://unit-test/warehouse/")
    path = Path(__file__).resolve().parent.parent / "lambda/fund-fallback-fetcher/handler.py"
    spec = importlib.util.spec_from_file_location("fallback_recent_reporting", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


fb = _load()
TARGET = date(2026, 10, 29)


def _gap(days, fund_type="", name="华西长盈63个月封闭债券"):
    catalog, names, daily = MagicMock(), MagicMock(), MagicMock()
    names.scan.return_value.to_arrow.return_value = pa.table({
        "fund_code": ["029238"], "fund_name": [name],
        "fund_type": [fund_type], "snapshot_date": [TARGET],
    })
    pairs = pa.table({
        "fund_code": ["029238"] * len(days), "trade_date": days,
        "unit_nav": [1.0001] * len(days),
    })
    covered = pa.table({
        "fund_code": ["029238"] if TARGET in days else [],
        "unit_nav": [1.0001] if TARGET in days else [],
    })
    daily.scan.return_value.to_arrow.side_effect = [covered, pairs]
    catalog.load_table.side_effect = (
        lambda ident: names if ident[1] == "fund_name" else daily
    )
    gap = fb._get_missing_universe(catalog, TARGET, TARGET - timedelta(days=14))
    assert daily.scan.call_count == 2  # reuse the overseas-check scan
    return set(gap.fund_code)


@pytest.mark.parametrize("fund_type", ["", "债券型-长债", "混合型-偏股"])
def test_029238_is_selected_even_with_target_day_nav(fund_type):
    assert _gap([TARGET], fund_type=fund_type) == {"029238"}


def test_recently_resumed_fund_is_rechecked_for_a_few_days():
    assert _gap([TARGET - timedelta(days=2), TARGET]) == {"029238"}


def test_established_domestic_fund_is_not_added_to_every_run():
    assert _gap([TARGET - timedelta(days=8), TARGET]) == set()


def test_old_periodic_disclosure_is_not_a_recent_restart():
    assert _gap([TARGET - timedelta(days=8)]) == {"029238"}  # ordinary target gap
    assert _gap([TARGET - timedelta(days=5), TARGET]) == set()


def test_target_day_gap_is_still_selected_without_history():
    assert _gap([]) == {"029238"}


def test_recent_reporting_does_not_duplicate_overseas_candidates():
    assert _gap([TARGET], fund_type="QDII-普通股票") == {"029238"}
