"""Exercise the real money verifier: late history must be appended, not skipped."""
from __future__ import annotations

import importlib.util
import os
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest


TARGET = date(2026, 11, 23)
SUNDAY = date(2026, 11, 22)
CODE = "001973"


@pytest.fixture
def fallback(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_PATH", "s3://unit-test/warehouse/")
    path = Path(__file__).parents[1] / "lambda/fund-fallback-fetcher/handler.py"
    spec = importlib.util.spec_from_file_location("money_history_fallback", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "FALLBACK_WORKERS", 1)
    return module


def frame(rows):
    return pd.DataFrame([
        {
            "fund_code": CODE, "fund_name": "光大保德信耀钱包货币A",
            "trade_date": day, "unit_nav": nav, "accum_nav": 0.664,
            "daily_return_pct": 0.0, "subscription_status": "开放申购",
            "redemption_status": "开放赎回",
        }
        for day, nav in rows
    ])


def run_verifier(fallback, monkeypatch, held_rows, source_rows, write_result=None):
    names = pd.DataFrame([{
        "fund_code": CODE, "fund_name": "光大保德信耀钱包货币A",
        "fund_type": "货币型-普通货币", "snapshot_date": TARGET,
    }])
    held = frame(held_rows)
    held["ingested_at"] = pd.Timestamp("2026-11-23T17:00:00")
    tables = {}
    for name, data in (("fund_name", names), ("fund_daily", held)):
        table = Mock()
        table.scan.return_value.to_arrow.return_value.to_pandas.return_value = data
        tables[name] = table
    catalog = Mock()
    catalog.load_table.side_effect = lambda identifier: tables[identifier[1]]
    monkeypatch.setattr(
        fallback, "_fetch_one",
        lambda item, lo, hi: (item[0], frame(source_rows), None),
    )
    writer = Mock()
    writer.write.side_effect = lambda table, data, **kwargs: (
        write_result if write_result is not None else {"rows_appended": len(data)}
    )
    result = fallback._verify_money_funds(catalog, TARGET, writer)
    return result, writer


def test_append_missing_sunday_even_when_monday_is_already_covered(fallback, monkeypatch):
    result, writer = run_verifier(
        fallback, monkeypatch, [(TARGET, 0.959)],
        [(SUNDAY, 0.3655), (TARGET, 0.959)],
    )
    writer.write.assert_called_once()
    repaired = writer.write.call_args.args[1]
    assert list(zip(repaired.trade_date, repaired.unit_nav)) == [(SUNDAY, 0.3655)]
    assert result["added"] == 1
    assert result["corrected"] == 0


def test_no_second_append_once_the_date_is_held(fallback, monkeypatch):
    result, writer = run_verifier(
        fallback, monkeypatch, [(SUNDAY, 0.3655), (TARGET, 0.959)],
        [(SUNDAY, 0.3655), (TARGET, 0.959)],
    )
    writer.write.assert_not_called()
    assert result["added"] == result["corrected"] == 0


def test_missing_row_and_existing_wrong_value_are_counted_separately(fallback, monkeypatch):
    result, writer = run_verifier(
        fallback, monkeypatch, [(TARGET, 0.8)],
        [(SUNDAY, 0.3655), (TARGET, 0.959)],
    )
    assert len(writer.write.call_args.args[1]) == 2
    assert result["added"] == result["corrected"] == 1


def test_no_invented_weekend_dates(fallback, monkeypatch):
    result, writer = run_verifier(
        fallback, monkeypatch, [(TARGET, 0.959)], [(TARGET, 0.959)],
    )
    writer.write.assert_not_called()
    assert result["added"] == 0


def test_null_local_nav_is_eligible_for_real_history(fallback, monkeypatch):
    result, writer = run_verifier(
        fallback, monkeypatch, [(SUNDAY, None), (TARGET, 0.959)],
        [(SUNDAY, 0.3655), (TARGET, 0.959)],
    )
    assert list(writer.write.call_args.args[1].trade_date) == [SUNDAY]
    assert result["added"] == 1


def test_latest_write_wins_before_comparison(fallback, monkeypatch):
    result, writer = run_verifier(
        fallback, monkeypatch, [(TARGET, 0.8), (TARGET, 0.959)],
        [(SUNDAY, 0.3655), (TARGET, 0.959)],
    )
    assert list(writer.write.call_args.args[1].trade_date) == [SUNDAY]
    assert result["corrected"] == 0


def test_source_date_outside_verified_window_fails_closed(fallback, monkeypatch):
    with pytest.raises(ValueError, match="window"):
        run_verifier(
            fallback, monkeypatch, [(TARGET, 0.959)],
            [(date(2026, 11, 24), 0.3655)],
        )


@pytest.mark.parametrize("write_result", [
    {"rows_appended": 0}, {"error": "commit failed"}, {},
])
def test_failed_or_partial_commit_does_not_report_repaired(fallback, monkeypatch, write_result):
    with pytest.raises(RuntimeError, match="committed"):
        run_verifier(
            fallback, monkeypatch, [(TARGET, 0.959)],
            [(SUNDAY, 0.3655), (TARGET, 0.959)], write_result,
        )


def test_null_source_nav_is_not_invented_as_zero(fallback, monkeypatch):
    result, writer = run_verifier(
        fallback, monkeypatch, [(TARGET, 0.959)],
        [(SUNDAY, None), (TARGET, 0.959)],
    )
    writer.write.assert_not_called()
    assert result["added"] == 0


def test_quote_rounding_does_not_cause_a_rewrite(fallback, monkeypatch):
    result, writer = run_verifier(
        fallback, monkeypatch, [(TARGET, 0.959)], [(TARGET, 0.9591)],
    )
    writer.write.assert_not_called()
    assert result["added"] == result["corrected"] == 0
