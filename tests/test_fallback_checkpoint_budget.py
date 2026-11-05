"""Exercise the real handler: preserve batches before a slow phase can time out."""
import importlib.util
import os
import sys
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest


def load():
    os.environ.setdefault("WAREHOUSE_PATH", "s3://unit-test/warehouse/")
    spec = importlib.util.spec_from_file_location(
        "fallback_checkpoint_test",
        Path(__file__).resolve().parents[1] / "lambda/fund-fallback-fetcher/handler.py",
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = m
    spec.loader.exec_module(m)
    return m


def nav(code):
    return pd.DataFrame({
        "fund_code": [code], "fund_name": ["fixture"], "trade_date": [date(2026, 11, 2)],
        "unit_nav": [1.01], "accum_nav": [1.01], "daily_return_pct": [0.1],
        "subscription_status": ["开放申购"], "redemption_status": ["开放赎回"],
    })


@pytest.fixture
def harness(monkeypatch):
    fb = load()
    universe = pd.DataFrame({
        "fund_code": ["023712", "024009", "014093"],
        "fund_name": ["fixture"] * 3, "fund_type": ["FOF-稳健型"] * 3,
    })
    catalog = Mock()
    monkeypatch.setattr(fb, "load_catalog", Mock(return_value=catalog))
    monkeypatch.setattr(fb, "_latest_loaded_date", lambda _c: date(2026, 11, 4))
    monkeypatch.setattr(fb, "_latest_fund_names", lambda _c: universe, raising=False)
    monkeypatch.setattr(fb, "_get_missing_universe", lambda *a, **kw: universe.copy())
    monkeypatch.setattr(fb, "_existing_pairs", lambda *a: set())
    writer = Mock()
    writer.write.side_effect = lambda _table, df, **kw: {"rows_appended": len(df)}
    monkeypatch.setattr(fb, "IcebergWriter", Mock(return_value=writer))
    monkeypatch.setattr(fb, "FALLBACK_BATCH_SIZE", 1, raising=False)
    monkeypatch.setattr(fb, "FALLBACK_WORKERS", 1)
    monkeypatch.setattr(fb, "_fetch_one", lambda item, *_: (item[0], nav(item[0]), None))
    monkeypatch.setattr(fb, "VERIFY_MONEY_FUNDS", False)
    return fb, writer, universe


def test_handler_writes_each_batch_before_requesting_the_next(harness, monkeypatch):
    fb, writer, _ = harness
    order = []

    def fetch(item, *_):
        order.append("fetch")
        return item[0], nav(item[0]), None

    def write(_t, df, **_):
        order.append("write")
        return {"rows_appended": len(df)}

    monkeypatch.setattr(fb, "_fetch_one", fetch)
    writer.write.side_effect = write
    result = fb.lambda_handler({}, None)
    assert order == ["fetch", "write"] * 3
    assert result["rows_appended"] == 3
    assert result["complete"] is True


def test_budget_stop_keeps_committed_rows_and_reports_pending(harness):
    fb, writer, _ = harness
    context = Mock()
    context.get_remaining_time_in_millis.side_effect = (
        lambda: 70_000 if writer.write.called else 900_000
    )
    result = fb.lambda_handler({}, context)
    assert writer.write.call_count == 1
    assert result["rows_appended"] == 1
    assert result["pending"] == 2
    assert result["complete"] is False
    assert result["success"] is False


def test_gap_fill_precedes_money_verification(harness, monkeypatch):
    fb, writer, _ = harness
    order = []
    monkeypatch.setattr(fb, "VERIFY_MONEY_FUNDS", True)

    def money(*_a, **_kw):
        order.append("money")
        assert writer.write.called
        return {"checked": 977, "corrected": 0, "complete": False,
                "pending": 900, "errors": 0}

    monkeypatch.setattr(fb, "_verify_money_funds", money)
    result = fb.lambda_handler({}, None)
    assert order == ["money"]
    assert result["rows_appended"] == 3
    assert result["success"] is False
    assert result["money_verify"]["complete"] is False


def test_source_errors_are_not_success(harness, monkeypatch):
    fb, writer, _ = harness
    monkeypatch.setattr(fb, "_fetch_one", lambda item, *_: (item[0], None, "Timeout"))
    result = fb.lambda_handler({}, None)
    assert result["success"] is False
    assert result["errors"] == 3
    writer.write.assert_not_called()


def test_write_failure_is_not_success_or_counted_as_committed(harness):
    fb, writer, _ = harness
    writer.write.return_value = None
    writer.write.side_effect = lambda *a, **kw: {"error": "commit failed"}
    result = fb.lambda_handler({}, None)
    assert result["success"] is False
    assert result["rows_appended"] == 0
    assert "commit failed" in result["error"]


def test_completed_batches_update_dedup_state(harness, monkeypatch):
    fb, writer, _ = harness
    # Even if two requests accidentally return the same pair, never append twice.
    monkeypatch.setattr(fb, "_fetch_one", lambda item, *_: (item[0], nav("023712"), None))
    result = fb.lambda_handler({}, None)
    assert result["rows_appended"] == 1
    assert writer.write.call_count == 1


def test_low_remaining_time_does_not_start_network_calls(harness, monkeypatch):
    fb, writer, _ = harness
    fetch = Mock()
    monkeypatch.setattr(fb, "_fetch_one", fetch)
    context = Mock()
    context.get_remaining_time_in_millis.return_value = 20_000
    result = fb.lambda_handler({}, context)
    assert result["success"] is False
    assert result["complete"] is False
    fetch.assert_not_called()
    writer.write.assert_not_called()


def source_row(day="2026-11-02", value="1.01"):
    return {"FSRQ": day, "DWJZ": value, "LJJZ": "1.01", "JZZZL": "0.1",
            "SGZT": "开放申购", "SHZT": "开放赎回"}


@pytest.mark.parametrize("body", [
    {"ErrCode": 503, "Data": None},
    {"ErrCode": 0, "Data": {"LSJZList": [source_row("2026-11-05")]}},
    {"ErrCode": 0, "Data": {"LSJZList": [source_row(value="inf")]}},
])
def test_source_error_and_future_dates_fail_closed(monkeypatch, body):
    fb = load()
    response = Mock()
    response.json.return_value = body
    monkeypatch.setattr(fb.requests, "get", Mock(return_value=response))
    code, frame, error = fb._fetch_one(("023712", "fixture"), "2026-11-01", "2026-11-04")
    assert code == "023712" and frame is None and error != "empty"
    response.raise_for_status.assert_called_once()


def test_successful_empty_payload_is_not_a_network_failure(monkeypatch):
    fb = load()
    response = Mock()
    response.json.return_value = {"ErrCode": 0, "Data": None}
    monkeypatch.setattr(fb.requests, "get", Mock(return_value=response))
    assert fb._fetch_one(("023712", "fixture"), "2026-11-01", "2026-11-04") == (
        "023712", None, "empty",
    )


def test_money_check_commits_batches_and_reports_budget_stop(monkeypatch):
    fb = load()
    universe = pd.DataFrame({
        "fund_code": ["000001", "000002"], "fund_name": ["fixture"] * 2,
        "fund_type": ["货币型"] * 2,
    })
    held = nav("000001")
    held = pd.concat([held, nav("000002")], ignore_index=True)
    held["unit_nav"] = 2.0
    held["ingested_at"] = pd.Timestamp("2026-11-04")
    catalog = Mock()
    catalog.load_table.return_value.scan.return_value.to_arrow.return_value.to_pandas.return_value = held
    writer = Mock()
    writer.write.side_effect = lambda _t, df, **kw: {"rows_appended": len(df)}
    monkeypatch.setattr(fb, "FALLBACK_BATCH_SIZE", 1)
    monkeypatch.setattr(fb, "FALLBACK_WORKERS", 1)
    monkeypatch.setattr(fb, "_fetch_one", lambda item, *_: (item[0], nav(item[0]), None))
    budget = Mock()
    budget.expired.side_effect = lambda: writer.write.called
    result = fb._verify_money_funds(
        catalog, date(2026, 11, 4), writer, names=universe, budget=budget,
    )
    assert result["corrected"] == result["checked"] == 1
    assert result["complete"] is False and result["pending"] == 1
    assert writer.write.call_count == 1
