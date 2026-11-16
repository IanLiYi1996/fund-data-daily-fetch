import importlib.util
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from unittest.mock import Mock


@pytest.fixture
def acceptance():
    path = Path(__file__).resolve().parents[1] / "scripts/verify_portfolio_history_20261116.py"
    spec = importlib.util.spec_from_file_location("portfolio_acceptance_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    old = pd.DataFrame([{
        "fund_code": "000001", "report_date": date(2026, 6, 30),
        "holding_code": "00700", "holding_name": "fixture",
        "weight_pct": 1.0, "shares": 100.0, "market_value": 200.0,
    }])
    new = pd.concat([old, old.assign(report_date=date(2026, 9, 30))], ignore_index=True)
    return module, old, new


def test_acceptance_keeps_all_old_values_and_validates_new_quarter(acceptance):
    module, old, new = acceptance
    result = module.verify_frames(old, new)
    assert result["historical_rows_preserved"] == 1
    assert result["latest_report_date"] == "2026-09-30"


def test_acceptance_detects_lost_history(acceptance):
    module, old, new = acceptance
    with pytest.raises(AssertionError, match="lost"):
        module.verify_frames(old, new.iloc[1:])


def test_acceptance_detects_changed_historical_values(acceptance):
    module, old, new = acceptance
    new.loc[0, "shares"] = 110.0
    with pytest.raises(AssertionError, match="changed"):
        module.verify_frames(old, new)


def test_acceptance_rejects_future_dates(acceptance):
    module, old, new = acceptance
    new.loc[1, "report_date"] = date(2026, 12, 31)
    with pytest.raises(AssertionError, match="completed Q3"):
        module.verify_frames(old, new)


def test_reference_retries_transient_empty_without_changing_sample(acceptance, monkeypatch):
    module, old, new = acceptance
    fetch = Mock(side_effect=[old.iloc[:0], new.iloc[1:]])
    sleep = Mock()
    monkeypatch.setattr(module, "fetch_holdings", fetch)
    monkeypatch.setattr(module.time, "sleep", sleep)
    frame = module.read_reference("000001")
    assert len(frame) == 1
    assert fetch.call_count == 2
    assert all(call.args == ("000001", 2026) for call in fetch.call_args_list)
    sleep.assert_called_once_with(5)


def test_reference_permanent_empty_is_not_fabricated(acceptance, monkeypatch):
    module, old, _ = acceptance
    fetch = Mock(return_value=old.iloc[:0])
    monkeypatch.setattr(module, "fetch_holdings", fetch)
    monkeypatch.setattr(module.time, "sleep", Mock())
    assert module.read_reference("000001").empty
    assert fetch.call_count == 3
