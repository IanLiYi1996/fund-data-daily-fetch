import importlib.util
from datetime import date
from pathlib import Path

import pandas as pd
import pytest


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
