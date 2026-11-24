"""Single-date repair guards and full consumer preservation, without AWS."""
import importlib.util
from pathlib import Path

import pandas as pd
import pytest


path = Path(__file__).parents[1] / "scripts/repair_money_history_20261124.py"
spec = importlib.util.spec_from_file_location("money_history_repair", path)
repair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(repair)


def row(code, day, nav):
    return {"fund_code": code, "trade_date": day, "unit_nav": nav, "accum_nav": None}


def test_exact_guarded_state_required():
    repair.require_versions(123, "version", "123", "version")
    with pytest.raises(RuntimeError, match="changed"):
        repair.require_versions(124, "version", "123", "version")
    with pytest.raises(RuntimeError, match="changed"):
        repair.require_versions(123, "next-version", "123", "version")


@pytest.mark.parametrize("rows", [
    [], [{"FSRQ": "2026-11-23", "DWJZ": "0.3655"}],
    [{"FSRQ": "2026-11-22", "DWJZ": "inf"}],
    [{"FSRQ": "2026-11-22", "DWJZ": "0.3655"}] * 2,
])
def test_no_other_date_or_invalid_source(rows):
    with pytest.raises(ValueError):
        repair.require_source_rows(rows)


def test_all_original_values_retained_and_only_approved_row_added():
    before = pd.DataFrame([row("000001", repair.DAY, 1.23)])
    after = pd.concat([
        before, pd.DataFrame([row(repair.CODE, repair.DAY, 0.3655)]),
    ], ignore_index=True)
    repair.verify_export(before, after, 0.3655)
    wrong = after.copy()
    wrong.loc[0, "unit_nav"] = 1.24
    with pytest.raises(AssertionError):
        repair.verify_export(before, wrong, 0.3655)
    with pytest.raises(RuntimeError):
        repair.verify_export(before, pd.concat([after, after.iloc[[1]]]), 0.3655)


def test_unapproved_new_key_rejected():
    before = pd.DataFrame([row("000001", repair.DAY, 1.23)])
    after = pd.concat([
        before, pd.DataFrame([row("999999", repair.DAY, 0.3655)]),
    ], ignore_index=True)
    with pytest.raises(RuntimeError, match="Unexpected"):
        repair.verify_export(before, after, 0.3655)
