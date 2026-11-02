"""HK rankings must be keyed by JSON field name, never response field order."""
from __future__ import annotations

import io
from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from shared.fetchers.fund_fetcher import FundFetcher


def _row(code="968008", currency="港元"):
    # Actual field order returned from 2026-10-27. AkShare's positional
    # renaming turned CURRENCY into 基金代码 and Y into 币种.
    return {
        "_id": "1000621096", "HKFCODE": "1000621096", "CURRENCY": currency,
        "D": 0.5896, "FCODE": code, "FULLNAME": "恒生精选基金系列",
        "HY": 19.1104, "ISBUY": "", "JZRQ": "2026-10-28", "M": 3.0886,
        "NAV": 46.06, "NAVCHGRT": 0.5896, "Q": 7.0416, "SE": 11.7419,
        "SHORTNAME": "恒生中国企业指数(港币)", "SY": 27.6961,
        "TRY": 17.7103, "TWY": 16.372, "W": 1.0975, "Y": 28.1225,
    }


def _response(monkeypatch, rows):
    response = MagicMock()
    response.json.return_value = {"Data": rows}
    get = MagicMock(return_value=response)
    monkeypatch.setattr("shared.fetchers.fund_fetcher.requests.get", get)
    return get, response


@pytest.mark.parametrize("reverse", [False, True])
def test_real_fetch_path_uses_named_fields(monkeypatch, reverse):
    row = _row()
    if reverse:
        row = dict(reversed(list(row.items())))
    get, response = _response(monkeypatch, [row])
    result = FundFetcher().fetch_one("fund_hk_rank")
    assert result.success, result.error
    out = result.data.iloc[0]
    assert out["基金代码"] == "968008"
    assert out["基金简称"] == "恒生中国企业指数(港币)"
    assert out["币种"] == "港元"
    assert out["日期"] == date(2026, 10, 28)
    assert out["单位净值"] == 46.06
    assert out["近1周"] == 1.0975
    assert out["近1月"] == 3.0886
    assert out["近1年"] == 28.1225
    assert out["近2年"] == 16.372
    assert out["近3年"] == 17.7103
    assert out["今年来"] == 27.6961
    assert out["成立来"] == 11.7419
    response.raise_for_status.assert_called_once()
    assert get.call_args.kwargs["timeout"] <= 20


def test_blank_numeric_values_are_parquet_safe(monkeypatch):
    first, second = _row(), _row("968063", "美元")
    second.update({"Y": "", "NAV": "", "JZRQ": "", "ISBUY": "1"})
    _response(monkeypatch, [first, second])
    result = FundFetcher().fetch_one("fund_hk_rank")
    assert result.success, result.error
    assert result.data["基金代码"].nunique() == 2
    assert pd.isna(result.data.loc[1, "单位净值"])
    assert pd.isna(result.data.loc[1, "近1年"])
    assert pd.isna(result.data.loc[1, "日期"])
    assert result.data.loc[1, "可购买"] == "可购买"
    result.data.to_parquet(io.BytesIO(), engine="pyarrow", index=False)


def test_unknown_json_fields_do_not_shift_columns(monkeypatch):
    _response(monkeypatch, [{"new_field": "anything", **_row()}])
    result = FundFetcher().fetch_one("fund_hk_rank")
    assert result.success, result.error
    assert result.data.loc[0, "基金代码"] == "968008"


@pytest.mark.parametrize("code", ["人民币", "", None])
def test_invalid_identifier_fails_before_either_write(monkeypatch, code):
    _response(monkeypatch, [_row(code)])
    result = FundFetcher().fetch_one("fund_hk_rank")
    assert not result.success
    assert "fund code" in result.error


def test_duplicate_identifier_is_not_silently_collapsed(monkeypatch):
    _response(monkeypatch, [_row(), _row()])
    result = FundFetcher().fetch_one("fund_hk_rank")
    assert not result.success
    assert "duplicate" in result.error


def test_missing_source_field_fails_closed(monkeypatch):
    row = _row()
    del row["FCODE"]
    _response(monkeypatch, [row])
    result = FundFetcher().fetch_one("fund_hk_rank")
    assert not result.success
    assert "FCODE" in result.error


def test_empty_response_is_safe(monkeypatch):
    _response(monkeypatch, [])
    result = FundFetcher().fetch_one("fund_hk_rank")
    assert result.success
    assert result.data.empty
