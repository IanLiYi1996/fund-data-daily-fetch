"""Tests for fund-history HTML / JS parsers."""

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from shared.fetchers.fund_history_fetcher import (
    parse_manager_change_html,
    parse_scale_fluctuation,
)


FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def manager_html_710001() -> str:
    return (FIXTURE_DIR / "jjjl_710001.html").read_text(encoding="utf-8")


@pytest.fixture
def pingzhongdata_710001() -> str:
    return (FIXTURE_DIR / "pingzhongdata_710001.js").read_text(encoding="utf-8")


# --- parse_manager_change_html ---


def test_manager_parser_returns_one_row_per_manager_per_tenure(manager_html_710001):
    """Multi-manager tenure (e.g. '申坤 李守峰') should produce N rows, one per manager."""
    df = parse_manager_change_html(manager_html_710001, fund_code="710001")
    # current tenure: 李守峰 (1 manager) → 1 row
    current = df[df["是否现任"]]
    assert len(current) == 1
    assert current.iloc[0]["经理姓名"] == "李守峰"

    # tenure 2026-01-30 → 2026-03-03 had two managers '申坤 李守峰' → 2 rows
    coed = df[df["起始日"] == dt.date(2026, 1, 30)]
    assert set(coed["经理姓名"]) == {"申坤", "李守峰"}


def test_manager_parser_extracts_dates_and_tenure_metrics(manager_html_710001):
    df = parse_manager_change_html(manager_html_710001, fund_code="710001")

    # First row in the fixture: 2026-03-04 至今 李守峰 71天 12.17%
    current = df[df["是否现任"]].iloc[0]
    assert current["起始日"] == dt.date(2026, 3, 4)
    assert pd.isna(current["结束日"])
    assert current["任期天数"] == 71
    assert current["任期回报"] == pytest.approx(12.17)

    # Tenure with '1年又187天' should parse to 365+187=552 days
    long_tenure = df[(df["起始日"] == dt.date(2024, 7, 26)) & (df["经理姓名"] == "申坤")].iloc[0]
    assert long_tenure["任期天数"] == 365 + 187
    assert long_tenure["任期回报"] == pytest.approx(71.50)
    assert long_tenure["结束日"] == dt.date(2026, 1, 29)


def test_manager_parser_attaches_fund_code(manager_html_710001):
    df = parse_manager_change_html(manager_html_710001, fund_code="710001")
    assert (df["基金代码"] == "710001").all()


def test_manager_parser_handles_missing_table():
    # No 基金经理变动 section → empty DataFrame with correct columns
    html = "<html><body><h4>无内容</h4></body></html>"
    df = parse_manager_change_html(html, fund_code="999999")
    assert df.empty
    assert "经理姓名" in df.columns
    assert "起始日" in df.columns


def test_manager_parser_handles_dash_return():
    """A '--' tenure return should produce NaN, not crash."""
    html = """
    <h4>基金经理变动一览</h4>
    <table>
      <tr><th>起始期</th><th>截止期</th><th>基金经理</th><th>任职期间</th><th>任职回报</th></tr>
      <tr><td>2026-04-01</td><td>至今</td><td>张三</td><td>40天</td><td>--</td></tr>
    </table>
    <h4>现任基金经理简介</h4>
    """
    df = parse_manager_change_html(html, fund_code="000001")
    assert len(df) == 1
    assert pd.isna(df.iloc[0]["任期回报"])


# --- parse_scale_fluctuation ---


def test_scale_parser_extracts_quarterly_series(pingzhongdata_710001):
    df = parse_scale_fluctuation(pingzhongdata_710001, fund_code="710001")
    # fixture has 5 quarters from 2025-03-31 to 2026-03-31
    assert len(df) == 5
    assert (df["基金代码"] == "710001").all()
    # last row
    last = df.iloc[-1]
    assert last["报告期"] == dt.date(2026, 3, 31)
    assert last["期末净资产_亿元"] == pytest.approx(5.26)
    assert last["净资产环比变动率"] == pytest.approx(-25.08)


def test_scale_parser_handles_empty_data():
    js = "var Data_fluctuationScale = {categories: [], series: []};"
    df = parse_scale_fluctuation(js, fund_code="999999")
    assert df.empty
    assert {"基金代码", "报告期", "期末净资产_亿元", "净资产环比变动率"}.issubset(df.columns)


def test_scale_parser_missing_variable_returns_empty():
    df = parse_scale_fluctuation("var unrelated = 1;", fund_code="000001")
    assert df.empty
