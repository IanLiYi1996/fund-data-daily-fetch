"""Tests for FundHistoryFetcher orchestration (concurrency + retry + aggregation)."""

import datetime as dt
from typing import Optional

import pandas as pd
import pytest

from shared.fetchers.fund_history_fetcher import FundHistoryFetcher


class FakeClient:
    """Mocks HTTP fetches; records call counts; supports per-code failure injection."""

    def __init__(self):
        self.manager_calls: dict[str, int] = {}
        self.scale_calls: dict[str, int] = {}
        self.fail_codes: set[str] = set()
        self.fail_until_attempt: dict[str, int] = {}  # code → succeed only on attempt N

    def fetch_manager_html(self, fund_code: str) -> str:
        self.manager_calls[fund_code] = self.manager_calls.get(fund_code, 0) + 1
        attempt = self.manager_calls[fund_code]
        if fund_code in self.fail_codes:
            raise RuntimeError(f"network error for {fund_code}")
        if attempt < self.fail_until_attempt.get(fund_code, 0):
            raise RuntimeError(f"transient {fund_code} attempt {attempt}")
        # Minimal HTML with 1 tenure row for the requested code
        return f"""
        <h4>基金经理变动一览</h4>
        <table>
          <tr><th>起始期</th><th>截止期</th><th>基金经理</th><th>任职期间</th><th>任职回报</th></tr>
          <tr><td>2024-01-01</td><td>至今</td><td>张{fund_code[-1]}</td><td>500天</td><td>10.00%</td></tr>
        </table>
        <h4>现任基金经理简介</h4>
        """

    def fetch_pingzhongdata(self, fund_code: str) -> str:
        self.scale_calls[fund_code] = self.scale_calls.get(fund_code, 0) + 1
        if fund_code in self.fail_codes:
            raise RuntimeError(f"network error for {fund_code}")
        return (
            "var Data_fluctuationScale = "
            '{categories: ["2025-12-31","2026-03-31"], '
            'series: [{y: 1.0, mom: "5.00%"}, {y: 1.5, mom: "50.00%"}]};'
        )


# --- happy path ---


def test_fetch_manager_history_aggregates_per_fund_dataframes():
    fetcher = FundHistoryFetcher(client=FakeClient(), max_workers=2)
    snapshot_date = dt.date(2026, 5, 14)

    df, errors = fetcher.fetch_manager_history(["000001", "000002"], snapshot_date=snapshot_date)

    assert len(df) == 2
    assert set(df["基金代码"]) == {"000001", "000002"}
    assert (df["snapshot_date"] == snapshot_date).all()
    assert errors == []


def test_fetch_scale_history_aggregates_per_fund_dataframes():
    fetcher = FundHistoryFetcher(client=FakeClient(), max_workers=2)
    snapshot_date = dt.date(2026, 5, 14)

    df, errors = fetcher.fetch_scale_history(["000001", "000002"], snapshot_date=snapshot_date)

    # 2 funds × 2 quarters
    assert len(df) == 4
    assert set(df["基金代码"]) == {"000001", "000002"}
    assert (df["snapshot_date"] == snapshot_date).all()
    assert errors == []


# --- error handling ---


def test_failed_fund_recorded_in_errors_does_not_abort():
    client = FakeClient()
    client.fail_codes = {"000002"}
    fetcher = FundHistoryFetcher(client=client, max_workers=2, max_retries=1)

    df, errors = fetcher.fetch_manager_history(
        ["000001", "000002", "000003"], snapshot_date=dt.date(2026, 5, 14)
    )

    assert set(df["基金代码"]) == {"000001", "000003"}
    assert len(errors) == 1
    assert errors[0]["基金代码"] == "000002"
    assert "network error" in errors[0]["error"]


def test_transient_failure_retries_and_succeeds():
    client = FakeClient()
    client.fail_until_attempt["000001"] = 2  # fails on attempt 1, succeeds on attempt 2
    fetcher = FundHistoryFetcher(client=client, max_workers=1, max_retries=3, retry_delay=0.0)

    df, errors = fetcher.fetch_manager_history(["000001"], snapshot_date=dt.date(2026, 5, 14))

    assert len(df) == 1
    assert errors == []
    assert client.manager_calls["000001"] == 2


def test_empty_input_returns_empty_dataframe():
    fetcher = FundHistoryFetcher(client=FakeClient(), max_workers=2)
    df, errors = fetcher.fetch_manager_history([], snapshot_date=dt.date(2026, 5, 14))
    assert df.empty
    assert "基金代码" in df.columns
    assert errors == []
