"""Tests for BaseFetcher._safe_fetch retrying transient connection drops.

2026-10-17 and 2026-10-20: fund_money_fund_daily_em died ~1s in with
``Response ended prematurely`` (requests ChunkedEncodingError) and the
table missed the day; a re-run minutes later succeeded.
"""
from __future__ import annotations

import http.client

import pandas as pd
import pytest
import requests

from shared.fetchers import base_fetcher
from shared.fetchers.base_fetcher import BaseFetcher


class _Stub(BaseFetcher):
    @property
    def category(self):
        return "fund"

    def fetch_all(self):
        raise NotImplementedError


@pytest.fixture
def fetcher():
    return _Stub()


@pytest.fixture
def sleeps(monkeypatch):
    calls = []
    monkeypatch.setattr(base_fetcher.time, "sleep", lambda s: calls.append(s))
    return calls


def _flaky(errors, df):
    """Raise each error in turn, then return df."""
    state = {"calls": 0}

    def fn():
        state["calls"] += 1
        if errors:
            raise errors.pop(0)
        return df

    return fn, state


DF = pd.DataFrame({"a": [1, 2, 3]})


@pytest.mark.parametrize("exc", [
    requests.exceptions.ChunkedEncodingError("Response ended prematurely"),
    requests.exceptions.ConnectionError(
        "Connection aborted.",
        http.client.RemoteDisconnected("Remote end closed connection without response"),
    ),
    http.client.IncompleteRead(b""),
])
def test_transient_error_then_success_retries(fetcher, sleeps, exc):
    fn, state = _flaky([exc], DF)

    result = fetcher._safe_fetch("fund_money_daily", fn)

    assert result.success
    assert result.row_count == 3
    assert state["calls"] == 2
    assert len(sleeps) == 1


def test_gives_up_after_max_attempts(fetcher, sleeps):
    errs = [requests.exceptions.ChunkedEncodingError(f"drop {i}") for i in range(5)]
    fn, state = _flaky(errs, DF)

    result = fetcher._safe_fetch("fund_money_daily", fn)

    assert not result.success
    assert state["calls"] == base_fetcher.TRANSIENT_MAX_ATTEMPTS == 3
    assert result.error == "drop 2"
    assert len(sleeps) == 2


@pytest.mark.parametrize("exc", [
    ValueError("bad payload"),
    KeyError("基金代码"),
    requests.exceptions.ReadTimeout("read timeout=15"),
    requests.exceptions.ConnectTimeout("connect timeout"),
    requests.exceptions.HTTPError("502 Server Error"),
])
def test_non_transient_error_not_retried(fetcher, sleeps, exc):
    fn, state = _flaky([exc], DF)

    result = fetcher._safe_fetch("t", fn)

    assert not result.success
    assert state["calls"] == 1
    assert sleeps == []


def test_wrapped_transient_error_is_retried(fetcher, sleeps):
    """A library that re-raises with ``from`` still counts as transient."""
    def wrap():
        try:
            raise requests.exceptions.ChunkedEncodingError("Response ended prematurely")
        except Exception as e:
            raise RuntimeError("fetch failed") from e

    state = {"calls": 0}

    def fn():
        state["calls"] += 1
        if state["calls"] == 1:
            wrap()
        return DF

    assert fetcher._safe_fetch("t", fn).success
    assert state["calls"] == 2


def test_slow_failure_not_retried(fetcher, sleeps, monkeypatch):
    """A drop after minutes of work is not cheap to redo; a re-run could
    push the partition past the 15 min Lambda timeout."""
    clock = iter([0.0, base_fetcher.TRANSIENT_RETRY_MAX_ELAPSED + 1])
    monkeypatch.setattr(base_fetcher.time, "monotonic", lambda: next(clock))
    fn, state = _flaky([requests.exceptions.ChunkedEncodingError("late")], DF)

    result = fetcher._safe_fetch("t", fn)

    assert not result.success
    assert state["calls"] == 1


def test_success_first_try_no_sleep(fetcher, sleeps):
    fn, state = _flaky([], DF)
    assert fetcher._safe_fetch("t", fn).success
    assert state["calls"] == 1
    assert sleeps == []
