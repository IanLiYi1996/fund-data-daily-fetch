"""Per-fund manager tenure + scale history.

Sources (verified 2026-05-14):
- Manager tenure history: https://fundf10.eastmoney.com/jjjl_{code}.html
- Scale fluctuation series: https://fund.eastmoney.com/pingzhongdata/{code}.js
  → JS variable Data_fluctuationScale
"""
from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import re
import time
from typing import Callable, Optional, Protocol

import pandas as pd


_MANAGER_COLUMNS = [
    "基金代码",
    "经理姓名",
    "起始日",
    "结束日",
    "任期天数",
    "任期回报",
    "是否现任",
]

_SCALE_COLUMNS = [
    "基金代码",
    "报告期",
    "期末净资产_亿元",
    "净资产环比变动率",
]


def _strip_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def _parse_tenure_days(text: str) -> Optional[int]:
    """'1年又187天' → 552, '71天' → 71, '' → None."""
    text = text.strip()
    if not text:
        return None
    m = re.match(r"(?:(\d+)年又)?(\d+)天", text)
    if not m:
        return None
    years = int(m.group(1)) if m.group(1) else 0
    days = int(m.group(2))
    return years * 365 + days


def _parse_percent(text: str) -> float:
    """'12.17%' → 12.17, '-3.51%' → -3.51, '--' → NaN."""
    text = text.strip().rstrip("%").strip()
    if not text or text == "--":
        return float("nan")
    try:
        return float(text)
    except ValueError:
        return float("nan")


def _parse_date(text: str) -> Optional[dt.date]:
    text = text.strip()
    if not text or text == "至今":
        return None
    try:
        return dt.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_manager_change_html(html: str, fund_code: str) -> pd.DataFrame:
    """Parse the '基金经理变动一览' table from fundf10 page.

    Returns one row per (tenure × manager). For multi-manager tenures
    (e.g. '申坤 李守峰'), the row is split into N rows.
    """
    idx = html.find("基金经理变动一览")
    if idx < 0:
        return pd.DataFrame(columns=_MANAGER_COLUMNS)
    end = html.find("现任基金经理简介", idx)
    section = html[idx:end] if end > 0 else html[idx:]

    table_match = re.search(r"<table[^>]*>(.*?)</table>", section, re.DOTALL)
    if not table_match:
        return pd.DataFrame(columns=_MANAGER_COLUMNS)

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_match.group(1), re.DOTALL)
    records = []
    for row in rows:
        cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row, re.DOTALL)
        if len(cells) != 5:
            continue
        clean = [_strip_tags(c) for c in cells]
        # Skip header row
        if clean[0] == "起始期":
            continue
        start_str, end_str, managers_str, tenure_str, return_str = clean
        start = _parse_date(start_str)
        if start is None:
            continue
        end = _parse_date(end_str)
        is_current = end_str.strip() == "至今"
        days = _parse_tenure_days(tenure_str)
        ret = _parse_percent(return_str)
        for manager in managers_str.split():
            records.append({
                "基金代码": fund_code,
                "经理姓名": manager,
                "起始日": start,
                "结束日": end,
                "任期天数": days,
                "任期回报": ret,
                "是否现任": is_current,
            })

    if not records:
        return pd.DataFrame(columns=_MANAGER_COLUMNS)
    return pd.DataFrame(records, columns=_MANAGER_COLUMNS)


def parse_scale_fluctuation(js_text: str, fund_code: str) -> pd.DataFrame:
    """Parse Data_fluctuationScale from pingzhongdata.js."""
    m = re.search(
        r"var\s+Data_fluctuationScale\s*=\s*(\{.*?\})\s*;",
        js_text,
        re.DOTALL,
    )
    if not m:
        return pd.DataFrame(columns=_SCALE_COLUMNS)

    raw = m.group(1)
    # Convert JS object literal to JSON: quote unquoted keys.
    # Keys here are ASCII identifiers (categories, series, y, mom).
    json_text = re.sub(r"([{,])\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", r'\1"\2":', raw)
    try:
        data = json.loads(json_text)
    except json.JSONDecodeError:
        return pd.DataFrame(columns=_SCALE_COLUMNS)

    categories = data.get("categories") or []
    series = data.get("series") or []
    if not categories or not series or len(categories) != len(series):
        return pd.DataFrame(columns=_SCALE_COLUMNS)

    records = []
    for date_str, point in zip(categories, series):
        report_date = _parse_date(date_str)
        if report_date is None:
            continue
        records.append({
            "基金代码": fund_code,
            "报告期": report_date,
            "期末净资产_亿元": point.get("y"),
            "净资产环比变动率": _parse_percent(str(point.get("mom", ""))),
        })

    if not records:
        return pd.DataFrame(columns=_SCALE_COLUMNS)
    return pd.DataFrame(records, columns=_SCALE_COLUMNS)


class HttpClient(Protocol):
    def fetch_manager_html(self, fund_code: str) -> str: ...
    def fetch_pingzhongdata(self, fund_code: str) -> str: ...


class EastMoneyHttpClient:
    """Default HTTP client hitting eastmoney endpoints."""

    MANAGER_URL = "https://fundf10.eastmoney.com/jjjl_{code}.html"
    PINGZHONG_URL = "https://fund.eastmoney.com/pingzhongdata/{code}.js"

    def __init__(self, timeout: float = 10.0):
        import requests
        from akshare.utils.cons import headers

        self._session = requests.Session()
        self._session.headers.update(headers)
        self._timeout = timeout

    def fetch_manager_html(self, fund_code: str) -> str:
        r = self._session.get(self.MANAGER_URL.format(code=fund_code), timeout=self._timeout)
        r.raise_for_status()
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text

    def fetch_pingzhongdata(self, fund_code: str) -> str:
        r = self._session.get(self.PINGZHONG_URL.format(code=fund_code), timeout=self._timeout)
        r.raise_for_status()
        return r.text


class FundHistoryFetcher:
    """Concurrently fetches per-fund manager tenure and scale history."""

    def __init__(
        self,
        client: Optional[HttpClient] = None,
        max_workers: int = 8,
        max_retries: int = 3,
        retry_delay: float = 0.5,
    ):
        self._client = client if client is not None else EastMoneyHttpClient()
        self._max_workers = max_workers
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    def fetch_manager_history(
        self, fund_codes: list[str], snapshot_date: dt.date
    ) -> tuple[pd.DataFrame, list[dict]]:
        return self._run_parallel(
            fund_codes=fund_codes,
            snapshot_date=snapshot_date,
            fetch_fn=self._client.fetch_manager_html,
            parse_fn=parse_manager_change_html,
            empty_columns=_MANAGER_COLUMNS,
        )

    def fetch_scale_history(
        self, fund_codes: list[str], snapshot_date: dt.date
    ) -> tuple[pd.DataFrame, list[dict]]:
        return self._run_parallel(
            fund_codes=fund_codes,
            snapshot_date=snapshot_date,
            fetch_fn=self._client.fetch_pingzhongdata,
            parse_fn=parse_scale_fluctuation,
            empty_columns=_SCALE_COLUMNS,
        )

    def _run_parallel(
        self,
        fund_codes: list[str],
        snapshot_date: dt.date,
        fetch_fn: Callable[[str], str],
        parse_fn: Callable[[str, str], pd.DataFrame],
        empty_columns: list[str],
    ) -> tuple[pd.DataFrame, list[dict]]:
        if not fund_codes:
            empty = pd.DataFrame(columns=empty_columns + ["snapshot_date"])
            return empty, []

        frames: list[pd.DataFrame] = []
        errors: list[dict] = []

        def worker(code: str) -> tuple[str, Optional[pd.DataFrame], Optional[str]]:
            for attempt in range(1, self._max_retries + 1):
                try:
                    raw = fetch_fn(code)
                    df = parse_fn(raw, code)
                    return code, df, None
                except Exception as exc:
                    if attempt >= self._max_retries:
                        return code, None, f"{type(exc).__name__}: {exc}"
                    if self._retry_delay > 0:
                        time.sleep(self._retry_delay * attempt)
            return code, None, "exceeded retries"

        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = [pool.submit(worker, code) for code in fund_codes]
            for fut in concurrent.futures.as_completed(futures):
                code, df, err = fut.result()
                if err is not None:
                    errors.append({"基金代码": code, "error": err})
                elif df is not None and not df.empty:
                    frames.append(df)

        if frames:
            combined = pd.concat(frames, ignore_index=True)
        else:
            combined = pd.DataFrame(columns=empty_columns)

        combined["snapshot_date"] = snapshot_date
        return combined, errors
