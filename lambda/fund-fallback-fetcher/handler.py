"""Fund-daily fallback fetcher.

Runs AFTER the main fund-fetcher Map completes. Compares:
  - universe = fund_name latest snapshot
  - covered  = fund_daily rows written for today (or the requested date)

Any fund in the gap gets fetched from 天天基金 REST API
(api.fund.eastmoney.com/f10/lsjz), whose coverage extends beyond
akshare's daily snapshot endpoints. Empirically recovers ~940 out of
~1900 daily-missing funds — mostly money-market funds, back-end share
classes, and periodic-open funds whose disclosure date isn't today.

Event:
    {"trade_date": "2026-06-16"}   # optional; defaults to today
    {"lookback_days": 5}           # optional; also fills gap for the
                                   # N previous trading days (default 3)

Idempotent: fund_daily is append-mode; weekly compaction dedups.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Dict

import pandas as pd
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

from shared.storage.iceberg_writer import IcebergWriter
from shared.utils.logger import get_logger

logger = get_logger(__name__)

WAREHOUSE = os.environ["WAREHOUSE_PATH"]
DATABASE = "fund_data_lake"
REGION = os.environ.get("AWS_REGION", "us-east-1")

_HEADERS = {
    "Referer": "https://fundf10.eastmoney.com/",
    "User-Agent": "Mozilla/5.0",
}


def _fetch_one(code_name, start_date: str, end_date: str):
    code, name = code_name
    try:
        r = requests.get(
            f"https://api.fund.eastmoney.com/f10/lsjz"
            f"?fundCode={code}&pageIndex=1&pageSize=50"
            f"&startDate={start_date}&endDate={end_date}",
            headers=_HEADERS,
            timeout=15,
        )
        rows = r.json().get("Data", {}).get("LSJZList", []) or []
    except Exception as e:
        return code, None, f"{type(e).__name__}: {str(e)[:60]}"

    if not rows:
        return code, None, "empty"

    df = pd.DataFrame(rows)
    df = df[df.FSRQ.notna() & (df.FSRQ != "")]
    if df.empty:
        return code, None, "empty"

    df["fund_code"] = code
    df["fund_name"] = name
    df["trade_date"] = pd.to_datetime(df.FSRQ).dt.date
    df["unit_nav"] = pd.to_numeric(df.DWJZ, errors="coerce")
    df["accum_nav"] = pd.to_numeric(df.LJJZ, errors="coerce")
    df["daily_return_pct"] = pd.to_numeric(df.JZZZL, errors="coerce")
    df["subscription_status"] = df.SGZT
    df["redemption_status"] = df.SHZT
    df = df[df.unit_nav.notna()]
    if df.empty:
        return code, None, "no-nav"

    return code, df[[
        "fund_code", "fund_name", "trade_date", "unit_nav", "accum_nav",
        "daily_return_pct", "subscription_status", "redemption_status",
    ]], None


def _get_missing_universe(
    catalog, end_date: date, start_date: date,
) -> pd.DataFrame:
    """Return fund_code + fund_name for funds not in fund_daily for [start, end]."""
    name_tbl = catalog.load_table((DATABASE, "fund_name"))
    fn = name_tbl.scan().to_arrow().to_pandas()
    latest = fn.snapshot_date.max()
    fn_latest = (
        fn[fn.snapshot_date == latest].drop_duplicates("fund_code").reset_index(drop=True)
    )

    daily_tbl = catalog.load_table((DATABASE, "fund_daily"))
    covered_arrow = daily_tbl.scan(
        row_filter=And(
            GreaterThanOrEqual("trade_date", start_date),
            LessThanOrEqual("trade_date", end_date),
        ),
        selected_fields=("fund_code",),
    ).to_arrow()
    covered = set(covered_arrow["fund_code"].to_pylist())

    return fn_latest[~fn_latest.fund_code.isin(covered)][["fund_code", "fund_name"]]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    started = datetime.utcnow()

    end_date_str = event.get("trade_date")
    end_date = (
        date.fromisoformat(end_date_str) if end_date_str else date.today()
    )
    lookback = int(event.get("lookback_days", 3))
    start_date = end_date - timedelta(days=lookback)

    logger.info(
        f"Fallback fetch: window {start_date.isoformat()} → {end_date.isoformat()}"
    )

    catalog = load_catalog("glue", **{
        "type": "glue", "glue.region": REGION, "warehouse": WAREHOUSE,
    })
    missing = _get_missing_universe(catalog, end_date, start_date)
    logger.info(f"Missing funds in window: {len(missing):,}")

    if missing.empty:
        return {
            "statusCode": 200,
            "downloader": "fund-fallback",
            "success": True,
            "missing": 0,
            "fetched": 0,
            "rows_appended": 0,
            "elapsed_seconds": (datetime.utcnow() - started).total_seconds(),
        }

    items = list(zip(missing.fund_code, missing.fund_name))
    frames: list[pd.DataFrame] = []
    empty = err = 0

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {
            ex.submit(
                _fetch_one, it, start_date.isoformat(), end_date.isoformat()
            ): it
            for it in items
        }
        for fut in as_completed(futs):
            code, df, error = fut.result()
            if df is None or df.empty:
                if error and not error.startswith("empty"):
                    err += 1
                else:
                    empty += 1
                continue
            frames.append(df)

    if not frames:
        logger.info(f"No fallback data recovered. empty={empty} err={err}")
        return {
            "statusCode": 200,
            "downloader": "fund-fallback",
            "success": True,
            "missing": len(missing),
            "fetched": 0,
            "rows_appended": 0,
            "elapsed_seconds": (datetime.utcnow() - started).total_seconds(),
        }

    merged = pd.concat(frames, ignore_index=True)
    logger.info(
        f"Recovered {merged.fund_code.nunique():,} funds, {len(merged):,} rows"
    )

    writer = IcebergWriter(
        database=DATABASE, warehouse=WAREHOUSE, subprocess_mode=False,
    )
    result = writer.write("fund_daily", merged, fetch_date=end_date)

    return {
        "statusCode": 200,
        "downloader": "fund-fallback",
        "success": True,
        "missing": len(missing),
        "fetched": int(merged.fund_code.nunique()),
        "rows_appended": int(result.get("rows_appended", result.get("rows_inserted", 0))),
        "empty": empty,
        "errors": err,
        "elapsed_seconds": (datetime.utcnow() - started).total_seconds(),
    }
