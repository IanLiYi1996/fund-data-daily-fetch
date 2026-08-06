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
    {"lookback_days": 5}           # optional; how far back to look for
                                   # a disclosure (default 14, override
                                   # with FALLBACK_LOOKBACK_DAYS)

Idempotent: fund_daily is append-mode; weekly compaction dedups.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

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

# The daily gap is ~5,700 codes at ~1.8s each. At 8 workers that is ~21
# minutes, which overruns Lambda's 15-minute ceiling — the 2026-08-05 run
# timed out and left the day short. 24 workers brings it to ~7 minutes with
# headroom. Upstream tolerated 8 comfortably; if it starts rate-limiting,
# lower this rather than accepting silent truncation, because a timeout
# leaves the day partially filled with no error surfaced anywhere.
FALLBACK_WORKERS = int(os.environ.get("FALLBACK_WORKERS", "24"))


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
    """Funds with no fund_daily row ON end_date.

    The gap is computed for the target day alone, while the FETCH window
    stays [start_date, end_date] — those are deliberately different spans.

    Widening the lookback to 14 days (so hold-period products' disclosure
    dates fall inside the fetch range) originally also widened this
    membership test, which broke it: a money-market or ETF code that got
    filled on any day in the window counted as "covered" and was never
    fetched again. ~2,500 funds silently dropped out of daily coverage
    from 2026-08-03 — Iceberg went from 26.3k to 23.8k codes per day
    while the fetch window still looked healthy.
    """
    name_tbl = catalog.load_table((DATABASE, "fund_name"))
    fn = name_tbl.scan().to_arrow().to_pandas()
    latest = fn.snapshot_date.max()
    fn_latest = (
        fn[fn.snapshot_date == latest].drop_duplicates("fund_code").reset_index(drop=True)
    )

    # "Covered" means a usable NAV, not merely a row. The main snapshot
    # endpoint returns rows for funds it has no value for (both date columns
    # blank), and treating those as covered made the fallback skip them —
    # e.g. 010866 sat at NULL for a week while lsjz had 1.0107..1.0090 the
    # whole time.
    daily_tbl = catalog.load_table((DATABASE, "fund_daily"))
    covered_arrow = daily_tbl.scan(
        row_filter=And(
            GreaterThanOrEqual("trade_date", end_date),
            LessThanOrEqual("trade_date", end_date),
        ),
        selected_fields=("fund_code", "unit_nav"),
    ).to_arrow()
    covered = {
        c for c, v in zip(covered_arrow["fund_code"].to_pylist(),
                          covered_arrow["unit_nav"].to_pylist())
        if v is not None
    }

    return fn_latest[~fn_latest.fund_code.isin(covered)][["fund_code", "fund_name"]]


def _latest_loaded_date(catalog) -> Optional[date]:
    """Newest trade_date present in fund_daily.

    Not date.today(): Lambda runs in UTC, the collection cron fires at
    17:00 UTC, so between 00:00 and 17:00 UTC "today" has no rows at all.
    Using it as the target day makes the entire universe look missing and
    the fallback tries to fetch all ~27k codes.
    """
    tbl = catalog.load_table((DATABASE, "fund_daily"))
    since = date.today() - timedelta(days=10)
    arrow = tbl.scan(
        row_filter=GreaterThanOrEqual("trade_date", since),
        selected_fields=("trade_date",),
    ).to_arrow()
    days = arrow["trade_date"].to_pylist()
    return max(days) if days else None


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    started = datetime.utcnow()

    catalog = load_catalog("glue", **{
        "type": "glue", "glue.region": REGION, "warehouse": WAREHOUSE,
    })

    end_date_str = event.get("trade_date")
    if end_date_str:
        end_date = date.fromisoformat(end_date_str)
    else:
        end_date = _latest_loaded_date(catalog) or date.today()
    # 3 days was too tight. Periodic-open funds (3/6-month-hold FOF, some
    # 定开债) disclose on their own cadence, so a fund whose latest NAV is
    # 10 days old still counts as "missing" every single day while the
    # window never reaches its disclosure date. Measured on the 2026-08-03
    # gap of 714 codes: 36 of them (34 FOF + 2 定开债) were sitting in
    # lsjz the whole time, just outside a 3-day window. 14 days covers a
    # quarterly-hold fund's gap between disclosures without pulling so
    # much history that the run slows down.
    lookback = int(event.get("lookback_days", os.environ.get("FALLBACK_LOOKBACK_DAYS", "14")))
    start_date = end_date - timedelta(days=lookback)

    logger.info(
        f"Fallback fetch: target {end_date.isoformat()} / "
        f"fetch window {start_date.isoformat()} → {end_date.isoformat()}"
    )

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

    with ThreadPoolExecutor(max_workers=FALLBACK_WORKERS) as ex:
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
