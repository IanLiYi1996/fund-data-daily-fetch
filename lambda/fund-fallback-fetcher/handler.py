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

# 每万份收益 is quoted to 4 decimals, so anything past rounding is a real
# disagreement. Set above 1e-4 to avoid rewriting rows over the last digit.
MONEY_NAV_TOLERANCE = float(os.environ.get("MONEY_NAV_TOLERANCE", "0.0005"))
# The money-fund verification pass costs ~977 requests; skip it via env when
# debugging a slow run rather than editing code.
VERIFY_MONEY_FUNDS = os.environ.get("VERIFY_MONEY_FUNDS", "1") != "0"
# How far back the money check re-verifies. Must exceed 1 day: the snapshot
# endpoint rewrites YESTERDAY too, so a target-day-only check gets undone by
# the next night's run. 5 covers a weekend plus a day either side.
MONEY_VERIFY_LOOKBACK_DAYS = int(os.environ.get("MONEY_VERIFY_LOOKBACK_DAYS", "5"))


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


def _is_public_reit(fund_type: Optional[str]) -> bool:
    """True for exchange-listed public REITs (fund_name.fund_type "Reits").

    Deliberately NOT "QDII-REITs": those are open-end funds holding overseas
    REIT shares and publish a NAV every trading day like any other QDII.
    """
    return bool(fund_type) and str(fund_type).startswith("Reits")


def _quarantine_reit_rows(
    merged: pd.DataFrame, reit_codes: set, target: date,
) -> pd.DataFrame:
    """Drop REIT rows dated the target day; they must survive a day upstream.

    lsjz sometimes publishes, on the evening of a trading day, a row for a
    public REIT dated that day and carrying the fund's last disclosed NAV, then
    withdraws it hours or days later. Written as-is those rows said "508600 has
    a NAV for 2026-09-30" when upstream had nothing for that date — 12 such
    rows across 6 REITs by 2026-10-09, and the reconcile check could only ever
    catch the ones it happened to sample. REITs disclose NAV quarterly, so a
    same-day row is far more likely to be one of these than a real disclosure.

    A quarantine rather than a skip: the 14-day fetch window re-fetches the
    fund the next night, and a row upstream still carries then is written
    then. Real disclosures land one day late; transient rows never land.
    """
    if merged.empty or not reit_codes:
        return merged
    drop = merged.fund_code.isin(reit_codes) & (merged.trade_date == target)
    return merged[~drop]


# Overseas funds publish NAV late (T+1, T+2), so the target-day gap alone
# misses them: on 2026-10-19 024149 was in the gap but lsjz had no 10-19 yet,
# and by 10-20 the snapshot had written 10-20, so 10-19 was never asked for
# again. A manual 10-19 re-run recovered 12 such QDII rows. Re-check overseas
# funds holding a hole on any of the last few market trading days.
LATE_OVERSEAS_DAYS = 3
# A date counts as a market trading day only if this many funds have a NAV
# on it; ~24k do on a normal day, a handful on exchange holidays.
LATE_MARKET_MIN_FUNDS = 10000


def _is_overseas(fund_name: Optional[str], fund_type: Optional[str]) -> bool:
    """QDII by type or name. Some QDII index funds are typed 指数型-海外股票."""
    t = str(fund_type or "")
    return t.startswith("QDII") or "海外" in t or "QDII" in str(fund_name or "")


def _late_overseas_codes(
    universe: pd.DataFrame, pairs: set, end_date: date,
    n_days: int = LATE_OVERSEAS_DAYS,
) -> set:
    """Overseas codes with a NAV hole on one of the n trading days before end_date.

    ``pairs`` is (fund_code, trade_date) holding a NAV. Funds with no NAV at
    all in the window are skipped: they are dormant or new, not late.
    """
    per_day: Dict[date, int] = {}
    for _, d in pairs:
        per_day[d] = per_day.get(d, 0) + 1
    days = sorted(
        (d for d, n in per_day.items()
         if d < end_date and n >= LATE_MARKET_MIN_FUNDS),
        reverse=True,
    )[:n_days]
    if not days:
        return set()
    window = set(days) | {end_date}
    have: Dict[str, set] = {}
    for c, d in pairs:
        if d in window:
            have.setdefault(c, set()).add(d)
    out = set()
    for code, name, ftype in zip(universe.fund_code, universe.fund_name,
                                 universe.fund_type):
        got = have.get(code)
        if got and _is_overseas(name, ftype) and any(d not in got for d in days):
            out.add(code)
    return out


def _get_missing_universe(
    catalog, end_date: date, start_date: date,
) -> pd.DataFrame:
    """Funds with no fund_daily row ON end_date, plus late overseas funds.

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
    # Full universe for the late-overseas check, before weekend scoping.
    fn_all = (fn_latest if "fund_type" in fn_latest.columns
              else fn_latest.assign(fund_type=None))

    # On non-trading days only money-market funds accrue, so scoping the gap
    # to the full universe asks for ~27k codes and guarantees a timeout — the
    # 2026-08-08 run hit exactly that. Restrict to 货币型 instead.
    if end_date.weekday() >= 5 and "fund_type" in fn_latest.columns:
        fn_latest = fn_latest[
            fn_latest.fund_type.fillna("").str.startswith("货币型")
        ].reset_index(drop=True)
        logger.info(f"non-trading day: scoped to {len(fn_latest):,} money-market funds")

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

    if "fund_type" not in fn_latest.columns:
        fn_latest = fn_latest.assign(fund_type=None)
    gap = fn_latest[~fn_latest.fund_code.isin(covered)]

    late = _late_overseas_codes(
        fn_all,
        _existing_pairs(catalog, end_date - timedelta(days=10), end_date),
        end_date,
    ) - set(gap.fund_code)
    if late:
        logger.info(f"late overseas: re-checking {len(late)} QDII funds with recent holes")
        gap = pd.concat([gap, fn_all[fn_all.fund_code.isin(late)]])
    return gap[["fund_code", "fund_name", "fund_type"]].reset_index(drop=True)


def _existing_pairs(catalog, start_date: date, end_date: date) -> set:
    """(fund_code, trade_date) pairs that already hold a NAV in the window.

    Used to keep the fallback from re-appending rows it fetched on previous
    days. Pairs with a NULL NAV are deliberately excluded so they remain
    eligible for a real value.
    """
    arrow = catalog.load_table((DATABASE, "fund_daily")).scan(
        row_filter=And(
            GreaterThanOrEqual("trade_date", start_date),
            LessThanOrEqual("trade_date", end_date),
        ),
        selected_fields=("fund_code", "trade_date", "unit_nav"),
    ).to_arrow()
    return {
        (c, d)
        for c, d, v in zip(arrow["fund_code"].to_pylist(),
                           arrow["trade_date"].to_pylist(),
                           arrow["unit_nav"].to_pylist())
        if v is not None
    }


def _verify_money_funds(
    catalog, target: date, writer: "IcebergWriter",
) -> Dict[str, Any]:
    """Re-assert money-market NAVs from lsjz, writing only where we disagree.

    The daily snapshot endpoint reports 每万份收益 for some money funds as an
    accrual covering everything since their last publication, not for the one
    day the row is labelled with. Written as-is that overstates a single day:
    measured 2026-08-14..08-20, 13 of 6,303 money-fund rows disagreed with the
    history endpoint — 2x on a Sunday (`022605` 0.674 vs 0.337), 3x on a Friday
    (`000331` 0.739 vs 0.2468), and some non-integer ratios.

    An earlier pass corrected 11 such rows by hand. That treated the symptom:
    nothing in the write path changed, so it recurred the following weekend.
    This is the cause fix — lsjz is the source of record (it publishes one row
    per day, including Saturday and Sunday), so its value wins.

    Only ~977 codes are money-market, so re-checking all of them costs about a
    thousand requests. Rows are written ONLY where the values differ, which
    keeps the daily volume at a handful rather than ~977 duplicates; the
    ingested_at tiebreak then makes the new row authoritative in both the export
    and weekly compaction, so no delete is required.
    """
    name_tbl = catalog.load_table((DATABASE, "fund_name"))
    fn = name_tbl.scan().to_arrow().to_pandas()
    fn = fn[fn.snapshot_date == fn.snapshot_date.max()].drop_duplicates("fund_code")
    if "fund_type" not in fn.columns:
        return {"checked": 0, "corrected": 0}
    money = fn[fn.fund_type.fillna("").str.startswith("货币型")]
    if money.empty:
        return {"checked": 0, "corrected": 0}

    # Check a WINDOW, not just the target day. The snapshot endpoint returns
    # today AND yesterday, so the run on day D rewrites day D-1 — and it
    # rewrites it with the same cumulative figure. A target-day-only check
    # therefore verifies D, then the next night's run silently re-breaks D and
    # nothing looks at it again: 004904@2026-08-23 held a single row stamped
    # 2026-08-24 17:16, written by the following day's collection, and the
    # 2026-08-21 fix never saw it. Because the correction wins on ingested_at,
    # a LATER bad write also beats an EARLIER correction, so re-checking the
    # window is what makes the fix durable rather than momentary.
    #
    # lsjz accepts a date range, so the whole window still costs one request
    # per fund.
    window_start = target - timedelta(days=MONEY_VERIFY_LOOKBACK_DAYS)
    held = catalog.load_table((DATABASE, "fund_daily")).scan(
        row_filter=And(
            GreaterThanOrEqual("trade_date", window_start),
            LessThanOrEqual("trade_date", target),
        ),
        selected_fields=("fund_code", "trade_date", "unit_nav", "ingested_at"),
    ).to_arrow().to_pandas()
    held = held[held.unit_nav.notna()]
    if held.empty:
        return {"checked": 0, "corrected": 0}
    # Compare against the row the consumer actually gets — newest write per
    # (code, date) — or a stale duplicate would look like a live discrepancy
    # and get "corrected" on every single run.
    held = held.sort_values("ingested_at", na_position="first")
    ours = {
        (r.fund_code, r.trade_date): r.unit_nav
        for _, r in held.iterrows()
    }

    money_codes = set(money.fund_code)
    items = [
        (c, n) for c, n in zip(money.fund_code, money.fund_name)
        if c in money_codes
    ]
    frames: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=FALLBACK_WORKERS) as ex:
        futs = {
            ex.submit(
                _fetch_one, it, window_start.isoformat(), target.isoformat(),
            ): it
            for it in items
        }
        for fut in as_completed(futs):
            code, df, _err = fut.result()
            if df is None or df.empty:
                continue
            bad = df[df.apply(
                lambda r: (
                    (code, r.trade_date) in ours
                    and r.unit_nav is not None
                    and not pd.isna(r.unit_nav)
                    and abs(float(r.unit_nav)
                            - float(ours[(code, r.trade_date)]))
                    > MONEY_NAV_TOLERANCE
                ),
                axis=1,
            )] if not df.empty else df
            if not bad.empty:
                frames.append(bad)

    if not frames:
        return {"checked": len(items), "corrected": 0}

    merged = pd.concat(frames, ignore_index=True)
    writer.write("fund_daily", merged, fetch_date=target)
    logger.info(
        f"money verify: corrected {len(merged)} rows over "
        f"{window_start}..{target} across {len(items)} funds: "
        + ", ".join(f"{r.fund_code}@{r.trade_date}"
                    for _, r in merged.head(10).iterrows())
    )
    return {
        "checked": len(items),
        "window": f"{window_start}..{target}",
        "corrected": int(len(merged)),
        "codes": [f"{r.fund_code}@{r.trade_date}"
                  for _, r in merged.head(20).iterrows()],
    }


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

    writer = IcebergWriter(
        database=DATABASE, warehouse=WAREHOUSE, subprocess_mode=False,
    )

    # Runs before the gap fill and on every exit path: a day can have no gaps
    # at all and still hold a wrong money-market value, because that defect is
    # about the VALUE the snapshot endpoint reported, not about a missing row.
    # Best-effort — a failure here must not cost the gap fill.
    money: Dict[str, Any] = {"checked": 0, "corrected": 0}
    if VERIFY_MONEY_FUNDS:
        try:
            money = _verify_money_funds(catalog, end_date, writer)
        except Exception as e:
            logger.warning(f"money verify failed: {type(e).__name__}: {e}")
            money = {"checked": 0, "corrected": 0, "error": f"{type(e).__name__}"}

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
            "money_verify": money,
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
            "money_verify": money,
            "elapsed_seconds": (datetime.utcnow() - started).total_seconds(),
        }

    merged = pd.concat(frames, ignore_index=True)

    # Write only rows we don't already hold. The fetch window spans 14 days so
    # hold-period products' disclosure dates fall inside it, but appending the
    # whole window every day duplicated rows we already had: fund_daily is
    # append-mode, so by 2026-08-07 the table carried 71,889 rows for 25,649
    # funds — nearly 3x — and 319,234 duplicates overall. Weekly compaction
    # dedups, but daily accumulation outpaced it.
    #
    # Keep the target day plus any earlier date genuinely missing a NAV, so
    # periodic-open funds still get backfilled the first time we see their
    # disclosure.
    existing = _existing_pairs(catalog, start_date, end_date)
    before = len(merged)
    merged = merged[
        ~merged.apply(
            lambda r: (r.fund_code, r.trade_date) in existing, axis=1
        )
    ]
    # REIT rows dated today wait a day before we trust them (see the helper).
    reit_codes = set(missing.fund_code[missing.fund_type.map(_is_public_reit)])
    after_existing = len(merged)
    merged = _quarantine_reit_rows(merged, reit_codes, end_date)
    quarantined = after_existing - len(merged)
    logger.info(
        f"Recovered {merged.fund_code.nunique():,} funds, {len(merged):,} rows "
        f"(dropped {before - after_existing:,} already present, "
        f"quarantined {quarantined} same-day REIT rows)"
    )
    if merged.empty:
        return {
            "statusCode": 200,
            "downloader": "fund-fallback",
            "success": True,
            "missing": len(missing),
            "fetched": 0,
            "rows_appended": 0,
            "money_verify": money,
            "elapsed_seconds": (datetime.utcnow() - started).total_seconds(),
        }

    result = writer.write("fund_daily", merged, fetch_date=end_date)

    return {
        "statusCode": 200,
        "downloader": "fund-fallback",
        "success": True,
        "missing": len(missing),
        "fetched": int(merged.fund_code.nunique()),
        "rows_appended": int(result.get("rows_appended", result.get("rows_inserted", 0))),
        "quarantined_reit_rows": int(quarantined),
        "empty": empty,
        "errors": err,
        "money_verify": money,
        "elapsed_seconds": (datetime.utcnow() - started).total_seconds(),
    }
