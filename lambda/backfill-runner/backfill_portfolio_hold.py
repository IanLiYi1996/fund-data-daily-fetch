"""Backfill / refresh fund_portfolio_hold Iceberg table via akshare.

akshare's fund_portfolio_hold_em(symbol, date=YEAR) returns per-fund
quarterly top-holdings for a given year. This script iterates every
fund_code in the pipeline universe, calls the endpoint for each requested
year (default: current year + last year), and appends normalized rows
to the Iceberg table fund_data_lake.fund_portfolio_hold.

Runtime characteristics (measured):
- Serial per-fund latency: 1-2s
- 4-worker parallel: ~30 funds/min
- Full universe (~19k equity-like funds) × 2 years: ~10-15h wall clock

Called via Fargate (task family FundHistoryBackfill) with:
    --mode portfolio [--years 2026,2025] [--limit N]

Resume semantics: skip fund_codes already present in the target Iceberg
table (any report_date), so partial runs converge on subsequent tries.
This is the same idempotency contract used by the one-off script that
seeded the initial data.
"""
from __future__ import annotations

import argparse
import os
import re
import socket
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

# Force per-request timeout so a hung upstream can't stall a worker
# indefinitely. 45s comfortably covers a slow-but-alive call while
# still letting the pool make progress.
socket.setdefaulttimeout(45)
_orig_request = requests.Session.request
def _bounded_request(self, method, url, **kw):
    kw.setdefault("timeout", 45)
    return _orig_request(self, method, url, **kw)
requests.Session.request = _bounded_request  # type: ignore[assignment]

import akshare as ak  # noqa: E402

# Same layout as backfill_fund_history: shared/ ships alongside in the image
_here = Path(__file__).resolve().parent
for _cand in (_here.parent, _here / "..", Path("lambda"), Path("/app")):
    _cand = _cand.resolve() if hasattr(_cand, "resolve") else _cand
    if (_cand / "shared").is_dir() and str(_cand) not in sys.path:
        sys.path.insert(0, str(_cand))
        break
from shared.storage.iceberg_writer import IcebergWriter  # noqa: E402
from pyiceberg.catalog import load_catalog  # noqa: E402
from pyiceberg.expressions import EqualTo  # noqa: E402


DEFAULT_BUCKET = os.environ.get(
    "S3_BUCKET", "fund-data-pipeline-463470973226-us-east-1"
)
DEFAULT_S3_PREFIX = os.environ.get("S3_PREFIX", "fund-data-pipeline/")
DEFAULT_WAREHOUSE = os.environ.get(
    "WAREHOUSE_PATH", f"s3://{DEFAULT_BUCKET}/{DEFAULT_S3_PREFIX}iceberg/"
)
DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Map "YYYY年N季度..." string → last day of that quarter.
_QUARTER_END = {
    (2024, 1): date(2024, 3, 31), (2024, 2): date(2024, 6, 30),
    (2024, 3): date(2024, 9, 30), (2024, 4): date(2024, 12, 31),
    (2025, 1): date(2025, 3, 31), (2025, 2): date(2025, 6, 30),
    (2025, 3): date(2025, 9, 30), (2025, 4): date(2025, 12, 31),
    (2026, 1): date(2026, 3, 31), (2026, 2): date(2026, 6, 30),
    (2026, 3): date(2026, 9, 30), (2026, 4): date(2026, 12, 31),
    (2027, 1): date(2027, 3, 31), (2027, 2): date(2027, 6, 30),
    (2027, 3): date(2027, 9, 30), (2027, 4): date(2027, 12, 31),
}
_QUARTER_RE = re.compile(r"^(\d{4})年(\d)季度")


def _parse_quarter(s: str):
    m = _QUARTER_RE.match(str(s))
    if not m:
        return None
    y, q = int(m.group(1)), int(m.group(2))
    return _QUARTER_END.get((y, q))


def _fetch_one(code_name, years):
    code, name = code_name
    frames = []
    err = None
    for yr in years:
        try:
            df = ak.fund_portfolio_hold_em(symbol=code, date=str(yr))
            if df is None or df.empty:
                continue
            df = df.copy()
            df["report_date"] = df["季度"].map(_parse_quarter)
            df = df[df.report_date.notna()]
            if df.empty:
                continue
            df["fund_code"] = code
            df = df.rename(columns={
                "股票代码": "holding_code",
                "股票名称": "holding_name",
                "占净值比例": "weight_pct",
                "持股数": "shares",
                "持仓市值": "market_value",
            })
            # akshare returns 持股数 in 万股, 持仓市值 in 万元 — scale up.
            df["shares"] = pd.to_numeric(df["shares"], errors="coerce") * 10000
            df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce") * 10000
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce")
            df = df[[
                "fund_code", "report_date", "holding_code", "holding_name",
                "weight_pct", "shares", "market_value",
            ]]
            frames.append(df)
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:80]}"
    if not frames:
        return code, None, err or "no-data"
    return code, pd.concat(frames, ignore_index=True), None


def _build_universe(warehouse: str) -> pd.DataFrame:
    """Load fund_name latest snapshot, filter to equity-like fund names.

    Money-market / pure-bond / REITs funds do not have "股票投资明细"
    disclosures; skipping them avoids ~40% wasted API calls.
    """
    cat = load_catalog("glue", **{
        "type": "glue", "glue.region": DEFAULT_REGION, "warehouse": warehouse,
    })
    tbl = cat.load_table(("fund_data_lake", "fund_name"))
    fn = tbl.scan().to_arrow().to_pandas()
    latest = fn.snapshot_date.max()
    fn_latest = (fn[fn.snapshot_date == latest]
                 .drop_duplicates("fund_code")
                 .reset_index(drop=True))

    non_equity = [
        "债券", "纯债", "短债", "中债", "长债", "信用债", "利率债", "转债", "政金债",
        "货币", "现金", "增利", "余额宝", "天天", "日日", "日利宝", "薪金", "7日",
        "REITs", "REIT", "不动产",
    ]
    pattern = "|".join(non_equity)
    equity = fn_latest[~fn_latest.fund_name.fillna("").str.contains(pattern, regex=True)]
    return equity[["fund_code", "fund_name"]].reset_index(drop=True)


def _load_done_set(
    catalog, table_name: str, target_quarter: Optional[date] = None,
) -> set[str]:
    """Return the set of fund_codes already loaded for the target quarter.

    Historical quarters are stable disclosures — once a fund_code has
    landed a row for a given `report_date`, that pair is frozen. The
    only interesting resume-skip is per-quarter: codes that already have
    a row for the newest quarter don't need re-fetching, but codes only
    seen in older quarters DO (they may have disclosed the new quarter).

    When ``target_quarter`` is None (legacy behavior), skip any code
    with any row.
    """
    try:
        tbl = catalog.load_table(("fund_data_lake", table_name))
        if target_quarter is None:
            codes = tbl.scan(
                selected_fields=("fund_code",),
            ).to_arrow().to_pandas()
        else:
            codes = tbl.scan(
                row_filter=EqualTo("report_date", target_quarter),
                selected_fields=("fund_code",),
            ).to_arrow().to_pandas()
        return set(codes.fund_code.unique())
    except Exception:
        return set()


def _resolve_target_quarter(spec: Optional[str]) -> date:
    """Return the report_date (quarter-end) implied by ``spec`` or today.

    ``spec`` accepts ``YYYY-QN`` (e.g. ``2026-Q3``). Absent, we pick the
    most recent quarter whose disclosure window has opened — the
    quarterly EventBridge rule runs on the 25th of Jan/Apr/Jul/Oct, so
    on those dates the previous quarter's disclosures have finished.
    """
    if spec:
        m = re.match(r"^(\d{4})-?[Qq]([1-4])$", spec)
        if not m:
            raise ValueError(f"invalid --target-quarter: {spec}")
        y, q = int(m.group(1)), int(m.group(2))
        return _QUARTER_END[(y, q)]

    today = date.today()
    year = today.year
    if today.month <= 3:
        return _QUARTER_END[(year - 1, 4)]
    if today.month <= 6:
        return _QUARTER_END[(year, 1)]
    if today.month <= 9:
        return _QUARTER_END[(year, 2)]
    return _QUARTER_END[(year, 3)]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    p.add_argument("--database", default="fund_data_lake")
    p.add_argument("--years", default=None,
                   help="Comma-separated years (default: current + last)")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--batch", type=int, default=100,
                   help="Flush to Iceberg every N completed funds")
    p.add_argument("--limit", type=int, default=0,
                   help="Cap total funds processed (0=all)")
    p.add_argument("--force", action="store_true",
                   help="Ignore resume set and re-fetch every fund")
    p.add_argument("--target-quarter", default=None,
                   help=("Quarter to refresh, e.g. '2026-Q3'. Resume skips "
                         "fund_codes that already have a row for this quarter; "
                         "codes missing only THIS quarter's disclosure get "
                         "re-fetched even if older rows are present. Default: "
                         "derive from today (last quarter whose window has "
                         "opened). Pass '--target-quarter all' to fall back "
                         "to full-universe backfill semantics."))
    args = p.parse_args()

    if args.target_quarter and args.target_quarter.lower() == "all":
        target_quarter: Optional[date] = None
        print("target_quarter: (full history)", flush=True)
    else:
        target_quarter = _resolve_target_quarter(args.target_quarter)
        print(f"target_quarter: {target_quarter}", flush=True)

    if args.years:
        years = [int(x.strip()) for x in args.years.split(",") if x.strip()]
    elif target_quarter is not None:
        # Only the year containing the target quarter needs to be fetched;
        # older quarters are stable and already in the table.
        years = [target_quarter.year]
    else:
        cur = date.today().year
        years = [cur, cur - 1]
    print(f"years: {years}", flush=True)

    universe = _build_universe(args.warehouse)
    print(f"universe (equity-like): {len(universe):,}", flush=True)

    catalog = load_catalog("glue", **{
        "type": "glue", "glue.region": DEFAULT_REGION,
        "warehouse": args.warehouse,
    })
    done = (
        set() if args.force
        else _load_done_set(catalog, "fund_portfolio_hold", target_quarter)
    )
    print(f"already loaded for target (skip): {len(done):,}", flush=True)

    todo = universe[~universe.fund_code.isin(done)]
    if args.limit:
        todo = todo.head(args.limit)
    print(f"todo: {len(todo):,}", flush=True)

    if todo.empty:
        print("nothing to do", flush=True)
        return 0

    writer = IcebergWriter(
        database=args.database, warehouse=args.warehouse, subprocess_mode=False,
    )

    pending: list[pd.DataFrame] = []
    ok = empty = total_rows = 0
    t0 = time.time()

    items = list(zip(todo.fund_code, todo.fund_name))
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_fetch_one, item, years): item for item in items}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                code, df, _err = fut.result(timeout=60)
            except Exception:
                empty += 1
                continue
            if df is None or df.empty:
                empty += 1
            else:
                ok += 1
                total_rows += len(df)
                pending.append(df)
            if i % 20 == 0:
                elapsed = time.time() - t0
                eta_min = (len(items) - i) * elapsed / i / 60
                print(f"  [{i}/{len(items)}] ok={ok} empty={empty} rows={total_rows} "
                      f"elapsed={elapsed:.0f}s ETA={eta_min:.0f}min", flush=True)
            if i % args.batch == 0 and pending:
                merged = pd.concat(pending, ignore_index=True)
                r = writer.write(
                    "fund_portfolio_hold", merged, fetch_date=date.today(),
                )
                print(f"  flush {i}: {len(merged)} rows → {r}", flush=True)
                pending = []

    if pending:
        merged = pd.concat(pending, ignore_index=True)
        r = writer.write("fund_portfolio_hold", merged, fetch_date=date.today())
        print(f"  final: {len(merged)} rows → {r}", flush=True)

    print(f"\nDONE ok={ok} empty={empty} rows={total_rows} "
          f"elapsed={time.time()-t0:.0f}s", flush=True)

    _export_flat_parquet(args.warehouse)
    return 0


def _export_flat_parquet(warehouse: str) -> None:
    """Dedup + write flat parquet under fund/_history/ so it replicates
    to financial-dataset-mx alongside fund_manager_history /
    fund_scale_history. This is the file Mengxin's team reads directly.
    """
    import tempfile
    import boto3
    import duckdb

    bucket = DEFAULT_BUCKET
    prefix = DEFAULT_S3_PREFIX  # "fund-data-pipeline/"
    key = f"{prefix}fund/_history/fund_portfolio_hold_history.parquet"
    src_glob = f"s3://{bucket}/{prefix}iceberg/fund_data_lake.db/fund_portfolio_hold/data/**/*.parquet"

    print(f"\nExporting flat parquet → s3://{bucket}/{key}", flush=True)
    con = duckdb.connect()
    con.sql(f"CREATE SECRET s3 (TYPE s3, PROVIDER credential_chain, REGION '{DEFAULT_REGION}');")

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = f.name
    try:
        con.sql(f"""
            COPY (
                WITH ranked AS (
                    SELECT *, row_number() OVER (
                        PARTITION BY fund_code, report_date, holding_code
                        ORDER BY report_date DESC
                    ) AS rn
                    FROM read_parquet('{src_glob}')
                )
                SELECT fund_code, report_date, holding_code, holding_name,
                       weight_pct, shares, market_value
                FROM ranked WHERE rn = 1
            ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        rows = con.sql(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
        funds = con.sql(f"SELECT count(DISTINCT fund_code) FROM read_parquet('{tmp}')").fetchone()[0]
        size = os.path.getsize(tmp)
        print(f"  {rows:,} rows / {funds:,} funds / {size:,} bytes", flush=True)

        boto3.client("s3", region_name=DEFAULT_REGION).upload_file(tmp, bucket, key)
        print(f"  uploaded: s3://{bucket}/{key}", flush=True)
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
