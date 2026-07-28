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


def _load_done_set(catalog, table_name: str) -> set[str]:
    """Return the set of fund_codes already present in the target table.

    Used for resume: rerunning after a partial run only fetches the codes
    that haven't landed anything yet. Once a code has any row, it's not
    re-fetched — season updates get handled by force-refresh flag if
    needed later.
    """
    try:
        tbl = catalog.load_table(("fund_data_lake", table_name))
        codes = tbl.scan(selected_fields=("fund_code",)).to_arrow().to_pandas()
        return set(codes.fund_code.unique())
    except Exception:
        return set()


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
    args = p.parse_args()

    if args.years:
        years = [int(x.strip()) for x in args.years.split(",") if x.strip()]
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
    done = set() if args.force else _load_done_set(catalog, "fund_portfolio_hold")
    print(f"already loaded (skip): {len(done):,}", flush=True)

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
    return 0


if __name__ == "__main__":
    sys.exit(main())
