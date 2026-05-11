#!/usr/bin/env python3
"""Backfill historical NAV for open-end funds from akshare into Iceberg.

For each fund code listed in the ``fund_name`` raw parquet (~20 000 funds),
pull the complete NAV history via ``ak.fund_open_fund_info_em`` (both
"单位净值走势" and "累计净值走势"), merge on date, and upsert into the
``fund_daily`` Iceberg table.

Design notes:
- Resumable: maintains a progress file (``~/.cache/fund_backfill_progress.json``
  by default) with the set of already-processed codes. Safe to Ctrl-C and
  re-run.
- Batched writes: every ``--batch-size`` funds (default 100), concatenate
  and upsert into Iceberg in one call. Reduces catalog round-trips.
- Rate-limited: a small sleep per request (default 0.3 s) to avoid
  overloading akshare / getting IP banned.
- Parallel fetches: ``--workers`` threads (default 4) run concurrent
  akshare requests; writes are serialized on the main thread.
- Idempotent: Iceberg upsert is keyed on (fund_code, trade_date), so a
  re-run on partially-filled data simply no-ops for existing rows.

Prerequisites:
- AWS credentials with S3 + Glue write on the fund-data-pipeline prefix
- Local akshare install (in dev deps)

Usage:
    # Dry run: print what would be done, don't actually fetch/write
    uv run python scripts/backfill_fund_history.py --dry-run --limit 5

    # Backfill 10 funds (for a smoke test)
    uv run python scripts/backfill_fund_history.py --limit 10

    # Full backfill (takes ~4-8 hours for 20k funds depending on network)
    uv run python scripts/backfill_fund_history.py

    # Resume with different batch size / more workers
    uv run python scripts/backfill_fund_history.py --workers 8 --batch-size 200

    # Only backfill specific codes (useful for single-fund testing)
    uv run python scripts/backfill_fund_history.py --codes 000001,110022
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd

sys.path.insert(0, "lambda")
from shared.storage.iceberg_writer import IcebergWriter  # noqa: E402


DEFAULT_BUCKET = "fsi-investmentadvisory-data-463470973226-us-east-1"
DEFAULT_S3_PREFIX = "fund-data-pipeline/"
DEFAULT_PROGRESS = Path.home() / ".cache" / "fund_backfill_progress.json"


@dataclass
class Progress:
    done: set[str]
    failed: dict[str, str]
    started_at: str

    @classmethod
    def load(cls, path: Path) -> "Progress":
        if path.exists():
            raw = json.loads(path.read_text())
            return cls(
                done=set(raw.get("done", [])),
                failed=dict(raw.get("failed", {})),
                started_at=raw.get("started_at", datetime.now().isoformat()),
            )
        return cls(done=set(), failed={}, started_at=datetime.now().isoformat())

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "done": sorted(self.done),
            "failed": self.failed,
            "started_at": self.started_at,
            "updated_at": datetime.now().isoformat(),
            "total_done": len(self.done),
            "total_failed": len(self.failed),
        }, ensure_ascii=False, indent=2))


def load_fund_codes(bucket: str, s3_prefix: str,
                    region: str) -> list[tuple[str, str]]:
    """Return [(fund_code, fund_name), ...] from today's fund_name parquet."""
    s3 = boto3.client("s3", region_name=region)
    today = date.today().isoformat()
    key = f"{s3_prefix}fund/{today}/fund_name.parquet"
    print(f"Loading fund list from s3://{bucket}/{key}")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        # Fall back to latest available date
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{s3_prefix}fund/",
                                  Delimiter="/")
        dates = [p["Prefix"].split("/")[-2]
                 for p in resp.get("CommonPrefixes", [])]
        if not dates:
            raise RuntimeError("No fund_name parquet found")
        latest = sorted(dates)[-1]
        key = f"{s3_prefix}fund/{latest}/fund_name.parquet"
        print(f"  (today not present, using {key})")
        obj = s3.get_object(Bucket=bucket, Key=key)

    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    # akshare returns 基金代码 + 基金简称
    code_col = "基金代码" if "基金代码" in df.columns else df.columns[0]
    name_col = "基金简称" if "基金简称" in df.columns else df.columns[1]
    pairs = list(zip(df[code_col].astype(str), df[name_col].astype(str)))
    print(f"  found {len(pairs)} funds")
    return pairs


def fetch_one_fund(code: str, name: str, sleep_between: float) -> pd.DataFrame:
    """Pull full NAV history for one fund; merge unit + accumulated.

    Returns a DataFrame with columns: 基金代码, 基金简称, 净值日期,
    单位净值, 累计净值, 日增长率 — i.e. shaped like a fund_open_fund_daily_em
    row so the existing IcebergWriter normalizer handles it.
    """
    import akshare as ak

    unit = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
    time.sleep(sleep_between)
    accum = ak.fund_open_fund_info_em(symbol=code, indicator="累计净值走势")

    if unit is None or len(unit) == 0:
        return pd.DataFrame()

    # Merge on 净值日期
    merged = unit.merge(accum, on="净值日期", how="left")
    merged.insert(0, "基金代码", code)
    merged.insert(1, "基金简称", name)
    return merged


def flush_batch(
    writer: IcebergWriter,
    buffer: list[pd.DataFrame],
    logger_prefix: str,
) -> dict:
    """Concatenate buffered DataFrames and upsert into fund_daily."""
    if not buffer:
        return {"skipped": True}
    combined = pd.concat(buffer, ignore_index=True)
    print(f"  {logger_prefix} flushing {len(buffer)} funds, "
          f"{len(combined)} rows → fund_daily...")
    t0 = time.time()
    result = writer.write("fund_daily", combined)
    elapsed = time.time() - t0
    print(f"  {logger_prefix} wrote in {elapsed:.1f}s: {result}")
    return result


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--database", default="fund_data_lake")
    p.add_argument("--progress-file", type=Path, default=DEFAULT_PROGRESS,
                   help="Path to resume checkpoint JSON")
    p.add_argument("--workers", type=int, default=4,
                   help="Parallel akshare fetcher threads")
    p.add_argument("--batch-size", type=int, default=100,
                   help="Concat this many funds before each Iceberg upsert")
    p.add_argument("--sleep", type=float, default=0.3,
                   help="Seconds between akshare calls within a worker")
    p.add_argument("--limit", type=int, default=0,
                   help="Stop after N successfully-processed funds (0=all)")
    p.add_argument("--codes", help="Comma-separated list of fund codes "
                   "(overrides the full list from S3)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be done, don't fetch or write")
    p.add_argument("--reset-progress", action="store_true",
                   help="Delete checkpoint file before starting")
    args = p.parse_args()

    if args.reset_progress and args.progress_file.exists():
        args.progress_file.unlink()
        print(f"Deleted progress file: {args.progress_file}")

    # 1. Resolve fund list
    if args.codes:
        pairs = [(c.strip(), c.strip()) for c in args.codes.split(",")
                 if c.strip()]
    else:
        pairs = load_fund_codes(args.bucket, args.s3_prefix, args.region)

    # 2. Load progress + filter
    progress = Progress.load(args.progress_file)
    print(f"Progress: {len(progress.done)} already done, "
          f"{len(progress.failed)} previously failed")
    todo = [(c, n) for c, n in pairs if c not in progress.done]
    if args.limit > 0:
        todo = todo[: args.limit]
    print(f"Todo: {len(todo)} funds")

    if args.dry_run:
        print("\n(dry run) first 10 funds to process:")
        for c, n in todo[:10]:
            print(f"  {c} {n}")
        return 0

    # 3. Initialize writer (shared across batches)
    warehouse = f"s3://{args.bucket}/{args.s3_prefix}iceberg/"
    writer = IcebergWriter.from_glue(database=args.database, warehouse=warehouse)
    # We're running in-process from a dev/EC2 shell, not Lambda — turn off
    # subprocess mode so we skip fork overhead per batch.
    writer.subprocess_mode = False

    # 4. Fetch + batch-upsert loop
    buffer: list[pd.DataFrame] = []
    t_start = time.time()
    n_processed = 0
    n_empty = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(fetch_one_fund, code, name, args.sleep): (code, name)
            for code, name in todo
        }
        for fut in as_completed(futures):
            code, name = futures[fut]
            try:
                df = fut.result()
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                progress.failed[code] = err[:200]
                print(f"  [FAIL] {code} {name}: {err[:100]}")
                continue
            if df is None or len(df) == 0:
                n_empty += 1
                progress.done.add(code)  # count as done; just no data
                continue
            buffer.append(df)
            progress.done.add(code)
            n_processed += 1

            if len(buffer) >= args.batch_size:
                flush_batch(writer, buffer, f"[{n_processed}/{len(todo)}]")
                buffer = []
                progress.save(args.progress_file)
                eta_s = (time.time() - t_start) / n_processed * (len(todo) - n_processed)
                print(f"  progress saved. ETA ~{eta_s/60:.1f} min")

    # Final flush
    if buffer:
        flush_batch(writer, buffer, "[final]")
    progress.save(args.progress_file)

    elapsed = time.time() - t_start
    print(f"\n=== Done ===")
    print(f"Processed: {n_processed} funds with data")
    print(f"Empty:     {n_empty}")
    print(f"Failed:    {len(progress.failed)}")
    print(f"Elapsed:   {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"Progress file: {args.progress_file}")
    return 0 if not progress.failed else 1


if __name__ == "__main__":
    sys.exit(main())
