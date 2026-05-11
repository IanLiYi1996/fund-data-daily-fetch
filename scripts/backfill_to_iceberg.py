#!/usr/bin/env python3
"""Backfill historical raw parquet from S3 into Iceberg tables.

Run from EC2 or local with AWS credentials configured:
    uv run python scripts/backfill_to_iceberg.py \\
        --bucket fund-data-... \\
        --table fund_daily \\
        --start 2026-01-01 --end 2026-05-08

Or backfill all tables:
    uv run python scripts/backfill_to_iceberg.py --bucket ... --all
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta

import boto3
import pandas as pd

sys.path.insert(0, "lambda")
from shared.schemas import TABLES  # noqa: E402
from shared.storage.iceberg_writer import IcebergWriter  # noqa: E402


def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def backfill_table(s3, writer, bucket: str, table_name: str,
                   start: date, end: date) -> dict:
    spec = TABLES[table_name]
    n_files, n_rows_inserted, n_rows_updated, n_failures = 0, 0, 0, 0

    for d in daterange(start, end):
        key = f"{spec.source_category}/{d.isoformat()}/{table_name}.parquet"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
        except s3.exceptions.NoSuchKey:
            continue
        try:
            df = pd.read_parquet(obj["Body"])
            result = writer.write(table_name, df)
            n_files += 1
            n_rows_inserted += result.get("rows_inserted", 0)
            n_rows_updated += result.get("rows_updated", 0)
            n_rows_inserted += result.get("rows_appended", 0)
            print(f"  {key}: {result}")
        except Exception as e:
            n_failures += 1
            print(f"  {key}: FAIL {e}")

    return {
        "table": table_name,
        "files": n_files,
        "rows_inserted": n_rows_inserted,
        "rows_updated": n_rows_updated,
        "failures": n_failures,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--database", default="fund_data_lake")
    p.add_argument("--start", help="YYYY-MM-DD", default="2025-01-01")
    p.add_argument("--end", help="YYYY-MM-DD",
                   default=datetime.now().date().isoformat())
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--table", help="single table name")
    g.add_argument("--all", action="store_true")
    args = p.parse_args()

    s3 = boto3.client("s3", region_name=args.region)
    writer = IcebergWriter.from_glue(
        database=args.database, warehouse=f"s3://{args.bucket}/iceberg/"
    )

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    targets = list(TABLES) if args.all else [args.table]
    summaries = []
    for t in targets:
        print(f"\n=== {t} ===")
        summary = backfill_table(s3, writer, args.bucket, t, start, end)
        summaries.append(summary)
        print(f"summary: {summary}")

    print("\n=== overall ===")
    for s in summaries:
        print(s)
    return 0


if __name__ == "__main__":
    sys.exit(main())
