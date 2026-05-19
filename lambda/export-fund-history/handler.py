"""Export the current month's fund NAV history to a single parquet file.

Reads s3://bucket/fund-data-pipeline/iceberg/fund_data_lake.db/fund_daily/data/
trade_month=YYYY-MM/*.parquet, dedupes on (fund_code, trade_date), writes one
consolidated parquet to s3://bucket/fund-data-pipeline/fund_history/
trade_month=YYYY-MM/part-0.parquet. S3 Replication then mirrors to
financial-dataset-mx/fund-data-pipeline/fund_history/ for Mengxin's use.

Only the current month is touched in the daily run — older months don't change.
Month can be overridden via event.month='YYYY-MM' for ad-hoc re-export.

Safe to read parquet directly (no iceberg_scan): fund_daily has no delete
files — writers only append/upsert and compaction is the only rewriter, so
every visible row is in some data file.
"""
from __future__ import annotations

import os
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict

import boto3
import duckdb

BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "fund-data-pipeline/")
SRC_DATA = f"s3://{BUCKET}/{S3_PREFIX}iceberg/fund_data_lake.db/fund_daily/data"
DST_PREFIX = f"{S3_PREFIX}fund_history"


def _this_month() -> str:
    t = date.today()
    return f"{t.year:04d}-{t.month:02d}"


def _export_month(ym: str, tmp: Path) -> Dict[str, Any]:
    """Dedupe + write one month's parquet; upload to S3. Returns stats."""
    src_glob = f"{SRC_DATA}/trade_month={ym}/*.parquet"
    dst_key = f"{DST_PREFIX}/trade_month={ym}/part-0.parquet"
    local = tmp / f"{ym}.parquet"

    # Lambda has no $HOME; point DuckDB at our in-image extension cache
    # (populated at Dockerfile build time) and at writable /tmp for any
    # runtime state.
    con = duckdb.connect()
    con.sql(f"SET home_directory='{tmp}';")
    ext_dir = os.environ.get("DUCKDB_EXT_DIR")
    if ext_dir:
        con.sql(f"SET extension_directory='{ext_dir}';")
    con.sql("LOAD httpfs;")
    con.sql("LOAD aws;")
    con.sql("CREATE SECRET s3 (TYPE s3, PROVIDER credential_chain, "
            f"REGION '{os.environ.get('AWS_REGION', 'us-east-1')}');")

    t0 = time.time()
    con.sql(f"""
        COPY (
            WITH ranked AS (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY fund_code, trade_date
                           ORDER BY CASE WHEN unit_nav IS NOT NULL THEN 0 ELSE 1 END,
                                    CASE WHEN accum_nav IS NOT NULL THEN 0 ELSE 1 END
                       ) AS rn
                FROM read_parquet('{src_glob}')
            )
            SELECT fund_code, fund_name, trade_date, unit_nav, accum_nav,
                   daily_return_pct, subscription_status, redemption_status, fee
            FROM ranked WHERE rn = 1
        )
        TO '{local}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    if not local.exists() or local.stat().st_size == 0:
        return {"month": ym, "rows": 0, "bytes": 0, "skipped": "empty"}

    rows = con.sql(f"SELECT COUNT(*) FROM read_parquet('{local}')").fetchone()[0]
    size = local.stat().st_size

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    s3.put_object(
        Bucket=BUCKET,
        Key=dst_key,
        Body=local.read_bytes(),
        ContentType="application/x-parquet",
    )
    local.unlink()
    return {
        "month": ym,
        "rows": rows,
        "bytes": size,
        "s3_key": dst_key,
        "elapsed_seconds": round(time.time() - t0, 2),
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    ym = event.get("month") or _this_month()
    with tempfile.TemporaryDirectory() as td:
        result = _export_month(ym, Path(td))
    result.update({
        "statusCode": 200,
        "downloader": "export-fund-history",
        "success": result.get("rows", 0) > 0 or result.get("skipped") == "empty",
    })
    return result
