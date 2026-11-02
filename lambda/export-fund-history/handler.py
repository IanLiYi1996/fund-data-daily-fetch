"""Export recent fund NAV history to one consumer parquet file per month.

Reads s3://bucket/fund-data-pipeline/iceberg/fund_data_lake.db/fund_daily/data/
trade_month=YYYY-MM/*.parquet, dedupes on (fund_code, trade_date), writes one
consolidated parquet to s3://bucket/fund-data-pipeline/fund_history/
trade_month=YYYY-MM/part-0.parquet. S3 Replication then mirrors to
financial-dataset-mx/fund-data-pipeline/fund_history/ for Mengxin's use.

Daily runs refresh the current AND previous month, so delayed NAV publication
across a month boundary still reaches consumers. A month with no live source
files is reported as waiting, never populated with fabricated/old-month rows.
Month can be overridden via event.month='YYYY-MM' for ad-hoc re-export.

The file list comes from the Iceberg snapshot, NOT a glob over the data
directory. Globbing was wrong: overwrite-based operations (weekly
compaction, any cleanup pass) write new data files and leave the
superseded ones in place, so a glob silently unions current and orphaned
files. That shipped stale rows to the consumer after every compaction —
and after the 2026-08-04 phantom-weekend cleanup it served ~24k deleted
weekend rows plus pre-cleanup NAVs. DuckDB still does the dedup and
projection; it just gets an explicit file list.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict

import boto3
import duckdb

from shared.quality.export_policy import export_months, month_start

BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "fund-data-pipeline/")
DST_PREFIX = f"{S3_PREFIX}fund_history"
REGION = os.environ.get("AWS_REGION", "us-east-1")


def _duckdb(tmp: Path):
    """DuckDB connection wired for S3 reads inside Lambda.

    Lambda has no $HOME, so both the home and extension directories have to
    be set explicitly or LOAD httpfs fails with a bare "extension not found".
    """
    con = duckdb.connect()
    con.sql(f"SET home_directory='{tmp}';")
    ext_dir = os.environ.get("DUCKDB_EXT_DIR")
    if ext_dir:
        con.sql(f"SET extension_directory='{ext_dir}';")
    con.sql("LOAD httpfs;")
    con.sql("LOAD aws;")
    con.sql("CREATE SECRET s3 (TYPE s3, PROVIDER credential_chain, "
            f"REGION '{REGION}');")
    return con


def _snapshot_files(con) -> list[str]:
    """Data files belonging to one current snapshot, across all months.

    Resolved straight from the Iceberg metadata via boto3 — Glue gives the
    pointer to metadata.json, which lists the manifests, which list the data
    files. Deliberately avoids pyiceberg here: its S3 filesystem extra pulls
    an fsspec pin that conflicts with duckdb in this image, and all we need
    is the file list, not a read path.
    """
    import gzip

    glue = boto3.client("glue", region_name=REGION)
    tbl = glue.get_table(DatabaseName="fund_data_lake", Name="fund_daily")
    metadata_uri = tbl["Table"]["Parameters"]["metadata_location"]

    s3 = boto3.client("s3", region_name=REGION)

    def _get_json(uri: str) -> Dict[str, Any]:
        bucket, key = uri.replace("s3://", "", 1).split("/", 1)
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        return json.loads(raw)

    meta = _get_json(metadata_uri)
    current = meta["current-snapshot-id"]
    snapshot = next(s for s in meta["snapshots"] if s["snapshot-id"] == current)

    # The manifest list and manifests are Avro. Rather than add an Avro
    # dependency, let DuckDB read them — it ships Avro support and is
    # already in the image.
    manifest_list_uri = snapshot["manifest-list"]
    manifests = [
        r[0] for r in con.sql(
            f"SELECT manifest_path FROM read_avro('{manifest_list_uri}')"
        ).fetchall()
    ]
    files: list[str] = []
    for mf in manifests:
        rows = con.sql(
            f"SELECT data_file.file_path, status FROM read_avro('{mf}')"
        ).fetchall()
        # status 2 == DELETED; those files are not part of the snapshot
        files.extend(fp for fp, st in rows if st != 2)
    return files


def _live_files(ym: str, con) -> list[str]:
    """Compatibility seam for a one-month read of the live snapshot."""
    prefix = f"trade_month={ym}/"
    return [fp for fp in _snapshot_files(con) if prefix in fp]


def _export_month(
    ym: str, tmp: Path, *, con=None, live: list[str] | None = None,
) -> Dict[str, Any]:
    """Dedupe + write one month's parquet; upload to S3. Returns stats."""
    dst_key = f"{DST_PREFIX}/trade_month={ym}/part-0.parquet"
    local = tmp / f"{ym}.parquet"

    con = con if con is not None else _duckdb(tmp)
    live = _live_files(ym, con) if live is None else live
    if not live:
        return {"month": ym, "rows": 0, "bytes": 0, "skipped": "no-live-files"}

    t0 = time.time()
    # union_by_name is required, not cosmetic: ingested_at was added to the
    # schema after ~30M rows already existed, so older data files do not carry
    # the column. Without it DuckDB rejects the mixed file set.
    source = f"read_parquet({live!r}, union_by_name=true)"
    # ...and union_by_name can only surface a column that at least one file in
    # THIS month has. Referencing it unconditionally is a hard BinderException
    # that takes the whole export down, which is what happened on the first
    # deploy: the schema had evolved but no data file carried the column yet.
    # So probe, then order accordingly — self-healing as files gain it.
    available = {
        r[0] for r in con.sql(f"DESCRIBE SELECT * FROM {source} LIMIT 0").fetchall()
    }
    # Newest write wins. Without this the order between two NON-null rows was
    # ARBITRARY, so a corrected value could lose to the wrong one it was meant
    # to replace and the fix would never reach the consumer. Rows predating the
    # column are NULL and sort last, making any new write authoritative.
    recency = ", ingested_at DESC NULLS LAST" if "ingested_at" in available else ""
    con.sql(f"""
        COPY (
            WITH ranked AS (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY fund_code, trade_date
                           ORDER BY CASE WHEN unit_nav IS NOT NULL THEN 0 ELSE 1 END,
                                    CASE WHEN accum_nav IS NOT NULL THEN 0 ELSE 1 END
                                    {recency}
                       ) AS rn
                FROM {source}
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

    # A relation no longer exposes fetchone() in DuckDB 2.0. COUNT returns
    # one row, so fetchall keeps this compatible without skipping the upload.
    rows = con.sql(f"SELECT COUNT(*) FROM read_parquet('{local}')").fetchall()[0][0]
    size = local.stat().st_size

    s3 = boto3.client("s3", region_name=REGION)
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
    explicit = event.get("month")
    if explicit is not None:
        month_start(explicit)
        months = (explicit,)
    else:
        months = export_months(date.today())
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        con = _duckdb(tmp)
        try:
            # Read metadata/manifests once, so both months use the same
            # snapshot without doubling remote manifest I/O.
            files = _snapshot_files(con)
            exports = [
                _export_month(ym, tmp, con=con, live=[
                    fp for fp in files if f"trade_month={ym}/" in fp
                ])
                for ym in months
            ]
        finally:
            con.close()
    populated = [r for r in exports if r.get("rows", 0) > 0]
    result = dict(populated[-1] if populated else exports[-1])
    if explicit is None:
        result["exports"] = exports
        result["waiting_months"] = [
            r["month"] for r in exports if r.get("skipped") == "no-live-files"
        ]
    result.update({
        "statusCode": 200,
        "downloader": "export-fund-history",
        "success": result.get("rows", 0) > 0 or result.get("skipped") == "empty",
    })
    return result
