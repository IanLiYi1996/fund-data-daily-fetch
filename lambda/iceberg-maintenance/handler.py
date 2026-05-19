"""Weekly Iceberg maintenance: compaction + snapshot expiration.

pyiceberg 0.11 does not expose `rewrite_data_files`. We compact each table by
reading the full scan into Arrow, deduping on the table's identifier columns
(if any), and writing it back with `table.overwrite()`. The PartitionSpec is
preserved, so partitioned tables (e.g. fund_daily by trade_month) still land
in the same partition directories — just as one or two large files per
partition instead of hundreds of small ones.

For our scale (largest table is fund_daily ~470 MB / ~3M rows) the full-scan
+ overwrite approach is cheap (~minutes per table) and avoids the extra
machinery of merge-on-read compaction.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pyarrow as pa

from shared.schemas import TABLES
from shared.storage.iceberg_writer import IcebergWriter
from shared.utils.config import Config
from shared.utils.logger import get_logger

logger = get_logger(__name__)

SNAPSHOT_RETENTION_DAYS = 14
# Skip compaction if a table already has fewer files than this — it is
# already healthy and overwriting would just churn snapshots.
COMPACT_FILE_COUNT_THRESHOLD = 8


def _table_id_columns(table) -> list[str]:
    """Return the table's identifier-field column names, if defined.

    Used to dedupe rows in case the upstream upsert produced duplicate keys
    that compaction should collapse. Returns [] if no identifier fields.
    """
    try:
        schema = table.schema()
        ids = list(schema.identifier_field_ids)
        return [schema.find_column_name(fid) for fid in ids if schema.find_column_name(fid)]
    except Exception:
        return []


def _count_data_files(table) -> int:
    """Quick file count via the table scan plan."""
    try:
        return sum(1 for _ in table.scan().plan_files())
    except Exception:
        return 0


def compact_table(table) -> Dict[str, Any]:
    """Compact one table via read-all + overwrite.

    Returns a per-table summary dict.
    """
    name = ".".join(table.name())
    file_count = _count_data_files(table)

    if file_count < COMPACT_FILE_COUNT_THRESHOLD:
        logger.info(f"{name}: {file_count} files — below threshold, skipping compaction")
        return {"table": name, "skipped": True, "files_before": file_count}

    logger.info(f"{name}: {file_count} files — compacting via overwrite")
    arrow = table.scan().to_arrow()
    rows_before = len(arrow)

    id_cols = _table_id_columns(table)
    if id_cols and rows_before > 0:
        df = arrow.to_pandas()
        df = df.sort_values(id_cols, kind="mergesort").drop_duplicates(
            subset=id_cols, keep="last"
        )
        arrow = pa.Table.from_pandas(df, preserve_index=False).cast(
            table.schema().as_arrow()
        )

    table.overwrite(arrow)

    return {
        "table": name,
        "skipped": False,
        "files_before": file_count,
        "rows_before": rows_before,
        "rows_after": len(arrow),
    }


def run_maintenance(catalog, database: str) -> Dict[str, Any]:
    """Iterate all registered tables; run compaction + snapshot expiration.

    Errors on a single table are isolated and recorded; others continue.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    summary: Dict[str, Any] = {
        "tables_processed": 0,
        "compactions": [],
        "errors": [],
    }

    for table_name in TABLES:
        try:
            table = catalog.load_table((database, table_name))
            compaction = compact_table(table)
            table.refresh()
            table.expire_snapshots(timestamp_ms=cutoff_ms)
            summary["tables_processed"] += 1
            summary["compactions"].append(compaction)
            logger.info(f"Maintenance ok: {table_name} ({compaction})")
        except Exception as e:
            logger.exception(f"Maintenance failed for {table_name}")
            summary["errors"].append({"table": table_name, "error": str(e)})
    return summary


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start = datetime.now()
    logger.info(f"Iceberg maintenance starting. Event: {event}")
    config = Config.from_env()
    config.validate()
    warehouse = f"s3://{config.s3_bucket}/{config.s3_prefix}iceberg/"
    writer = IcebergWriter.from_glue(database="fund_data_lake", warehouse=warehouse)
    summary = run_maintenance(writer.catalog, "fund_data_lake")
    elapsed = (datetime.now() - start).total_seconds()
    return {
        "statusCode": 200,
        "downloader": "iceberg-maintenance",
        "success": len(summary["errors"]) == 0,
        **summary,
        "elapsed_seconds": round(elapsed, 2),
        "timestamp": datetime.now().isoformat(),
    }
