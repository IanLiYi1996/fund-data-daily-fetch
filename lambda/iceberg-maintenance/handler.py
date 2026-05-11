"""Weekly Iceberg maintenance: compaction + snapshot expiration."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from shared.schemas import TABLES
from shared.storage.iceberg_writer import IcebergWriter
from shared.utils.config import Config
from shared.utils.logger import get_logger

logger = get_logger(__name__)

TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB
SNAPSHOT_RETENTION_DAYS = 14


def run_maintenance(catalog, database: str) -> Dict[str, Any]:
    """Iterate all registered tables; run compaction + snapshot expiration.

    Errors on a single table are isolated and recorded; others continue.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    summary: Dict[str, Any] = {
        "tables_processed": 0,
        "errors": [],
    }

    for table_name in TABLES:
        try:
            table = catalog.load_table((database, table_name))
            table.rewrite_data_files(target_size_bytes=TARGET_FILE_SIZE_BYTES)
            table.expire_snapshots(timestamp_ms=cutoff_ms)
            summary["tables_processed"] += 1
            logger.info(f"Maintenance ok: {table_name}")
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
