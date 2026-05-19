"""Lambda handler for fund data fetch (dual-write: raw parquet + Iceberg)."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict

from shared.utils.config import Config
from shared.utils.logger import get_logger
from shared.fetchers import FundFetcher
from shared.storage import S3Client
from shared.storage.iceberg_writer import IcebergWriter

logger = get_logger(__name__)


def _build_iceberg(config: Config) -> IcebergWriter:
    warehouse = f"s3://{config.s3_bucket}/{config.s3_prefix}iceberg/"
    return IcebergWriter.from_glue(database="fund_data_lake", warehouse=warehouse)


def _process_result(fetcher, result, s3_client, iceberg, category, fetch_date):
    out = fetcher.dual_write(
        result, s3_client, iceberg,
        category=category, date=fetch_date,
    )
    upload = None
    error = None
    iceberg_summary = None
    if out.get("skipped"):
        if not result.success:
            error = {"name": result.name, "error": result.error}
        return upload, error, iceberg_summary
    raw_out = out["raw"] or {}
    iceberg_out = out["iceberg"] or {}
    if "error" in raw_out:
        error = {"name": result.name, "error": f"raw: {raw_out['error']}"}
    else:
        upload = {
            "name": result.name, "rows": result.row_count,
            "s3_key": raw_out.get("key"), "size": raw_out.get("size"),
        }
    if "error" in iceberg_out:
        error = {"name": result.name, "error": f"iceberg: {iceberg_out['error']}"}
    iceberg_summary = {"name": result.name, **iceberg_out}
    return upload, error, iceberg_summary


def _run_partition(event: Dict[str, Any], start_time: datetime) -> Dict[str, Any]:
    table = event["table"]
    logger.info(f"Starting fund partition fetch: table={table}")
    config = Config.from_env()
    config.validate()
    s3_client = S3Client(config.s3_bucket)
    iceberg = _build_iceberg(config)
    fetch_date = datetime.now()
    fetcher = FundFetcher()

    result = fetcher.fetch_one(table)
    upload, error, iceberg_summary = _process_result(
        fetcher, result, s3_client, iceberg, fetcher.category, fetch_date,
    )
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"Partition {table} done in {elapsed:.2f}s")
    return {
        "statusCode": 200,
        "downloader": "fund",
        "table": table,
        "success": result.success and (error is None),
        "uploads": [upload] if upload else [],
        "errors": [error] if error else [],
        "iceberg": [iceberg_summary] if iceberg_summary else [],
        "rows": result.row_count,
        "elapsed_seconds": round(elapsed, 2),
        "timestamp": datetime.now().isoformat(),
    }


def _run_legacy_all(start_time: datetime) -> Dict[str, Any]:
    config = Config.from_env()
    config.validate()
    s3_client = S3Client(config.s3_bucket)
    iceberg = _build_iceberg(config)
    fetch_date = datetime.now()

    fetcher = FundFetcher()
    summary = fetcher.fetch_all()

    uploads: list[dict] = []
    errors: list[dict] = []
    iceberg_summaries: list[dict] = []

    for result in summary.results:
        upload, error, iceberg_summary = _process_result(
            fetcher, result, s3_client, iceberg, summary.category, fetch_date,
        )
        if upload:
            uploads.append(upload)
        if error:
            errors.append(error)
        if iceberg_summary:
            iceberg_summaries.append(iceberg_summary)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"Fund fetch completed: {len(uploads)} uploads, {len(errors)} errors "
        f"in {elapsed:.2f}s"
    )

    return {
        "statusCode": 200,
        "downloader": "fund",
        "success": True,
        "success_count": len(uploads),
        "error_count": len(errors),
        "total_rows": sum(u["rows"] for u in uploads),
        "uploads": uploads,
        "iceberg": iceberg_summaries,
        "errors": errors,
        "elapsed_seconds": round(elapsed, 2),
        "timestamp": datetime.now().isoformat(),
        "catalog": FundFetcher.get_data_catalog(),
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start_time = datetime.now()
    logger.info(f"Starting fund data fetch. Event: {json.dumps(event, default=str)}")

    try:
        if "table" in event:
            return _run_partition(event, start_time)
        return _run_legacy_all(start_time)

    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.exception("Fund fetch failed")
        return {
            "statusCode": 500, "downloader": "fund", "success": False,
            "error": str(e), "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }
