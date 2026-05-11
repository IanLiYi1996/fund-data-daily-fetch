"""Lambda handler for CN macroeconomic data fetch (dual-write: raw parquet + Iceberg)."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict

from shared.utils.config import Config
from shared.utils.logger import get_logger
from shared.fetchers import MacroFetcher
from shared.storage import S3Client
from shared.storage.iceberg_writer import IcebergWriter

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start_time = datetime.now()
    logger.info(f"Starting CN macro data fetch. Event: {json.dumps(event, default=str)}")

    try:
        config = Config.from_env()
        config.validate()
        s3_client = S3Client(config.s3_bucket)
        warehouse = f"s3://{config.s3_bucket}/iceberg/"
        iceberg = IcebergWriter.from_glue(database="fund_data_lake", warehouse=warehouse)
        fetch_date = datetime.now()

        fetcher = MacroFetcher()
        summary = fetcher.fetch_all()

        uploads: list[dict] = []
        errors: list[dict] = []
        iceberg_summaries: list[dict] = []

        for result in summary.results:
            out = fetcher.dual_write(
                result, s3_client, iceberg,
                category=summary.category, date=fetch_date,
            )
            if out.get("skipped"):
                if not result.success:
                    errors.append({"name": result.name, "error": result.error})
                continue
            raw_out = out["raw"] or {}
            iceberg_out = out["iceberg"] or {}
            if "error" in raw_out:
                errors.append({"name": result.name, "error": f"raw: {raw_out['error']}"})
            else:
                uploads.append({
                    "name": result.name, "rows": result.row_count,
                    "s3_key": raw_out.get("key"), "size": raw_out.get("size"),
                })
            if "error" in iceberg_out:
                errors.append({"name": result.name, "error": f"iceberg: {iceberg_out['error']}"})
            iceberg_summaries.append({"name": result.name, **iceberg_out})

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"CN macro fetch completed: {len(uploads)} uploads, {len(errors)} errors "
            f"in {elapsed:.2f}s"
        )

        return {
            "statusCode": 200,
            "downloader": "cn-macro",
            "success": True,
            "success_count": len(uploads),
            "error_count": len(errors),
            "total_rows": sum(u["rows"] for u in uploads),
            "uploads": uploads,
            "iceberg": iceberg_summaries,
            "errors": errors,
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
            "catalog": MacroFetcher.get_data_catalog(),
        }

    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.exception("CN macro fetch failed")
        return {
            "statusCode": 500, "downloader": "cn-macro", "success": False,
            "error": str(e), "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }
