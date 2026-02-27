"""Lambda handler for A-share market data fetch."""

import json
from datetime import datetime
from typing import Any, Dict

from shared.utils.config import Config
from shared.utils.logger import get_logger
from shared.fetchers import AShareFetcher
from shared.storage import S3Client

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Fetch A-share market data and upload to S3.

    Returns standardized response for Step Functions orchestration.
    """
    start_time = datetime.now()
    logger.info(f"Starting A-share data fetch. Event: {json.dumps(event, default=str)}")

    try:
        config = Config.from_env()
        config.validate()
        s3_client = S3Client(config.s3_bucket)
        fetch_date = datetime.now()

        fetcher = AShareFetcher()
        summary = fetcher.fetch_all()

        uploads = []
        errors = []
        for result in summary.results:
            if result.success and result.data is not None and not result.data.empty:
                try:
                    upload_info = s3_client.upload_dataframe(
                        df=result.data,
                        category=summary.category,
                        data_name=result.name,
                        date=fetch_date,
                    )
                    uploads.append({
                        "name": result.name,
                        "rows": result.row_count,
                        "s3_key": upload_info.get("key"),
                        "size": upload_info.get("size"),
                    })
                except Exception as e:
                    errors.append({"name": result.name, "error": f"Upload failed: {str(e)}"})
            elif not result.success:
                errors.append({"name": result.name, "error": result.error})

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"A-share fetch completed: {len(uploads)} uploads, {len(errors)} errors in {elapsed:.2f}s")

        return {
            "statusCode": 200,
            "downloader": "a-share",
            "success": True,
            "success_count": len(uploads),
            "error_count": len(errors),
            "total_rows": sum(u["rows"] for u in uploads),
            "uploads": uploads,
            "errors": errors,
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
            "catalog": AShareFetcher.get_data_catalog(),
        }

    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.error(f"A-share fetch failed: {e}")
        return {
            "statusCode": 500,
            "downloader": "a-share",
            "success": False,
            "error": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }
