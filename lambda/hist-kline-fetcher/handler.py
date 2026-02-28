"""Lambda handler for historical K-line data fetch."""

import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict

from shared.utils.config import Config
from shared.utils.logger import get_logger
from shared.fetchers.hist_kline_fetcher import HistKlineFetcher
from shared.storage import S3Client

logger = get_logger(__name__)

_UPLOAD_WORKERS = 16


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start_time = datetime.now()
    logger.info(f"Starting historical K-line data fetch. Event: {json.dumps(event, default=str)}")

    try:
        config = Config.from_env()
        config.validate()
        s3_client = S3Client(config.s3_bucket)
        fetch_date = datetime.now()

        fetcher = HistKlineFetcher()
        summary = fetcher.fetch_all()

        # ----------------------------------------------------------
        # Parallel S3 uploads (per-stock files, ~2700 files total)
        # ----------------------------------------------------------
        upload_count = 0
        upload_rows = 0
        upload_bytes = 0
        error_list = []

        # Collect per-group stats: "a_share/daily" -> {files, rows, bytes}
        group_stats = defaultdict(lambda: {"files": 0, "rows": 0, "bytes": 0})

        def _upload_one(result):
            upload_info = s3_client.upload_dataframe(
                df=result.data, category=summary.category,
                data_name=result.name, date=fetch_date,
            )
            return result.name, result.row_count, upload_info.get("size", 0)

        # Separate uploadable results from errors
        to_upload = []
        for result in summary.results:
            if result.success and result.data is not None and not result.data.empty:
                to_upload.append(result)
            elif not result.success:
                error_list.append({"name": result.name, "error": result.error})

        logger.info(f"Uploading {len(to_upload)} per-stock files with {_UPLOAD_WORKERS} workers...")

        with ThreadPoolExecutor(max_workers=_UPLOAD_WORKERS) as executor:
            futures = {executor.submit(_upload_one, r): r.name for r in to_upload}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    _, rows, size = future.result()
                    upload_count += 1
                    upload_rows += rows
                    upload_bytes += size
                    # Group key: "a_share/daily" from "a_share/daily/600519"
                    parts = name.rsplit("/", 1)
                    group_key = parts[0] if len(parts) > 1 else name
                    group_stats[group_key]["files"] += 1
                    group_stats[group_key]["rows"] += rows
                    group_stats[group_key]["bytes"] += size
                except Exception as e:
                    error_list.append({"name": name, "error": f"Upload failed: {str(e)}"})

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Hist K-line fetch completed: {upload_count} files uploaded, "
            f"{len(error_list)} errors in {elapsed:.2f}s"
        )

        # Build concise group summary instead of listing every file
        group_summary = [
            {"group": k, "files": v["files"], "rows": v["rows"], "bytes": v["bytes"]}
            for k, v in sorted(group_stats.items())
        ]

        return {
            "statusCode": 200, "downloader": "hist-kline", "success": True,
            "file_count": upload_count, "error_count": len(error_list),
            "total_rows": upload_rows, "total_bytes": upload_bytes,
            "groups": group_summary,
            "errors": error_list[:20],  # Cap errors in response
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
            "catalog": HistKlineFetcher.get_data_catalog(),
        }
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.error(f"Hist K-line fetch failed: {e}")
        return {
            "statusCode": 500, "downloader": "hist-kline", "success": False,
            "error": str(e), "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }
