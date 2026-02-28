"""Lambda handler for post-processing raw parquet into MCP-ready JSON."""

import json
from datetime import datetime
from typing import Any, Dict, List

import boto3

from shared.utils.config import Config
from shared.utils.logger import get_logger
from shared.processors import (
    FundProcessor,
    AShareProcessor,
    HKProcessor,
    USProcessor,
    MacroProcessor,
    MarketOverviewProcessor,
    HistKlineProcessor,
)

logger = get_logger(__name__)

# Map downloader names (from fetcher responses) to processor classes.
# Processors that don't depend on a specific fetcher succeed/fail are always run.
PROCESSORS = [
    FundProcessor,
    AShareProcessor,
    HKProcessor,
    USProcessor,
    MacroProcessor,
    HistKlineProcessor,
]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Process raw parquet data into MCP-ready JSON.

    Receives the array of parallel fetch results from Step Functions.
    Runs all processors with error isolation, then generates market overview.
    """
    start_time = datetime.now()
    logger.info(f"Starting data processing. Event: {event}")

    try:
        config = Config.from_env()
        config.validate()
        s3_client = boto3.client("s3")
        date_str = datetime.now().strftime("%Y-%m-%d")

        # Run each processor with error isolation
        processor_summaries = []
        for proc_cls in PROCESSORS:
            summary = _safe_process(proc_cls, s3_client, config.s3_bucket, date_str)
            processor_summaries.append(summary)

        # Market overview runs last (reads from raw data, not other outputs)
        overview_summary = _safe_process(
            MarketOverviewProcessor, s3_client, config.s3_bucket, date_str
        )
        processor_summaries.append(overview_summary)

        elapsed = (datetime.now() - start_time).total_seconds()

        total_files = sum(s.get("files_written", 0) for s in processor_summaries)
        total_errors = sum(len(s.get("errors", [])) for s in processor_summaries)

        logger.info(
            f"Data processing complete: {total_files} files written, "
            f"{total_errors} errors, {elapsed:.1f}s"
        )

        return {
            "statusCode": 200,
            "downloader": "data-processor",
            "success": True,
            "files_written": total_files,
            "error_count": total_errors,
            "processors": processor_summaries,
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.error(f"Data processing failed: {e}")
        return {
            "statusCode": 500,
            "downloader": "data-processor",
            "success": False,
            "error": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }


def _safe_process(
    proc_cls, s3_client, bucket: str, date_str: str
) -> Dict[str, Any]:
    """Run a single processor with error isolation."""
    name = proc_cls.__name__
    try:
        logger.info(f"Running {name}...")
        processor = proc_cls(s3_client, bucket, date_str)
        summary = processor.process()
        logger.info(f"{name} completed: {summary.get('files_written', 0)} files")
        return summary
    except Exception as e:
        logger.error(f"{name} failed: {e}")
        return {
            "processor": name,
            "files_written": 0,
            "errors": [f"Fatal: {e}"],
        }
