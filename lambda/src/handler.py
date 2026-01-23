"""
Lambda handler for daily fund data fetch.

This Lambda function fetches fund, stock index, and macroeconomic data
from akshare and stores it in S3 as Parquet files.
"""

import json
from datetime import datetime
from typing import Any, Dict, List

from utils.config import Config
from utils.logger import get_logger
from fetchers import FundFetcher, StockFetcher, MacroFetcher, FetchSummary
from storage import S3Client

logger = get_logger(__name__)


def process_fetch_summary(
    s3_client: S3Client, summary: FetchSummary, date: datetime
) -> Dict[str, Any]:
    """Process fetch results and upload to S3.

    Args:
        s3_client: S3 client instance
        summary: FetchSummary containing fetch results
        date: Date for partitioning

    Returns:
        Dict with processing results
    """
    uploads = []
    errors = []

    for result in summary.results:
        if result.success and result.data is not None and not result.data.empty:
            try:
                upload_info = s3_client.upload_dataframe(
                    df=result.data,
                    category=summary.category,
                    data_name=result.name,
                    date=date,
                )
                uploads.append({
                    "name": result.name,
                    "rows": result.row_count,
                    "s3_key": upload_info.get("key"),
                    "size": upload_info.get("size"),
                })
            except Exception as e:
                errors.append({
                    "name": result.name,
                    "error": f"Upload failed: {str(e)}",
                })
        elif not result.success:
            errors.append({
                "name": result.name,
                "error": result.error,
            })

    return {
        "category": summary.category,
        "success_count": len(uploads),
        "error_count": len(errors),
        "total_rows": sum(u["rows"] for u in uploads),
        "uploads": uploads,
        "errors": errors,
    }


def upload_data_catalog(s3_client: S3Client, fetchers: List, date: datetime) -> None:
    """Upload data catalog with descriptions for all data sources.

    Args:
        s3_client: S3 client instance
        fetchers: List of fetcher instances
        date: Date for partitioning
    """
    date_str = date.strftime("%Y-%m-%d")

    # Build complete catalog
    catalog = {
        "generated_at": datetime.now().isoformat(),
        "date": date_str,
        "categories": {},
    }

    for fetcher in fetchers:
        if hasattr(fetcher, "get_data_catalog"):
            catalog["categories"][fetcher.category] = fetcher.get_data_catalog()

    # Upload catalog to S3
    try:
        # Upload to date-partitioned path
        s3_client.upload_json(catalog, f"_catalog/{date_str}/data_catalog.json")
        # Also upload to root as latest catalog
        s3_client.upload_json(catalog, "_catalog/latest/data_catalog.json")
        logger.info("Data catalog uploaded successfully")
    except Exception as e:
        logger.warning(f"Failed to upload data catalog: {e}")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler function.

    Args:
        event: Lambda event (from EventBridge or manual trigger)
        context: Lambda context

    Returns:
        Dict with execution results
    """
    start_time = datetime.now()
    logger.info("Starting fund data fetch job")
    logger.info(f"Event: {json.dumps(event, default=str)}")

    # Load and validate configuration
    config = Config.from_env()
    try:
        config.validate()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return {
            "statusCode": 500,
            "body": {"error": f"Configuration error: {str(e)}"},
        }

    # Initialize S3 client
    s3_client = S3Client(config.s3_bucket)
    if not s3_client.check_bucket_exists():
        return {
            "statusCode": 500,
            "body": {"error": f"S3 bucket {config.s3_bucket} not accessible"},
        }

    # Current date for partitioning
    fetch_date = datetime.now()
    logger.info(f"Fetch date: {fetch_date.strftime('%Y-%m-%d')}")

    # Initialize fetchers
    fetchers = [
        FundFetcher(),
        StockFetcher(),
        MacroFetcher(),
    ]

    # Upload data catalog
    upload_data_catalog(s3_client, fetchers, fetch_date)

    # Execute fetchers and upload results
    results: List[Dict[str, Any]] = []
    total_errors = 0

    for fetcher in fetchers:
        logger.info(f"Starting {fetcher.category} data fetch...")
        try:
            summary = fetcher.fetch_all()
            result = process_fetch_summary(s3_client, summary, fetch_date)
            results.append(result)
            total_errors += result["error_count"]
            logger.info(
                f"{fetcher.category}: {result['success_count']} successful, "
                f"{result['error_count']} errors, {result['total_rows']} total rows"
            )
        except Exception as e:
            logger.error(f"Fatal error in {fetcher.category} fetcher: {e}")
            results.append({
                "category": fetcher.category,
                "success_count": 0,
                "error_count": 1,
                "total_rows": 0,
                "uploads": [],
                "errors": [{"name": "fatal", "error": str(e)}],
            })
            total_errors += 1

    # Calculate execution time
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Build response
    response = {
        "statusCode": 200 if total_errors == 0 else 207,  # 207 = Multi-Status
        "body": {
            "message": "Fund data fetch completed",
            "fetch_date": fetch_date.strftime("%Y-%m-%d"),
            "duration_seconds": round(duration, 2),
            "s3_bucket": config.s3_bucket,
            "results": results,
            "summary": {
                "total_categories": len(results),
                "total_uploads": sum(r["success_count"] for r in results),
                "total_errors": total_errors,
                "total_rows": sum(r["total_rows"] for r in results),
            },
        },
    }

    logger.info(f"Job completed in {duration:.2f}s with {total_errors} errors")
    logger.info(f"Response: {json.dumps(response, default=str)}")

    return response


# For local testing
if __name__ == "__main__":
    import os
    os.environ["S3_BUCKET"] = "test-bucket"
    os.environ["LOG_LEVEL"] = "DEBUG"

    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2, default=str))
