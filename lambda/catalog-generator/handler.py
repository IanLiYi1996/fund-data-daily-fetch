"""Lambda handler for generating data catalog from processing results."""

import json
from datetime import datetime
from typing import Any, Dict

from shared.utils.config import Config
from shared.utils.logger import get_logger
from shared.storage import S3Client

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Generate data catalog JSON.

    Receives a minimal event with processing summary from data-processor.
    Builds a unified catalog with the processed data inventory and uploads
    to S3 at _catalog/{date}/ and _catalog/latest/.
    """
    start_time = datetime.now()

    if isinstance(event, str):
        try:
            event = json.loads(event)
        except (json.JSONDecodeError, ValueError):
            event = {}

    logger.info(f"Starting catalog generation. Event: {event.get('action', 'unknown')}")

    try:
        config = Config.from_env()
        config.validate()
        s3_client = S3Client(config.s3_bucket)
        date_str = datetime.now().strftime("%Y-%m-%d")

        # Extract processing summary from data-processor output
        processing = event.get("processing", {})
        processors = processing.get("processors", [])

        # Build catalog
        catalog = {
            "generated_at": datetime.now().isoformat(),
            "date": date_str,
            "summary": {
                "files_written": processing.get("files_written", 0),
                "error_count": processing.get("error_count", 0),
                "processors": [
                    {
                        "name": p.get("processor", "unknown"),
                        "files_written": p.get("files_written", 0),
                        "errors": p.get("errors", []),
                    }
                    for p in processors
                ],
                "elapsed_seconds": processing.get("elapsed_seconds", 0),
            },
            "processed_data": {
                "market_overview": "data/latest/market_overview.json",
                "search_indexes": {
                    "fund": "data/latest/fund/search_index.json",
                    "a_share": "data/latest/a_share/search_index.json",
                    "hk_stock": "data/latest/hk_stock/search_index.json",
                    "us_stock": "data/latest/us_stock/search_index.json",
                },
                "per_code_lookups": {
                    "fund": "data/latest/fund/top/{code}.json",
                    "a_share": "data/latest/a_share/top/{code}.json",
                    "hk_stock": "data/latest/hk_stock/top/{code}.json",
                    "us_stock": "data/latest/us_stock/top/{code}.json",
                },
                "rankings": {
                    "fund": {
                        "by_daily_return": "data/latest/fund/rankings/by_daily_return.json",
                        "top_etf": "data/latest/fund/rankings/top_etf.json",
                        "money_fund": "data/latest/fund/rankings/money_fund.json",
                    },
                    "a_share": {
                        "top_gainers": "data/latest/a_share/rankings/top_gainers.json",
                        "top_losers": "data/latest/a_share/rankings/top_losers.json",
                        "top_volume": "data/latest/a_share/rankings/top_volume.json",
                        "hot_stocks": "data/latest/a_share/rankings/hot_stocks.json",
                    },
                    "a_share_boards": {
                        "industry": "data/latest/a_share/boards/industry.json",
                        "concept": "data/latest/a_share/boards/concept.json",
                    },
                    "a_share_fund_flow": {
                        "market": "data/latest/a_share/fund_flow/market.json",
                        "sector": "data/latest/a_share/fund_flow/sector.json",
                    },
                    "hk_stock": {
                        "hot_stocks": "data/latest/hk_stock/hot_stocks.json",
                    },
                    "fund_fees": "data/latest/fund/fee_summary.json",
                },
                "macro": {
                    "cn": "data/latest/macro/cn_latest.json",
                    "us": "data/latest/macro/us_latest.json",
                },
            },
        }

        # Upload catalog to S3
        s3_client.upload_json(catalog, f"_catalog/{date_str}/data_catalog.json")
        s3_client.upload_json(catalog, "_catalog/latest/data_catalog.json")
        logger.info("Data catalog uploaded successfully")

        elapsed = (datetime.now() - start_time).total_seconds()

        return {
            "statusCode": 200,
            "downloader": "catalog-generator",
            "success": True,
            "catalog_date": date_str,
            "summary": catalog["summary"],
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.error(f"Catalog generation failed: {e}")
        return {
            "statusCode": 500,
            "downloader": "catalog-generator",
            "success": False,
            "error": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }
