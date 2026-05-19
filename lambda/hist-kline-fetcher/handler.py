"""Lambda handler for historical K-line data fetch.

Two invocation modes:

1. **partition** mode (default when ``event.market`` and ``event.interval`` set):
   Fetches one (market, interval) slice. There are 9 partitions total
   ((a_share|hk|us) × (daily|weekly|monthly)). The Step Function fans out
   into 9 concurrent invocations so the whole job finishes in ~3 min instead
   of ~24 min (yfinance batch download is the slow leg).

2. **legacy fetch_all** mode (no event keys): Runs all 9 partitions
   serially in one Lambda. Kept for ad-hoc invocations / backfills.

Iceberg dual-write is intentionally deferred for hist-kline. Per-stock
filename routing to kline_a/hk/us tables needs a batching layer — see
spec M7/follow-up. Raw parquet continues as source of truth.
"""

import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List

from shared.fetchers.hist_kline_fetcher import HistKlineFetcher, _INTERVALS
from shared.storage import S3Client
from shared.utils.config import Config
from shared.utils.logger import get_logger

logger = get_logger(__name__)

_UPLOAD_WORKERS = 16

_VALID_MARKETS = ("a_share", "hk", "us")
_VALID_FREQ = tuple(name for name, _ in _INTERVALS)
_FREQ_TO_INTERVAL = dict(_INTERVALS)


def _resolve_tickers(fetcher: HistKlineFetcher, market: str) -> List[str]:
    if market == "a_share":
        return fetcher._get_a_share_tickers()
    if market == "hk":
        return fetcher._get_hk_tickers()
    if market == "us":
        return fetcher._get_us_tickers()
    raise ValueError(f"unknown market: {market!r}")


def _run_partition(event: Dict[str, Any], start_time: datetime) -> Dict[str, Any]:
    """Fetch one (market, frequency) slice and upload per-stock parquets."""
    market = event["market"]
    frequency = event["interval"]  # "daily" / "weekly" / "monthly"

    if market not in _VALID_MARKETS:
        raise ValueError(f"market must be one of {_VALID_MARKETS}, got {market!r}")
    if frequency not in _VALID_FREQ:
        raise ValueError(f"interval must be one of {_VALID_FREQ}, got {frequency!r}")

    yf_interval = _FREQ_TO_INTERVAL[frequency]

    config = Config.from_env()
    config.validate()
    s3_client = S3Client(config.s3_bucket)
    fetch_date = datetime.now()
    fetcher = HistKlineFetcher()

    logger.info(f"[{market}/{frequency}] resolving tickers...")
    tickers = _resolve_tickers(fetcher, market)
    if not tickers:
        return {
            "statusCode": 200, "downloader": "hist-kline", "success": True,
            "market": market, "frequency": frequency,
            "file_count": 0, "error_count": 0, "total_rows": 0,
            "errors": [], "elapsed_seconds": 0.0,
        }

    logger.info(f"[{market}/{frequency}] fetching {len(tickers)} tickers...")
    combined = fetcher._fetch_market_kline(tickers, yf_interval, market)

    if combined is None or combined.empty:
        elapsed = (datetime.now() - start_time).total_seconds()
        return {
            "statusCode": 200, "downloader": "hist-kline", "success": True,
            "market": market, "frequency": frequency,
            "file_count": 0, "error_count": 0, "total_rows": 0,
            "errors": [{"name": f"{market}/{frequency}", "error": "empty result"}],
            "elapsed_seconds": round(elapsed, 2),
        }

    # Upload one parquet per stock under hist_kline/{date}/{market}/{frequency}/{code}.parquet.
    upload_count = 0
    upload_rows = 0
    upload_bytes = 0
    error_list: list[dict] = []

    def _upload_one(code: str, df) -> tuple[str, int, int]:
        upload_info = s3_client.upload_dataframe(
            df=df, category=fetcher.category,
            data_name=f"{market}/{frequency}/{code}", date=fetch_date,
        )
        return code, len(df), upload_info.get("size", 0)

    groups = list(combined.groupby("代码", sort=True))
    logger.info(
        f"[{market}/{frequency}] uploading {len(groups)} per-stock files "
        f"with {_UPLOAD_WORKERS} workers..."
    )
    with ThreadPoolExecutor(max_workers=_UPLOAD_WORKERS) as executor:
        futures = {
            executor.submit(_upload_one, code, df.reset_index(drop=True)): code
            for code, df in groups
        }
        for future in as_completed(futures):
            code = futures[future]
            try:
                _, rows, size = future.result()
                upload_count += 1
                upload_rows += rows
                upload_bytes += size
            except Exception as e:
                error_list.append({"name": f"{market}/{frequency}/{code}", "error": f"Upload failed: {e}"})

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"[{market}/{frequency}] done: {upload_count} files, "
        f"{len(error_list)} errors in {elapsed:.2f}s"
    )

    return {
        "statusCode": 200, "downloader": "hist-kline", "success": True,
        "market": market, "frequency": frequency,
        "file_count": upload_count, "error_count": len(error_list),
        "total_rows": upload_rows, "total_bytes": upload_bytes,
        "errors": error_list[:20],
        "elapsed_seconds": round(elapsed, 2),
        "timestamp": datetime.now().isoformat(),
    }


def _run_legacy_all(start_time: datetime) -> Dict[str, Any]:
    """Original behavior: serial fetch of all 9 (market, frequency) slices."""
    config = Config.from_env()
    config.validate()
    s3_client = S3Client(config.s3_bucket)
    fetch_date = datetime.now()

    fetcher = HistKlineFetcher()
    summary = fetcher.fetch_all()

    upload_count = 0
    upload_rows = 0
    upload_bytes = 0
    error_list: list[dict] = []
    group_stats = defaultdict(lambda: {"files": 0, "rows": 0, "bytes": 0})

    def _upload_one(result):
        upload_info = s3_client.upload_dataframe(
            df=result.data, category=summary.category,
            data_name=result.name, date=fetch_date,
        )
        return result.name, result.row_count, upload_info.get("size", 0)

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
                parts = name.rsplit("/", 1)
                group_key = parts[0] if len(parts) > 1 else name
                group_stats[group_key]["files"] += 1
                group_stats[group_key]["rows"] += rows
                group_stats[group_key]["bytes"] += size
            except Exception as e:
                error_list.append({"name": name, "error": f"Upload failed: {e}"})

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"Hist K-line fetch completed: {upload_count} files uploaded, "
        f"{len(error_list)} errors in {elapsed:.2f}s"
    )

    group_summary = [
        {"group": k, "files": v["files"], "rows": v["rows"], "bytes": v["bytes"]}
        for k, v in sorted(group_stats.items())
    ]

    return {
        "statusCode": 200, "downloader": "hist-kline", "success": True,
        "file_count": upload_count, "error_count": len(error_list),
        "total_rows": upload_rows, "total_bytes": upload_bytes,
        "groups": group_summary,
        "errors": error_list[:20],
        "elapsed_seconds": round(elapsed, 2),
        "timestamp": datetime.now().isoformat(),
        "catalog": HistKlineFetcher.get_data_catalog(),
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start_time = datetime.now()
    logger.info(f"Starting historical K-line data fetch. Event: {json.dumps(event, default=str)}")

    try:
        if "market" in event and "interval" in event:
            return _run_partition(event, start_time)
        return _run_legacy_all(start_time)
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.exception(f"Hist K-line fetch failed: {e}")
        return {
            "statusCode": 500, "downloader": "hist-kline", "success": False,
            "error": str(e), "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }
