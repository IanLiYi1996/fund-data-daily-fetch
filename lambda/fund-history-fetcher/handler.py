"""Lambda handler for per-fund manager + scale history fetch.

Two modes:
1. *_full   — Map state fans out partitions; each partition Lambda fetches
              its slice and writes fund_*_history__part{i}.parquet
2. *_merge  — runs after the Map; concats all part files into a single
              fund_*_history.parquet and deletes the parts

Event shape (full mode):
    {
      "mode": "manager_full" | "scale_full",
      "fund_codes": ["000001", ...],     # optional; bootstrapped if missing
      "partition_index": 0,
      "partition_total": 4,
      "snapshot_date": "2026-05-14"
    }

Event shape (merge mode):
    {
      "mode": "manager_merge" | "scale_merge",
      "partition_total": 4,
      "snapshot_date": "2026-05-14"
    }
"""
from __future__ import annotations

import datetime as dt
import io
import json
import math
from typing import Any, Callable, Optional

import boto3
import pandas as pd

from shared.fetchers.fund_history_fetcher import FundHistoryFetcher
from shared.storage import S3Client
from shared.utils.config import Config
from shared.utils.logger import get_logger

logger = get_logger(__name__)


FULL_MODES = ("manager_full", "scale_full")
MERGE_MODES = ("manager_merge", "scale_merge")
VALID_MODES = FULL_MODES + MERGE_MODES

_OUTPUT_NAME = {
    "manager_full": "fund_manager_history",
    "scale_full": "fund_scale_history",
    "manager_merge": "fund_manager_history",
    "scale_merge": "fund_scale_history",
}


def slice_partition(items: list[str], partition_index: int, partition_total: int) -> list[str]:
    """Split items into partition_total contiguous chunks; return the partition_index-th."""
    if partition_total <= 1:
        return list(items)
    chunk_size = math.ceil(len(items) / partition_total)
    start = partition_index * chunk_size
    end = min(start + chunk_size, len(items))
    return list(items[start:end])


def fetch_fund_universe() -> list[str]:
    """Default list_provider: pulls the full active fund-code universe from akshare."""
    import akshare as ak

    df = ak.fund_name_em()
    return df["基金代码"].astype(str).str.zfill(6).tolist()


def run(
    event: dict,
    fetcher: FundHistoryFetcher,
    s3_client: S3Client,
    list_provider: Optional[Callable[[], list[str]]] = None,
    boto3_s3: Optional[Any] = None,
) -> dict[str, Any]:
    """Pure-function entrypoint that takes its dependencies — used by tests."""
    mode = event.get("mode")
    if mode not in VALID_MODES:
        raise ValueError(f"invalid mode: {mode!r}; expected one of {VALID_MODES}")

    snapshot_str = event.get("snapshot_date")
    snapshot_date = (
        dt.date.fromisoformat(snapshot_str) if snapshot_str else dt.datetime.utcnow().date()
    )

    if mode in MERGE_MODES:
        return _run_merge(mode, snapshot_date, event, s3_client, boto3_s3)

    fund_codes = event.get("fund_codes")
    if not fund_codes:
        provider = list_provider or fetch_fund_universe
        fund_codes = provider()
    partition_index = int(event.get("partition_index", 0))
    partition_total = int(event.get("partition_total", 1))

    chunk = slice_partition(fund_codes, partition_index, partition_total)
    logger.info(
        f"mode={mode} partition={partition_index}/{partition_total} "
        f"chunk_size={len(chunk)} snapshot_date={snapshot_date}"
    )

    if mode == "manager_full":
        df, errors = fetcher.fetch_manager_history(chunk, snapshot_date=snapshot_date)
    else:
        df, errors = fetcher.fetch_scale_history(chunk, snapshot_date=snapshot_date)

    upload_info: dict[str, Any] = {}
    if not df.empty:
        data_name = f"{_OUTPUT_NAME[mode]}__part{partition_index}"
        upload_info = s3_client.upload_dataframe(
            df=df,
            category="fund",
            data_name=data_name,
            date=dt.datetime.combine(snapshot_date, dt.time.min),
        )

    return {
        "success": True,
        "mode": mode,
        "partition_index": partition_index,
        "partition_total": partition_total,
        "snapshot_date": snapshot_date.isoformat(),
        "chunk_size": len(chunk),
        "row_count": len(df),
        "error_count": len(errors),
        "errors": errors[:50],  # cap payload size
        "s3": upload_info,
    }


def _run_merge(
    mode: str,
    snapshot_date: dt.date,
    event: dict,
    s3_client: S3Client,
    boto3_s3: Optional[Any],
) -> dict[str, Any]:
    partition_total = int(event.get("partition_total", 1))
    base_name = _OUTPUT_NAME[mode]
    s3 = boto3_s3 if boto3_s3 is not None else boto3.client("s3")
    bucket = s3_client.bucket_name
    # Honor S3 key_prefix from S3Client so we read part files from the same
    # location upload_dataframe wrote them to.
    key_prefix = getattr(s3_client, "key_prefix", "") or ""
    date_prefix = f"{key_prefix}fund/{snapshot_date.isoformat()}"

    frames: list[pd.DataFrame] = []
    merged_keys: list[str] = []
    missing = 0
    for i in range(partition_total):
        part_key = f"{date_prefix}/{base_name}__part{i}.parquet"
        try:
            obj = s3.get_object(Bucket=bucket, Key=part_key)
        except Exception as exc:
            err_code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            if err_code in {"NoSuchKey", "404", "NotFound"}:
                missing += 1
                continue
            raise
        frames.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
        merged_keys.append(part_key)

    if not frames:
        return {
            "success": True,
            "mode": mode,
            "snapshot_date": snapshot_date.isoformat(),
            "parts_merged": 0,
            "parts_missing": partition_total,
            "row_count": 0,
        }

    merged_df = pd.concat(frames, ignore_index=True)
    upload_info = s3_client.upload_dataframe(
        df=merged_df,
        category="fund",
        data_name=base_name,
        date=dt.datetime.combine(snapshot_date, dt.time.min),
    )

    # Delete part files after successful merge upload
    for key in merged_keys:
        try:
            s3.delete_object(Bucket=bucket, Key=key)
        except Exception as exc:
            logger.warning(f"failed to delete part file {key}: {exc}")

    return {
        "success": True,
        "mode": mode,
        "snapshot_date": snapshot_date.isoformat(),
        "parts_merged": len(merged_keys),
        "parts_missing": missing,
        "row_count": len(merged_df),
        "s3": upload_info,
    }


def lambda_handler(event: dict, context: Any) -> dict[str, Any]:
    logger.info(f"Event: {json.dumps(event, default=str)[:1000]}")
    config = Config.from_env()
    config.validate()
    s3_client = S3Client(config.s3_bucket, key_prefix=getattr(config, "s3_prefix", "") or "")
    fetcher = FundHistoryFetcher(max_workers=8, max_retries=3)
    try:
        return run(event, fetcher=fetcher, s3_client=s3_client)
    except Exception as exc:
        logger.exception("fund-history-fetcher failed")
        return {
            "success": False,
            "error": f"{type(exc).__name__}: {exc}",
            "event": event,
        }
