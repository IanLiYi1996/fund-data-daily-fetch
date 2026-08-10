"""Lambda handler for per-fund manager + scale history fetch.

Storage layout (no per-day duplication):
- Per-partition (intermediate, NOT replicated to mengxin):
    {prefix}_history_staging/{base}__part{i}.parquet
- Canonical merged output (replicated to mengxin via fund/ prefix rule):
    {prefix}fund/_history/{base}.parquet  (S3 versioning preserves history)

Two modes:
1. *_full   — Map state fans out partitions; each partition Lambda fetches
              its slice and writes a part file under _history_staging/
2. *_merge  — runs after the Map; concats all part files into a single
              fund/_history/{base}.parquet (overwrites, versioned) and
              deletes the staging parts

Event shape (full mode):
    {
      "mode": "manager_full" | "scale_full",
      "fund_codes": ["000001", ...],     # optional; bootstrapped if missing
      "partition_index": 0,
      "partition_total": 4
    }

Event shape (merge mode):
    {
      "mode": "manager_merge" | "scale_merge",
      "partition_total": 4
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

# Per-partition intermediate files. Outside the fund/ prefix so S3
# Replication's fund/ rule does NOT mirror these to mengxin.
STAGING_CATEGORY = "_history_staging"

# Canonical merged-output prefix under fund/ — replicated to mengxin and
# kept at one stable path with S3 versioning preserving older versions.
CANONICAL_SUBDIR = "_history"


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


def _staging_key(s3_client: S3Client, base_name: str, partition_index: int) -> str:
    key_prefix = getattr(s3_client, "key_prefix", "") or ""
    return f"{key_prefix}{STAGING_CATEGORY}/{base_name}__part{partition_index}.parquet"


def _canonical_key(s3_client: S3Client, base_name: str) -> str:
    key_prefix = getattr(s3_client, "key_prefix", "") or ""
    return f"{key_prefix}fund/{CANONICAL_SUBDIR}/{base_name}.parquet"


def _put_parquet(s3, bucket: str, key: str, df: pd.DataFrame) -> dict[str, Any]:
    """Write a DataFrame to S3 as parquet at an explicit key (no date dir)."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    body = buf.getvalue()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/x-parquet",
        Metadata={
            "row_count": str(len(df)),
            "column_count": str(len(df.columns)),
            "created_at": dt.datetime.utcnow().isoformat(),
        },
    )
    return {"bucket": bucket, "key": key, "size": len(body), "rows": len(df)}


def _carry_forward(
    s3, bucket: str, canonical_key: str, fresh: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """Union this run's rows with the previous canonical file, per fund_code.

    The merge used to publish `pd.concat(this week's parts)` directly, which
    made the canonical file a snapshot of whatever the run happened to
    collect rather than the accumulated best-known state. Upstream
    (fundf10.eastmoney.com) rate-limits us hard — the 2026-08-09 run logged
    error_count=1008 of ~27.5k codes as `HTTPError: 514 Frequency Capped`
    — and those funds were silently dropped from the published file. That is
    how 110022 易方达消费行业股票 lost a manager tenure it had carried for
    weeks: nothing deleted it, the overwrite just didn't include it.

    Because each run re-fetches a fund's FULL history, a fund present in
    this run is authoritative for that fund and its old rows are replaced
    wholesale. A fund absent from this run keeps its previous rows. So the
    granularity of the union has to be the fund, not the row — a row-level
    concat+dedup would resurrect tenures that upstream has since corrected.
    """
    if fresh.empty:
        return fresh, 0

    code_col = "基金代码"
    if code_col not in fresh.columns:
        return fresh, 0

    try:
        obj = s3.get_object(Bucket=bucket, Key=canonical_key)
        prior = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception as exc:
        err_code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        if err_code not in {"NoSuchKey", "404", "NotFound"}:
            # A read failure must not silently degrade into an overwrite that
            # drops history — that is the exact defect this guards against.
            raise
        logger.info(f"no prior {canonical_key}; publishing this run as-is")
        return fresh, 0

    if prior.empty or code_col not in prior.columns:
        return fresh, 0

    refreshed = set(fresh[code_col].astype(str))
    keep = prior[~prior[code_col].astype(str).isin(refreshed)]
    if keep.empty:
        return fresh, 0

    carried = int(keep[code_col].nunique())
    logger.info(
        f"carrying forward {carried:,} funds / {len(keep):,} rows not "
        f"refreshed this run (fetched {len(refreshed):,})"
    )
    return pd.concat([fresh, keep], ignore_index=True), carried


def run(
    event: dict,
    fetcher: FundHistoryFetcher,
    s3_client: S3Client,
    list_provider: Optional[Callable[[], list[str]]] = None,
    boto3_s3: Optional[Any] = None,
    remaining: Optional[Callable[[], float]] = None,
) -> dict[str, Any]:
    """Pure-function entrypoint that takes its dependencies — used by tests."""
    mode = event.get("mode")
    if mode not in VALID_MODES:
        raise ValueError(f"invalid mode: {mode!r}; expected one of {VALID_MODES}")

    if mode in MERGE_MODES:
        return _run_merge(mode, event, s3_client, boto3_s3)

    snapshot_str = event.get("snapshot_date")
    snapshot_date = (
        dt.date.fromisoformat(snapshot_str) if snapshot_str else dt.datetime.utcnow().date()
    )

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

    fetch = (
        fetcher.fetch_manager_history if mode == "manager_full"
        else fetcher.fetch_scale_history
    )
    df, errors = fetch(chunk, snapshot_date=snapshot_date, remaining=remaining)

    upload_info: dict[str, Any] = {}
    if not df.empty:
        upload_info = s3_client.upload_dataframe(
            df=df,
            category=STAGING_CATEGORY,
            data_name=f"{_OUTPUT_NAME[mode]}__part{partition_index}",
            date=None,
            with_date=False,
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
    event: dict,
    s3_client: S3Client,
    boto3_s3: Optional[Any],
) -> dict[str, Any]:
    partition_total = int(event.get("partition_total", 1))
    base_name = _OUTPUT_NAME[mode]
    s3 = boto3_s3 if boto3_s3 is not None else boto3.client("s3")
    bucket = s3_client.bucket_name

    frames: list[pd.DataFrame] = []
    merged_keys: list[str] = []
    missing = 0
    for i in range(partition_total):
        part_key = _staging_key(s3_client, base_name, i)
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
            "parts_merged": 0,
            "parts_missing": partition_total,
            "row_count": 0,
        }

    merged_df = pd.concat(frames, ignore_index=True)
    canonical_key = _canonical_key(s3_client, base_name)
    merged_df, carried = _carry_forward(s3, bucket, canonical_key, merged_df)
    upload_info = _put_parquet(s3, bucket, canonical_key, merged_df)

    # Delete staging part files after successful merge upload
    for key in merged_keys:
        try:
            s3.delete_object(Bucket=bucket, Key=key)
        except Exception as exc:
            logger.warning(f"failed to delete staging part {key}: {exc}")

    return {
        "success": True,
        "mode": mode,
        "parts_merged": len(merged_keys),
        "parts_missing": missing,
        "row_count": len(merged_df),
        "funds_carried_forward": carried,
        "s3": upload_info,
    }


def lambda_handler(event: dict, context: Any) -> dict[str, Any]:
    logger.info(f"Event: {json.dumps(event, default=str)[:1000]}")
    config = Config.from_env()
    config.validate()
    s3_client = S3Client(config.s3_bucket, key_prefix=getattr(config, "s3_prefix", "") or "")
    fetcher = FundHistoryFetcher(max_workers=8, max_retries=5)
    # Let the retry logic spend the invocation's real remaining time instead of
    # a fixed guess: rate-limit backoff is only worth taking if the partition
    # can still finish afterwards.
    remaining = None
    if context is not None and hasattr(context, "get_remaining_time_in_millis"):
        remaining = lambda: context.get_remaining_time_in_millis() / 1000.0
    try:
        return run(event, fetcher=fetcher, s3_client=s3_client, remaining=remaining)
    except Exception as exc:
        logger.exception("fund-history-fetcher failed")
        return {
            "success": False,
            "error": f"{type(exc).__name__}: {exc}",
            "event": event,
        }
