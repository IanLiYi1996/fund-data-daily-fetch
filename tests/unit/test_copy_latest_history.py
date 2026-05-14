"""Tests for catalog-generator's copy-latest-history-to-today helper."""

import datetime as dt
import importlib
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

HANDLER_DIR = Path(__file__).resolve().parent.parent.parent / "lambda" / "catalog-generator"
BUCKET = "test-bucket"


@pytest.fixture
def handler_module():
    sys.path.insert(0, str(HANDLER_DIR))
    try:
        if "handler" in sys.modules:
            del sys.modules["handler"]
        module = importlib.import_module("handler")
        yield module
    finally:
        sys.path.remove(str(HANDLER_DIR))


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _put_real_history(s3, date: dt.date, name: str, body: bytes = b"REAL"):
    """Upload a 'real' history parquet (no copied_from metadata)."""
    s3.put_object(
        Bucket=BUCKET,
        Key=f"fund/{date.isoformat()}/{name}.parquet",
        Body=body,
    )


def test_copy_when_today_missing_pulls_from_most_recent(handler_module, s3):
    today = dt.date(2026, 5, 14)
    real_date = dt.date(2026, 5, 11)  # 3 days ago
    _put_real_history(s3, real_date, "fund_manager_history", b"manager-data")
    _put_real_history(s3, real_date, "fund_scale_history", b"scale-data")

    result = handler_module.copy_latest_history_to_today(s3, BUCKET, today)

    # Today's keys should now exist with copied_from metadata
    mgr = s3.head_object(
        Bucket=BUCKET, Key="fund/2026-05-14/fund_manager_history.parquet"
    )
    assert mgr["Metadata"].get("copied_from") == "fund/2026-05-11/fund_manager_history.parquet"
    obj = s3.get_object(
        Bucket=BUCKET, Key="fund/2026-05-14/fund_manager_history.parquet"
    )
    assert obj["Body"].read() == b"manager-data"

    assert result["fund_manager_history"]["copied_from"] == \
        "fund/2026-05-11/fund_manager_history.parquet"
    assert result["fund_scale_history"]["copied_from"] == \
        "fund/2026-05-11/fund_scale_history.parquet"


def test_skip_when_today_already_has_real_file(handler_module, s3):
    today = dt.date(2026, 5, 14)
    older = dt.date(2026, 5, 10)
    _put_real_history(s3, older, "fund_manager_history")
    _put_real_history(s3, today, "fund_manager_history", b"REAL_TODAY")

    result = handler_module.copy_latest_history_to_today(s3, BUCKET, today)

    obj = s3.get_object(
        Bucket=BUCKET, Key="fund/2026-05-14/fund_manager_history.parquet"
    )
    assert obj["Body"].read() == b"REAL_TODAY"  # untouched
    # Should not have copied_from metadata
    assert "copied_from" not in (obj.get("Metadata") or {})
    assert result["fund_manager_history"]["status"] == "skipped_real_exists"


def test_no_recent_file_returns_status_not_found(handler_module, s3):
    today = dt.date(2026, 5, 14)
    # Bucket is empty for fund_manager_history
    result = handler_module.copy_latest_history_to_today(s3, BUCKET, today)

    assert result["fund_manager_history"]["status"] == "not_found"
    assert result["fund_scale_history"]["status"] == "not_found"


def test_copy_skips_other_copies_finds_real(handler_module, s3):
    """A copy from yesterday shouldn't be the source — should keep walking back to a real file."""
    today = dt.date(2026, 5, 14)
    real_date = dt.date(2026, 5, 11)
    yesterday = dt.date(2026, 5, 13)

    _put_real_history(s3, real_date, "fund_manager_history", b"REAL")
    # yesterday has a copy (with copied_from metadata)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"fund/{yesterday.isoformat()}/fund_manager_history.parquet",
        Body=b"REAL",
        Metadata={"copied_from": f"fund/{real_date.isoformat()}/fund_manager_history.parquet"},
    )

    result = handler_module.copy_latest_history_to_today(s3, BUCKET, today)

    # Should have copied from the REAL file at 2026-05-11, not from the copy at 2026-05-13
    assert result["fund_manager_history"]["copied_from"] == \
        f"fund/{real_date.isoformat()}/fund_manager_history.parquet"


def test_lookback_window_respected(handler_module, s3):
    """Files older than lookback_days should be ignored."""
    today = dt.date(2026, 5, 14)
    too_old = dt.date(2026, 1, 1)  # 133 days ago
    _put_real_history(s3, too_old, "fund_manager_history")

    result = handler_module.copy_latest_history_to_today(
        s3, BUCKET, today, lookback_days=90
    )

    assert result["fund_manager_history"]["status"] == "not_found"


def test_key_prefix_applied_to_lookup_and_copy(handler_module, s3):
    """When iceberg-branch S3_PREFIX is set, both source lookup and dest write
    must use the prefixed path (e.g. fund-data-pipeline/fund/...)."""
    today = dt.date(2026, 5, 14)
    real_date = dt.date(2026, 5, 11)
    prefix = "fund-data-pipeline/"

    s3.put_object(
        Bucket=BUCKET,
        Key=f"{prefix}fund/{real_date.isoformat()}/fund_manager_history.parquet",
        Body=b"prefixed-data",
    )

    result = handler_module.copy_latest_history_to_today(
        s3, BUCKET, today, key_prefix=prefix
    )

    # Real file lives under the prefix; today's copy should also live there.
    obj = s3.get_object(
        Bucket=BUCKET,
        Key=f"{prefix}fund/{today.isoformat()}/fund_manager_history.parquet",
    )
    assert obj["Body"].read() == b"prefixed-data"
    assert result["fund_manager_history"]["copied_from"] == \
        f"{prefix}fund/{real_date.isoformat()}/fund_manager_history.parquet"
    # Source not at bucket root should not have produced a copy at bucket root either
    listed = s3.list_objects_v2(Bucket=BUCKET, Prefix="fund/")
    # Either no Contents key, or empty
    assert not listed.get("Contents")
