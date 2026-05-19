"""Tests for fund-history-fetcher Lambda handler.

Tests partition slicing, mode dispatch, and result shape. The fetcher and
S3 client are injected to keep tests offline.
"""

import datetime as dt
import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest


HANDLER_DIR = Path(__file__).resolve().parent.parent.parent / "lambda" / "fund-history-fetcher"


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


def _make_fetcher_stub(manager_df=None, scale_df=None, errors=None):
    fetcher = MagicMock()
    fetcher.fetch_manager_history.return_value = (
        manager_df if manager_df is not None else pd.DataFrame(),
        errors if errors is not None else [],
    )
    fetcher.fetch_scale_history.return_value = (
        scale_df if scale_df is not None else pd.DataFrame(),
        errors if errors is not None else [],
    )
    return fetcher


def test_slice_partition_returns_correct_chunk(handler_module):
    codes = [f"{i:06d}" for i in range(100)]

    chunk0 = handler_module.slice_partition(codes, partition_index=0, partition_total=4)
    chunk1 = handler_module.slice_partition(codes, partition_index=1, partition_total=4)
    chunk3 = handler_module.slice_partition(codes, partition_index=3, partition_total=4)

    assert len(chunk0) == 25
    assert chunk0[0] == "000000"
    assert chunk1[0] == "000025"
    assert chunk3[-1] == "000099"
    # No overlap, full coverage
    all_chunks = sum(
        (handler_module.slice_partition(codes, i, 4) for i in range(4)), []
    )
    assert all_chunks == codes


def test_slice_partition_handles_uneven_division(handler_module):
    codes = [f"{i:06d}" for i in range(10)]
    chunks = [handler_module.slice_partition(codes, i, 3) for i in range(3)]
    flat = sum(chunks, [])
    assert flat == codes
    assert len(flat) == 10


def test_handler_manager_full_writes_to_staging_path(handler_module):
    """Partition output goes to _history_staging/ (NOT replicated to mengxin),
    so transient half-written part files never reach the consumer side."""
    df = pd.DataFrame([{
        "基金代码": "000001", "经理姓名": "张三",
        "起始日": dt.date(2024, 1, 1), "结束日": pd.NaT,
        "任期天数": 500, "任期回报": 10.0, "是否现任": True,
        "snapshot_date": dt.date(2026, 5, 14),
    }])
    fetcher = _make_fetcher_stub(manager_df=df)
    s3 = MagicMock()
    s3.upload_dataframe.return_value = {"key": "_history_staging/fund_manager_history__part0.parquet", "size": 1234}

    result = handler_module.run(
        event={"mode": "manager_full", "fund_codes": ["000001", "000002"],
               "partition_index": 0, "partition_total": 1},
        fetcher=fetcher, s3_client=s3,
    )

    fetcher.fetch_manager_history.assert_called_once()
    args, kwargs = fetcher.fetch_manager_history.call_args
    assert args[0] == ["000001", "000002"]
    # Must upload to staging category (not "fund"), so replication rule
    # filters skip these per-partition intermediate files.
    s3.upload_dataframe.assert_called_once()
    upload_kwargs = s3.upload_dataframe.call_args.kwargs
    assert upload_kwargs["category"] == "_history_staging"
    assert upload_kwargs["data_name"] == "fund_manager_history__part0"
    # date= must be None so the path has no date partition; we want a single
    # canonical staging slot per part, not one per day.
    assert upload_kwargs.get("date") is None
    assert result["success"] is True
    assert result["mode"] == "manager_full"
    assert result["partition_index"] == 0
    assert result["row_count"] == 1
    assert result["error_count"] == 0


def test_handler_scale_full_mode_invokes_scale_fetcher(handler_module):
    df = pd.DataFrame([{
        "基金代码": "000001",
        "报告期": dt.date(2026, 3, 31),
        "期末净资产_亿元": 5.26,
        "净资产环比变动率": -25.08,
        "snapshot_date": dt.date(2026, 5, 14),
    }])
    fetcher = _make_fetcher_stub(scale_df=df)
    s3 = MagicMock()
    s3.upload_dataframe.return_value = {"key": "_history_staging/fund_scale_history__part0.parquet", "size": 100}

    result = handler_module.run(
        event={"mode": "scale_full", "fund_codes": ["000001"],
               "partition_index": 0, "partition_total": 1},
        fetcher=fetcher, s3_client=s3,
    )

    fetcher.fetch_scale_history.assert_called_once()
    fetcher.fetch_manager_history.assert_not_called()
    upload_kwargs = s3.upload_dataframe.call_args.kwargs
    assert upload_kwargs["category"] == "_history_staging"
    assert upload_kwargs["data_name"] == "fund_scale_history__part0"
    assert result["mode"] == "scale_full"
    assert result["row_count"] == 1


def test_handler_propagates_errors(handler_module):
    fetcher = _make_fetcher_stub(
        manager_df=pd.DataFrame(),
        errors=[{"基金代码": "000001", "error": "RuntimeError: timeout"}],
    )
    s3 = MagicMock()

    result = handler_module.run(
        event={"mode": "manager_full", "fund_codes": ["000001"],
               "partition_index": 0, "partition_total": 1},
        fetcher=fetcher, s3_client=s3,
    )

    assert result["error_count"] == 1
    assert result["errors"][0]["基金代码"] == "000001"
    # No upload when DataFrame is empty
    s3.upload_dataframe.assert_not_called()


def test_handler_rejects_invalid_mode(handler_module):
    fetcher = _make_fetcher_stub()
    s3 = MagicMock()

    with pytest.raises(ValueError, match="invalid mode"):
        handler_module.run(
            event={"mode": "bogus", "fund_codes": ["000001"],
                   "partition_index": 0, "partition_total": 1},
            fetcher=fetcher, s3_client=s3,
        )


def test_handler_slices_when_partition_total_gt_1(handler_module):
    df = pd.DataFrame()
    fetcher = _make_fetcher_stub(manager_df=df)
    s3 = MagicMock()

    handler_module.run(
        event={
            "mode": "manager_full",
            "fund_codes": [f"{i:06d}" for i in range(10)],
            "partition_index": 1,
            "partition_total": 2,
        },
        fetcher=fetcher, s3_client=s3,
    )

    args, _ = fetcher.fetch_manager_history.call_args
    # 10 codes split into 2 partitions of 5; partition 1 = last 5
    assert args[0] == [f"{i:06d}" for i in range(5, 10)]


def test_handler_bootstraps_fund_codes_when_omitted(handler_module):
    """When fund_codes is missing, handler calls list_provider to fetch the universe."""
    fetcher = _make_fetcher_stub(manager_df=pd.DataFrame())
    s3 = MagicMock()
    list_provider = MagicMock(return_value=[f"{i:06d}" for i in range(8)])

    handler_module.run(
        event={
            "mode": "manager_full",
            "partition_index": 0,
            "partition_total": 2,
        },
        fetcher=fetcher,
        s3_client=s3,
        list_provider=list_provider,
    )

    list_provider.assert_called_once()
    args, _ = fetcher.fetch_manager_history.call_args
    # First half of 8 codes
    assert args[0] == [f"{i:06d}" for i in range(4)]


# ---- merge mode ----

import boto3
from moto import mock_aws

BUCKET = "test-bucket"


@pytest.fixture
def real_s3():
    """Real boto3 S3 client backed by moto. Used for merge tests where we
    need actual S3 read/write semantics, not a mock S3Client."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def _put_parquet(s3_client, key: str, df: pd.DataFrame):
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())


CANONICAL_KEY = "fund/_history/fund_manager_history.parquet"
SCALE_CANONICAL_KEY = "fund/_history/fund_scale_history.parquet"
STAGING_PREFIX = "_history_staging"


def test_handler_manager_merge_writes_canonical_path(handler_module, real_s3):
    """Merge reads from staging, writes ONE canonical file under fund/_history/
    (not date-partitioned). S3 versioning preserves history; replication
    picks it up via the fund/ prefix rule."""
    snapshot = dt.date(2026, 5, 14)
    df0 = pd.DataFrame([{"基金代码": "000001", "经理姓名": "张三", "snapshot_date": snapshot}])
    df1 = pd.DataFrame([{"基金代码": "000002", "经理姓名": "李四", "snapshot_date": snapshot}])
    _put_parquet(real_s3, f"{STAGING_PREFIX}/fund_manager_history__part0.parquet", df0)
    _put_parquet(real_s3, f"{STAGING_PREFIX}/fund_manager_history__part1.parquet", df1)

    from shared.storage import S3Client
    s3_client = S3Client(BUCKET)

    result = handler_module.run(
        event={"mode": "manager_merge", "partition_total": 2},
        fetcher=MagicMock(),
        s3_client=s3_client,
        boto3_s3=real_s3,
    )

    obj = real_s3.get_object(Bucket=BUCKET, Key=CANONICAL_KEY)
    import io
    merged = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    assert len(merged) == 2
    assert set(merged["基金代码"]) == {"000001", "000002"}
    assert result["mode"] == "manager_merge"
    assert result["row_count"] == 2
    assert result["parts_merged"] == 2
    assert result["s3"]["key"].endswith("fund/_history/fund_manager_history.parquet")


def test_handler_merge_skips_missing_parts(handler_module, real_s3):
    df0 = pd.DataFrame([{"基金代码": "000001"}])
    df2 = pd.DataFrame([{"基金代码": "000003"}])
    _put_parquet(real_s3, f"{STAGING_PREFIX}/fund_manager_history__part0.parquet", df0)
    # part1 missing
    _put_parquet(real_s3, f"{STAGING_PREFIX}/fund_manager_history__part2.parquet", df2)
    # part3 missing

    from shared.storage import S3Client
    result = handler_module.run(
        event={"mode": "manager_merge", "partition_total": 4},
        fetcher=MagicMock(),
        s3_client=S3Client(BUCKET),
        boto3_s3=real_s3,
    )

    obj = real_s3.get_object(Bucket=BUCKET, Key=CANONICAL_KEY)
    import io
    merged = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    assert len(merged) == 2
    assert result["parts_merged"] == 2
    assert result["parts_missing"] == 2


def test_handler_merge_deletes_staging_parts(handler_module, real_s3):
    df0 = pd.DataFrame([{"基金代码": "000001"}])
    _put_parquet(real_s3, f"{STAGING_PREFIX}/fund_manager_history__part0.parquet", df0)

    from shared.storage import S3Client
    handler_module.run(
        event={"mode": "manager_merge", "partition_total": 1},
        fetcher=MagicMock(),
        s3_client=S3Client(BUCKET),
        boto3_s3=real_s3,
    )

    # Staging part file should be gone
    staging = real_s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{STAGING_PREFIX}/")
    staging_keys = [o["Key"] for o in staging.get("Contents", [])]
    assert f"{STAGING_PREFIX}/fund_manager_history__part0.parquet" not in staging_keys
    # Canonical file exists
    canonical = real_s3.list_objects_v2(Bucket=BUCKET, Prefix="fund/_history/")
    canonical_keys = [o["Key"] for o in canonical.get("Contents", [])]
    assert CANONICAL_KEY in canonical_keys


def test_handler_merge_no_parts_returns_empty_result(handler_module, real_s3):
    from shared.storage import S3Client
    result = handler_module.run(
        event={"mode": "scale_merge", "partition_total": 4},
        fetcher=MagicMock(),
        s3_client=S3Client(BUCKET),
        boto3_s3=real_s3,
    )

    assert result["parts_merged"] == 0
    assert result["parts_missing"] == 4
    assert result["row_count"] == 0
    # No canonical file should exist either
    listed = real_s3.list_objects_v2(Bucket=BUCKET, Prefix="fund/_history/")
    assert not listed.get("Contents")
