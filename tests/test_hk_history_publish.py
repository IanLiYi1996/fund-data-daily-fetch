"""Publishing reconstruction never overwrites the original fund datasets."""
import importlib.util
import io
from pathlib import Path

import boto3
import pyarrow.parquet as pq
import pytest
from moto import mock_aws


def repair():
    path = Path(__file__).resolve().parents[1] / "scripts/reconstruct_hk_history_20261027.py"
    spec = importlib.util.spec_from_file_location("hk_history_repair", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@mock_aws
def test_immutable_publish_is_idempotent_and_preserves_original_objects():
    module = repair()
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=module.BUCKET)
    original = "fund-data-pipeline/fund/2026-10-27/fund_hk_rank.parquet"
    s3.put_object(Bucket=module.BUCKET, Key=original, Body=b"original")
    key = module.OUTPUT_PREFIX + "as_of_date=2026-10-27/part-0.parquet"
    manifest = module.OUTPUT_PREFIX + "manifest.json"
    objects = {key: b"panel", manifest: b"manifest"}
    first = module.publish_bundle(s3, objects)
    second = module.publish_bundle(s3, objects)
    assert first["created"] == [key, manifest]
    assert second["created"] == []
    assert second["existing"] == [key, manifest]
    assert s3.get_object(Bucket=module.BUCKET, Key=original)["Body"].read() == b"original"


@mock_aws
def test_conflicting_existing_object_aborts_before_any_write():
    module = repair()
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=module.BUCKET)
    key = module.OUTPUT_PREFIX + "as_of_date=2026-10-27/part-0.parquet"
    manifest = module.OUTPUT_PREFIX + "manifest.json"
    s3.put_object(Bucket=module.BUCKET, Key=manifest, Body=b"different")
    with pytest.raises(ValueError, match="conflict"):
        module.publish_bundle(s3, {key: b"panel", manifest: b"manifest"})
    assert s3.list_objects_v2(Bucket=module.BUCKET)["KeyCount"] == 1
    assert s3.get_object(Bucket=module.BUCKET, Key=manifest)["Body"].read() == b"different"


def test_publish_rejects_original_and_unrelated_prefixes():
    module = repair()
    from unittest.mock import Mock
    s3 = Mock()
    with pytest.raises(ValueError, match="scope"):
        module.publish_bundle(s3, {"fund-data-pipeline/fund_history/part-0.parquet": b"bad"})
    s3.get_object.assert_not_called()
    s3.put_object.assert_not_called()


def test_consumer_namespace_uses_existing_replication_without_mixing_raw_dates():
    module = repair()
    assert module.OUTPUT_PREFIX == "fund-data-pipeline/fund/_reconstructed/fund_hk_history/"
    assert not module.OUTPUT_PREFIX.startswith("fund-data-pipeline/fund/_history/")


def test_replication_rule_is_checked_without_changing_any_policy():
    module = repair()
    config = {"ReplicationConfiguration": {"Rules": [{
        "Status": "Enabled", "Filter": {"Prefix": "fund-data-pipeline/fund/"},
        "Destination": {"Bucket": "arn:aws:s3:::financial-dataset-mx"},
    }]}}
    module.assert_replication_coverage(config)


@pytest.mark.parametrize("filter_,status,destination", [
    ({"Prefix": "fund-data-pipeline/fund_history/"}, "Enabled", "financial-dataset-mx"),
    ({"Prefix": "fund-data-pipeline/fund/"}, "Disabled", "financial-dataset-mx"),
    ({"And": {"Prefix": "fund-data-pipeline/fund/", "Tags": []}},
     "Enabled", "financial-dataset-mx"),
    ({"Prefix": "fund-data-pipeline/fund/"}, "Enabled", "different-bucket"),
])
def test_nonmatching_replication_never_silently_publishes(filter_, status, destination):
    module = repair()
    config = {"ReplicationConfiguration": {"Rules": [{
        "Status": status, "Filter": filter_,
        "Destination": {"Bucket": "arn:aws:s3:::" + destination},
    }]}}
    with pytest.raises(ValueError, match="replication"):
        module.assert_replication_coverage(config)


@mock_aws
def test_manifest_is_published_last_even_if_input_order_is_reversed():
    module = repair()
    from unittest.mock import Mock
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=module.BUCKET)
    s3 = Mock(wraps=client)
    manifest = module.OUTPUT_PREFIX + "manifest.json"
    key = module.OUTPUT_PREFIX + "as_of_date=2026-10-27/part-0.parquet"
    module.publish_bundle(s3, {manifest: b"manifest", key: b"panel"})
    assert [c.kwargs["Key"] for c in s3.put_object.call_args_list] == [key, manifest]
    assert all(c.kwargs["IfNoneMatch"] == "*" for c in s3.put_object.call_args_list)


@mock_aws
def test_changed_protected_original_prevents_manifest_publication():
    module = repair()
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=module.BUCKET)
    manifest = module.OUTPUT_PREFIX + "manifest.json"
    key = module.OUTPUT_PREFIX + "as_of_date=2026-10-27/part-0.parquet"

    def changed():
        raise ValueError("Original changed")

    with pytest.raises(ValueError, match="Original changed"):
        module.publish_bundle(s3, {key: b"panel", manifest: b"manifest"},
                              before_manifest=changed)
    assert [o["Key"] for o in s3.list_objects_v2(Bucket=module.BUCKET)["Contents"]] == [key]


def test_permission_failure_is_not_treated_as_a_missing_object():
    module = repair()
    from unittest.mock import Mock
    from botocore.exceptions import ClientError
    s3 = Mock()
    s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied"}}, "GetObject",
    )
    with pytest.raises(ClientError):
        module.publish_bundle(s3, {module.OUTPUT_PREFIX + "manifest.json": b"manifest"})
    s3.put_object.assert_not_called()


def test_parquet_keeps_origin_dates_and_nullable_numbers():
    module = repair()
    from shared.quality.hk_history import build_valuations, normalize_members
    from datetime import date
    members = normalize_members([{
        "基金代码": "968063", "香港基金代码": "1002075744", "基金简称": "test",
        "币种": "美元", "日期": None, "单位净值": None,
    }])
    rows = build_valuations(members, {"968063": []}, [date(2026, 10, 27)])
    data = module.panel_parquet(rows)
    table = pq.ParquetFile(io.BytesIO(data)).read(use_threads=False)
    assert table["unit_nav"].type == module.pa.float64()
    assert table["unit_nav"].null_count == 1
    assert table["as_of_date"].to_pylist() == [date(2026, 10, 27)]
    assert table["data_origin"].to_pylist() == ["reconstructed"]
    assert table["observation_time_recovered"].to_pylist() == [False]
    assert table.schema.metadata[b"original_snapshot_recovered"] == b"false"
