"""Exercise the consumer export with real DuckDB, including its upload.

Only AWS and the remote Iceberg manifest reads are replaced with local
fixtures. The handler, parquet reads, deduplication, COPY and row count all
run unchanged, so a DuckDB API regression cannot silently skip the upload.
"""
from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime
from pathlib import Path

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.stub import Stubber
from moto import mock_aws


@pytest.fixture
def monthly_export(monkeypatch, tmp_path):
    bucket = "monthly-export-test"
    prefix = "fund-data-pipeline/"
    metadata_key = f"{prefix}iceberg/metadata.json"
    manifest_list_uri = f"s3://{bucket}/manifest-list.avro"
    manifest_uri = f"s3://{bucket}/manifest.avro"
    data_dir = tmp_path / "trade_month=2026-10"
    data_dir.mkdir()
    data_file = data_dir / "data.parquet"

    pq.write_table(pa.Table.from_pylist([
        {
            "fund_code": "000001", "fund_name": "fixture",
            "trade_date": date(2026, 10, 22),
            "unit_nav": 1.0, "accum_nav": 1.0, "daily_return_pct": 0.0,
            "subscription_status": "open", "redemption_status": "open",
            "fee": "0.15%", "ingested_at": datetime(2026, 10, 22, 17),
        },
        {
            "fund_code": "000001", "fund_name": "fixture",
            "trade_date": date(2026, 10, 22),
            "unit_nav": 1.2, "accum_nav": 1.2, "daily_return_pct": 0.0,
            "subscription_status": "open", "redemption_status": "open",
            "fee": "0.15%", "ingested_at": datetime(2026, 10, 23, 17),
        },
        {
            "fund_code": "000002", "fund_name": "fixture",
            "trade_date": date(2026, 10, 23),
            "unit_nav": 2.0, "accum_nav": 2.0, "daily_return_pct": 0.0,
            "subscription_status": "open", "redemption_status": "open",
            "fee": "0.15%", "ingested_at": datetime(2026, 10, 23, 17),
        },
    ]), data_file)
    manifest_list = tmp_path / "manifest-list.parquet"
    manifest = tmp_path / "manifest.parquet"
    pq.write_table(pa.Table.from_pylist([
        {"manifest_path": manifest_uri},
    ]), manifest_list)
    pq.write_table(pa.Table.from_pylist([
        {"data_file": {"file_path": str(data_file)}, "status": 1},
    ]), manifest)

    real_connect = duckdb.connect

    class FixtureConnection:
        """Replace remote extension reads, leaving export SQL untouched."""

        def __init__(self):
            self.connection = real_connect()

        def sql(self, query):
            if query.startswith(("LOAD ", "CREATE SECRET ")):
                return self.connection.sql("SELECT true")
            query = query.replace(
                f"read_avro('{manifest_list_uri}')",
                f"read_parquet('{manifest_list}')",
            ).replace(
                f"read_avro('{manifest_uri}')",
                f"read_parquet('{manifest}')",
            )
            return self.connection.sql(query)

    monkeypatch.setattr(duckdb, "connect", FixtureConnection)
    monkeypatch.setenv("S3_BUCKET", bucket)
    monkeypatch.setenv("S3_PREFIX", prefix)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    path = Path(__file__).resolve().parents[1] / "lambda/export-fund-history/handler.py"
    spec = importlib.util.spec_from_file_location("monthly_history_export", path)
    handler = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(handler)

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        s3.put_object(
            Bucket=bucket, Key=metadata_key,
            Body=json.dumps({
                "current-snapshot-id": 1,
                "snapshots": [{"snapshot-id": 1, "manifest-list": manifest_list_uri}],
            }).encode(),
        )
        glue = boto3.client("glue", region_name="us-east-1")
        real_client = boto3.client
        with Stubber(glue) as stubber:
            stubber.add_response("get_table", {
                "Table": {
                    "Name": "fund_daily",
                    "Parameters": {"metadata_location": f"s3://{bucket}/{metadata_key}"},
                },
            }, {"DatabaseName": "fund_data_lake", "Name": "fund_daily"})
            monkeypatch.setattr(
                boto3, "client",
                lambda service, **kwargs: glue if service == "glue" else real_client(service, **kwargs),
            )
            yield handler, s3, bucket, data_file, manifest


def test_monthly_history_upload_contains_recent_dates_and_newest_correction(monthly_export):
    handler, s3, bucket, _, _ = monthly_export

    result = handler.lambda_handler({"month": "2026-10"}, None)

    assert result["success"] is True
    assert result["rows"] == 2
    key = "fund-data-pipeline/fund_history/trade_month=2026-10/part-0.parquet"
    assert result["s3_key"] == key
    response = s3.get_object(Bucket=bucket, Key=key)
    uploaded = response["Body"].read()
    assert result["bytes"] == len(uploaded)
    rows = pq.read_table(pa.BufferReader(uploaded)).to_pylist()
    assert {(row["fund_code"], row["trade_date"], row["unit_nav"]) for row in rows} == {
        ("000001", date(2026, 10, 22), 1.2),
        ("000002", date(2026, 10, 23), 2.0),
    }


def test_monthly_history_upload_accepts_files_without_ingestion_timestamp(monthly_export):
    handler, s3, bucket, data_file, _ = monthly_export
    legacy = pq.ParquetFile(data_file).read().slice(1).drop(["ingested_at"])
    pq.write_table(legacy, data_file)

    result = handler.lambda_handler({"month": "2026-10"}, None)

    assert result["success"] is True
    assert result["rows"] == 2
    uploaded = s3.get_object(Bucket=bucket, Key=result["s3_key"])["Body"].read()
    rows = pq.read_table(pa.BufferReader(uploaded)).to_pylist()
    assert max(row["trade_date"] for row in rows) == date(2026, 10, 23)


def test_monthly_history_does_not_overwrite_export_with_deleted_snapshot_files(monthly_export):
    handler, s3, bucket, data_file, manifest = monthly_export
    pq.write_table(pa.Table.from_pylist([
        {"data_file": {"file_path": str(data_file)}, "status": 2},
    ]), manifest)
    key = "fund-data-pipeline/fund_history/trade_month=2026-10/part-0.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=b"existing consumer export")

    result = handler.lambda_handler({"month": "2026-10"}, None)

    assert result["success"] is False
    assert result["skipped"] == "no-live-files"
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"existing consumer export"
