"""Portfolio refresh regression tests: real export SQL, no remote writes."""
from __future__ import annotations

import importlib.util
import io
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import boto3
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws


@pytest.fixture
def runner(monkeypatch):
    path = Path(__file__).resolve().parents[1] / "lambda/backfill-runner/backfill_portfolio_hold.py"
    # Loading the runner must not permanently patch requests for other tests.
    import requests
    original = requests.Session.request
    spec = importlib.util.spec_from_file_location("portfolio_runner_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(requests.Session, "request", original)
    monkeypatch.setattr(module, "MIN_EXPORT_FUNDS", 1, raising=False)
    return module


def _rows():
    return [
        {"fund_code": "000001", "report_date": date(2026, 6, 30),
         "holding_code": "600000", "holding_name": "old",
         "weight_pct": 1.0, "shares": 100.0, "market_value": 200.0},
        {"fund_code": "000001", "report_date": date(2026, 9, 30),
         "holding_code": "600001", "holding_name": "new",
         "weight_pct": 2.0, "shares": 200.0, "market_value": 300.0},
    ]


@pytest.fixture
def export_fixture(runner, monkeypatch, tmp_path):
    data = tmp_path / "live.parquet"
    pq.write_table(pa.Table.from_pylist(_rows()), data)
    table = MagicMock()
    table.scan.return_value.plan_files.return_value = [
        SimpleNamespace(file=SimpleNamespace(file_path=str(data))),
    ]
    catalog = MagicMock()
    catalog.load_table.return_value = table
    monkeypatch.setattr(runner, "load_catalog", lambda *a, **kw: catalog)
    real_connect = duckdb.connect

    class Connection:
        def __init__(self):
            self.real = real_connect()

        def sql(self, query):
            if query.startswith("CREATE SECRET"):
                return self.real.sql("SELECT true")
            return self.real.sql(query)

        def close(self):
            self.real.close()

    monkeypatch.setattr(duckdb, "connect", Connection)
    with mock_aws():
        bucket = "portfolio-export-test"
        monkeypatch.setattr(runner, "DEFAULT_BUCKET", bucket)
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        yield runner, s3, bucket, data


def test_actual_export_uploads_with_duckdb_2(export_fixture):
    module, s3, bucket, _ = export_fixture
    module._export_flat_parquet("fixture")
    body = s3.get_object(
        Bucket=bucket,
        Key="fund-data-pipeline/fund/_history/fund_portfolio_hold_history.parquet",
    )["Body"].read()
    uploaded = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False).to_pylist()
    assert sorted(uploaded, key=lambda row: row["report_date"]) == _rows()


def test_no_fetch_work_still_repairs_export(runner, monkeypatch):
    monkeypatch.setattr("sys.argv", ["portfolio", "--target-quarter", "2026-Q3"])
    monkeypatch.setattr(runner, "_build_universe",
                        lambda _: pd.DataFrame({"fund_code": ["000001"], "fund_name": ["fixture"]}))
    monkeypatch.setattr(runner, "load_catalog", MagicMock())
    monkeypatch.setattr(runner, "_load_done_set", lambda *a: {"000001"})
    export = MagicMock()
    monkeypatch.setattr(runner, "_export_flat_parquet", export)
    assert runner.main() == 0
    export.assert_called_once()


def test_failed_source_does_not_publish_a_fresh_export(runner, monkeypatch):
    monkeypatch.setattr("sys.argv", ["portfolio", "--target-quarter", "2026-Q3"])
    monkeypatch.setattr(runner, "_build_universe",
                        lambda _: pd.DataFrame({"fund_code": ["000001"], "fund_name": ["fixture"]}))
    monkeypatch.setattr(runner, "load_catalog", MagicMock())
    monkeypatch.setattr(runner, "_load_done_set", lambda *a: set())
    monkeypatch.setattr(runner, "_fetch_one", lambda *a: ("000001", None, "HTTPError: 404"))
    monkeypatch.setattr(runner, "IcebergWriter", MagicMock())
    export = MagicMock()
    monkeypatch.setattr(runner, "_export_flat_parquet", export)
    assert runner.main() != 0
    export.assert_not_called()


def test_export_retains_previously_published_history(export_fixture):
    module, s3, bucket, _ = export_fixture
    old = {
        **_rows()[0], "fund_code": "000003", "report_date": date(2025, 12, 31),
    }
    body = io.BytesIO()
    pq.write_table(pa.Table.from_pylist([old]), body)
    key = "fund-data-pipeline/fund/_history/fund_portfolio_hold_history.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=body.getvalue())
    module._export_flat_parquet("fixture", date(2026, 9, 30))
    uploaded = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert old in pq.ParquetFile(io.BytesIO(uploaded)).read(use_threads=False).to_pylist()


def test_partial_target_quarter_cannot_refresh_file_timestamp(export_fixture):
    module, s3, bucket, data = export_fixture
    rows = [{**_rows()[0], "fund_code": f"{i:06}"} for i in range(10)] + [_rows()[1]]
    pq.write_table(pa.Table.from_pylist(rows), data)
    key = "fund-data-pipeline/fund/_history/fund_portfolio_hold_history.parquet"
    previous = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows[:-1]), previous)
    s3.put_object(Bucket=bucket, Key=key, Body=previous.getvalue())
    with pytest.raises(RuntimeError, match="coverage incomplete"):
        module._export_flat_parquet("fixture", date(2026, 9, 30))
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == previous.getvalue()


def test_fetch_only_commits_target_quarter(runner, monkeypatch):
    monkeypatch.setattr(runner, "fetch_holdings", lambda *a: pd.DataFrame(_rows()))
    code, frame, error = runner._fetch_one(("000001", "fixture"), [2026], date(2026, 9, 30))
    assert error is None
    assert set(frame.report_date) == {date(2026, 9, 30)}


def test_resume_read_error_is_not_blind_full_refresh(runner):
    catalog = MagicMock()
    catalog.load_table.side_effect = RuntimeError("access denied")
    with pytest.raises(RuntimeError, match="resume set"):
        runner._load_done_set(catalog, "fund_portfolio_hold", date(2026, 9, 30))
