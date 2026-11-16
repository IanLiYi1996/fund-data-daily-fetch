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
    monkeypatch.setattr(runner, "fetch_holdings", lambda *a, **kw: pd.DataFrame(_rows()))
    code, frame, error = runner._fetch_one(("000001", "fixture"), [2026], date(2026, 9, 30))
    assert error is None
    assert set(frame.report_date) == {date(2026, 9, 30)}


def test_resume_read_error_is_not_blind_full_refresh(runner):
    catalog = MagicMock()
    catalog.load_table.side_effect = RuntimeError("access denied")
    with pytest.raises(RuntimeError, match="resume set"):
        runner._load_done_set(catalog, "fund_portfolio_hold", date(2026, 9, 30))


def test_failed_code_retry_is_bounded_and_respects_completed_quarter(runner, monkeypatch):
    monkeypatch.setattr("sys.argv", [
        "portfolio", "--target-quarter", "2026-Q3", "--no-export",
        "--failure-plan", "s3://fixture/failures.json",
    ])
    # A retry must not depend on a changing recent-name universe.
    monkeypatch.setattr(runner, "_build_universe", MagicMock(side_effect=AssertionError("full scan")))
    monkeypatch.setattr(runner, "_load_failure_codes", lambda _: {"000001", "000002"}, raising=False)
    monkeypatch.setattr(runner, "load_catalog", MagicMock())
    monkeypatch.setattr(runner, "_load_done_set", lambda *a: {"000001"})
    fetch = MagicMock(return_value=("000002", None, "no-data"))
    monkeypatch.setattr(runner, "_fetch_one", fetch)
    monkeypatch.setattr(runner, "IcebergWriter", MagicMock())
    export = MagicMock()
    monkeypatch.setattr(runner, "_export_flat_parquet", export)
    assert runner.main() == 0
    assert fetch.call_count == 1
    assert fetch.call_args.args[0][0] == "000002"
    export.assert_not_called()


def test_frequency_cap_honors_retry_after_without_identity_change(runner, monkeypatch):
    import requests
    response = requests.Response()
    response.status_code = 514
    response.headers["Retry-After"] = "45"
    error = requests.HTTPError("Frequency Capped", response=response)
    fetch = MagicMock(side_effect=[error, pd.DataFrame(_rows()[1:])])
    sleep = MagicMock()
    monkeypatch.setattr(runner, "fetch_holdings", fetch)
    monkeypatch.setattr(runner.time, "sleep", sleep)
    code, frame, err = runner._fetch_one(("000001", "fixture"), [2026], date(2026, 9, 30))
    assert err is None and len(frame) == 1
    sleep.assert_called_once_with(45)
    assert fetch.call_args_list[0] == fetch.call_args_list[1]
    assert fetch.call_args.kwargs["target_quarter"] == date(2026, 9, 30)


def test_network_retries_stop_after_three_attempts(runner, monkeypatch):
    import requests
    fetch = MagicMock(side_effect=requests.ReadTimeout("source timeout"))
    sleep = MagicMock()
    monkeypatch.setattr(runner, "fetch_holdings", fetch)
    monkeypatch.setattr(runner.time, "sleep", sleep)
    _, frame, err = runner._fetch_one(("000001", ""), [2026], date(2026, 9, 30))
    assert frame is None and err.startswith("ReadTimeout:")
    assert fetch.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [5, 10]


def test_long_source_rate_cap_fails_closed_instead_of_retrying_early(runner, monkeypatch):
    import requests
    response = requests.Response()
    response.status_code = 429
    response.headers["Retry-After"] = "3600"
    fetch = MagicMock(side_effect=requests.HTTPError("capped", response=response))
    sleep = MagicMock()
    monkeypatch.setattr(runner, "fetch_holdings", fetch)
    monkeypatch.setattr(runner.time, "sleep", sleep)
    _, frame, err = runner._fetch_one(("000001", ""), [2026], date(2026, 9, 30))
    assert frame is None and err.startswith("HTTPError:")
    assert fetch.call_count == 1
    sleep.assert_not_called()


def test_audited_failure_plan_is_validated_before_retry(export_fixture):
    import json
    module, s3, bucket, _ = export_fixture
    key = "fund-data-pipeline/_repairs/fixture/failures.json"
    uri = f"s3://{bucket}/{key}"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps({"000001": "ReadTimeout"}))
    assert module._load_failure_codes(uri) == {"000001"}
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps({"000001": ""}))
    with pytest.raises(ValueError, match="code/error"):
        module._load_failure_codes(uri)
    with pytest.raises(ValueError, match="private repair prefix"):
        module._load_failure_codes(f"s3://{bucket}/fund-data-pipeline/public.json")


def test_refreshed_quarter_does_not_resurrect_removed_baseline_stocks(export_fixture):
    module, s3, bucket, _ = export_fixture
    obsolete = {**_rows()[1], "holding_code": "601857"}
    body = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(_rows() + [obsolete]), body)
    key = "fund-data-pipeline/fund/_history/fund_portfolio_hold_history.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=body.getvalue())
    module._export_flat_parquet("fixture", date(2026, 9, 30))
    uploaded = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    rows = pq.ParquetFile(io.BytesIO(uploaded)).read(use_threads=False).to_pylist()
    assert rows == _rows() or sorted(rows, key=lambda row: row["report_date"]) == _rows()
    assert obsolete not in rows


@pytest.fixture
def legacy_quarter_fixture(runner, monkeypatch, tmp_path):
    from pyiceberg.catalog import load_catalog
    from shared.schemas.registry import TABLES
    catalog = load_catalog(
        "legacy_test", type="sql",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}",
    )
    catalog.create_namespace("fund_data_lake")
    spec = TABLES["fund_portfolio_hold"]
    table = catalog.create_table(
        ("fund_data_lake", "fund_portfolio_hold"), schema=spec.schema,
        partition_spec=spec.partition_spec,
    )
    obsolete = {**_rows()[1], "holding_code": "601857"}
    other = {**_rows()[1], "fund_code": "000002"}
    original = _rows() + [obsolete, other]
    table.append(pa.Table.from_pylist(original).cast(table.schema().as_arrow()))
    with mock_aws():
        bucket = "portfolio-legacy-test"
        monkeypatch.setattr(runner, "DEFAULT_BUCKET", bucket)
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)
        yield runner, catalog, original, s3, bucket


def test_existing_quarter_is_replaced_without_touching_older_or_other_funds(
    legacy_quarter_fixture, monkeypatch,
):
    module, catalog, original, s3, bucket = legacy_quarter_fixture
    new = {**_rows()[1], "weight_pct": 3.0}
    monkeypatch.setattr(module, "_fetch_one",
                        lambda *a: ("000001", pd.DataFrame([new]), None))
    result = module._refresh_existing(
        catalog, {"000001"}, [2026], date(2026, 9, 30), workers=1,
    )
    assert result["changed_funds"] == 1
    table = catalog.load_table(("fund_data_lake", "fund_portfolio_hold"))
    actual = table.scan().to_arrow().to_pylist()
    assert _rows()[0] in actual
    assert original[-1] in actual
    assert new in actual and original[2] not in actual
    assert len(actual) == 3
    keys = [obj["Key"] for obj in s3.list_objects_v2(Bucket=bucket)["Contents"]]
    assert any(key.endswith("/existing-quarter-before.parquet") for key in keys)


@pytest.mark.parametrize("error", ["ReadTimeout: source failed", "no-data"])
def test_existing_quarter_cannot_be_deleted_on_failed_or_absent_source(
    legacy_quarter_fixture, monkeypatch, error,
):
    module, catalog, _, _, _ = legacy_quarter_fixture
    before = catalog.load_table(("fund_data_lake", "fund_portfolio_hold")).current_snapshot().snapshot_id
    monkeypatch.setattr(module, "_fetch_one", lambda *a: ("000001", None, error))
    monkeypatch.setattr(module.time, "sleep", MagicMock())
    with pytest.raises(RuntimeError, match="existing-quarter source validation failed"):
        module._refresh_existing(
            catalog, {"000001"}, [2026], date(2026, 9, 30), workers=1,
        )
    after = catalog.load_table(("fund_data_lake", "fund_portfolio_hold")).current_snapshot().snapshot_id
    assert before == after


def test_existing_quarter_transient_empty_is_retried_before_replacement(
    legacy_quarter_fixture, monkeypatch,
):
    module, catalog, _, _, _ = legacy_quarter_fixture
    fetch = MagicMock(side_effect=[
        ("000001", None, "no-data"),
        ("000001", pd.DataFrame(_rows()[1:]), None),
    ])
    sleep = MagicMock()
    monkeypatch.setattr(module, "_fetch_one", fetch)
    monkeypatch.setattr(module.time, "sleep", sleep)
    result = module._refresh_existing(
        catalog, {"000001"}, [2026], date(2026, 9, 30), workers=1,
    )
    assert result["validated_funds"] == 1 and result["changed_funds"] == 1
    assert fetch.call_count == 2
    sleep.assert_called_once_with(5)
