"""New-month waiting must not hide last month's late NAVs or real export faults."""
from __future__ import annotations

import importlib.util
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def check(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "monitor-rollover-test")
    monkeypatch.setenv("S3_PREFIX", "fund-data-pipeline/")
    monkeypatch.setenv("WAREHOUSE_PATH", "s3://monitor-rollover-test/warehouse/")
    monkeypatch.setenv("SLACK_WEBHOOK_SECRET_ARN",
                       "arn:aws:secretsmanager:us-east-1:0:secret:fixture")
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:us-east-1:0:stateMachine:fixture")
    path = Path(__file__).resolve().parents[1] / "lambda/freshness-check/handler.py"
    spec = importlib.util.spec_from_file_location("monitor_export_rollover", path)
    handler = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(handler)

    def run(days, objects, today=date(2026, 11, 2)):
        now = datetime(today.year, today.month, today.day, 8, tzinfo=timezone.utc)

        class PinnedDate(date):
            @classmethod
            def today(cls):
                return today

        class PinnedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return now.astimezone(tz) if tz else now.replace(tzinfo=None)

        monkeypatch.setattr(handler, "date", PinnedDate)
        monkeypatch.setattr(handler, "datetime", PinnedDatetime)
        catalog = MagicMock()
        catalog.load_table.return_value.scan.return_value.to_arrow.return_value = pa.table({
            "trade_date": pa.array(days, type=pa.date32()),
        })
        monkeypatch.setattr(handler, "_catalog", lambda: catalog)
        s3 = MagicMock()
        s3.exceptions.ClientError = ClientError

        def head(**kwargs):
            month = kwargs["Key"].split("trade_month=")[1].split("/")[0]
            obj = objects.get(month)
            if obj is None or "error" in obj:
                raise ClientError({"Error": {"Code": (obj or {}).get("error", "404")}},
                                  "HeadObject")
            return {
                "LastModified": now, "ContentLength": 1024,
                "ReplicationStatus": "COMPLETED", **obj,
            }

        s3.head_object.side_effect = head
        monkeypatch.setattr(handler.boto3, "client", lambda *args, **kwargs: s3)
        problems, facts = [], []
        handler._check_export(problems, facts)
        s3.put_object.assert_not_called()
        return problems, facts, s3

    return run


def test_empty_november_checks_october_and_reports_waiting(check):
    problems, facts, s3 = check([date(2026, 10, 30)], {"2026-10": {}})
    assert problems == []
    assert any("2026-11" in fact and "尚无" in fact for fact in facts)
    assert s3.head_object.call_count == 1
    assert "trade_month=2026-10/" in s3.head_object.call_args.kwargs["Key"]


def test_november_missing_after_first_nav_still_fails(check):
    problems, _, _ = check([date(2026, 10, 30), date(2026, 11, 2)], {"2026-10": {}})
    assert any("2026-11" in problem and "404" in problem for problem in problems)


def test_current_export_does_not_hide_missing_previous_month(check):
    problems, _, _ = check([date(2026, 10, 30), date(2026, 11, 2)], {"2026-11": {}})
    assert any("2026-10" in problem and "404" in problem for problem in problems)


def test_previous_month_lag_still_fails_while_new_month_waits(check):
    old = datetime(2026, 10, 31, 8, tzinfo=timezone.utc)
    problems, _, _ = check([date(2026, 10, 30)], {"2026-10": {"LastModified": old}})
    assert any("48.0" in problem for problem in problems)


def test_previous_month_replication_pending_still_fails(check):
    problems, _, _ = check([date(2026, 10, 30)],
                           {"2026-10": {"ReplicationStatus": "PENDING"}})
    assert any("PENDING" in problem for problem in problems)


def test_permission_error_is_not_suppressed_by_rollover(check):
    problems, _, _ = check([date(2026, 10, 30)], {"2026-10": {"error": "403"}})
    assert any("403" in problem for problem in problems)


def test_no_nav_data_does_not_silently_pass(check):
    problems, _, s3 = check([], {})
    assert any("净值数据" in problem for problem in problems)
    s3.head_object.assert_not_called()


def test_first_month_of_lake_does_not_require_nonexistent_previous_data(check):
    problems, _, s3 = check([date(2026, 11, 2)], {"2026-11": {}})
    assert problems == []
    assert s3.head_object.call_count == 1


def test_january_rollover_checks_december_not_month_zero(check):
    problems, _, s3 = check([date(2026, 12, 31)], {"2026-12": {}}, today=date(2027, 1, 1))
    assert problems == []
    assert "trade_month=2026-12/" in s3.head_object.call_args.kwargs["Key"]
