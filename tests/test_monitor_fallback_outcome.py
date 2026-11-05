"""A caught fallback failure must not be hidden by workflow SUCCEEDED."""
import importlib.util
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest


def load():
    for key, value in {
        "S3_BUCKET": "unit-test",
        "WAREHOUSE_PATH": "s3://unit-test/warehouse/",
        "SLACK_WEBHOOK_SECRET_ARN": "unused-in-this-test",
        "STATE_MACHINE_ARN": "unused-in-this-test",
    }.items():
        os.environ.setdefault(key, value)
    spec = importlib.util.spec_from_file_location(
        "monitor_fallback_test",
        Path(__file__).resolve().parents[1] / "lambda/freshness-check/handler.py",
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def events(output):
    return [
        {"type": "TaskStateEntered",
         "stateEnteredEventDetails": {"name": "InvokeFundFallback", "input": "{}"}},
        {"type": "TaskStateExited",
         "stateExitedEventDetails": {
             "name": "InvokeFundFallback", "output": json.dumps(output),
         }},
    ]


@pytest.mark.parametrize("output", [
    {"Error": "Sandbox.Timedout", "Cause": "Task timed out after 900.00 seconds"},
    {"success": False, "complete": False, "pending": 6000, "errors": 0},
    {"success": True, "errors": 2},
    {"success": True, "money_verify": {"complete": False, "pending": 500}},
])
def test_actual_workflow_check_reports_fallback_failure(monkeypatch, output):
    monitor = load()
    started = datetime.now(timezone.utc) - timedelta(minutes=30)
    sfn = Mock()
    sfn.list_executions.return_value = {"executions": [{
        "status": "SUCCEEDED", "startDate": started,
        "stopDate": started + timedelta(minutes=24), "executionArn": "fixture",
    }]}
    sfn.get_execution_history.return_value = {"events": events(output)}
    monkeypatch.setattr(monitor.boto3, "client", Mock(return_value=sfn))
    problems, facts = [], []
    monitor._check_workflow(problems, facts)
    assert any("补采" in message for message in problems)
    assert any("SUCCEEDED" in fact for fact in facts)


def test_successful_legacy_fallback_is_not_falsely_reported(monkeypatch):
    monitor = load()
    started = datetime.now(timezone.utc) - timedelta(minutes=30)
    sfn = Mock()
    sfn.list_executions.return_value = {"executions": [{
        "status": "SUCCEEDED", "startDate": started,
        "stopDate": started + timedelta(minutes=10), "executionArn": "fixture",
    }]}
    sfn.get_execution_history.return_value = {
        "events": events({"success": True, "errors": 0}),
    }
    monkeypatch.setattr(monitor.boto3, "client", Mock(return_value=sfn))
    problems, facts = [], []
    monitor._check_workflow(problems, facts)
    assert problems == []
