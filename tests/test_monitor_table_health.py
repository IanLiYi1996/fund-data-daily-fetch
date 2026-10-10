"""Pin the two monitor checks that make a silent per-table failure visible.

The collection Map wraps every fund-table Lambda in a catch, so a table
that 502s or times out becomes a `success: false` item (or, on a Lambda
timeout, an error payload with no table name at all) and the workflow still
ends SUCCEEDED. Nothing read those items: fund_etf answered 502 on every
run from 2026-09-18 and fund_lof from 2026-08-26, fund_name timed out on 8
of 12 days to 2026-10-08, and the heartbeat kept saying the run worked
because it only ever looked at fund_daily.

Two pure helpers, both exercised here without AWS:

- `_fund_partition_outcomes` reads the execution history and names the
  tables that failed (with the error) or never returned (timeout).
- `_stale_tables` turns per-table latest dates into the ones past the
  staleness threshold, which catches the same defect even if the history
  parse ever breaks, because it looks at what actually landed in the lake.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import date
from pathlib import Path

import pytest


def _load_monitor_module():
    os.environ.setdefault("S3_BUCKET", "unit-test-bucket")
    os.environ.setdefault("WAREHOUSE_PATH", "s3://unit-test/warehouse/")
    os.environ.setdefault("SLACK_WEBHOOK_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:0:secret:x")
    os.environ.setdefault("STATE_MACHINE_ARN", "arn:aws:states:us-east-1:0:stateMachine:x")
    script = (Path(__file__).resolve().parent.parent
              / "lambda" / "freshness-check" / "handler.py")
    spec = importlib.util.spec_from_file_location("freshness_check_handler", script)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["freshness_check_handler"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mon():
    return _load_monitor_module()


def _entered(table):
    return {"type": "TaskStateEntered",
            "stateEnteredEventDetails": {
                "name": "InvokeFundPartition",
                "input": json.dumps({"table": table})}}


def _exited(output):
    return {"type": "TaskStateExited",
            "stateExitedEventDetails": {
                "name": "InvokeFundPartition",
                "output": output if isinstance(output, str) else json.dumps(output)}}


def _ok(table, rows):
    return _exited({"statusCode": 200, "downloader": "fund", "table": table,
                    "success": True, "rows": rows, "errors": []})


def _failed(table, err):
    return _exited({"statusCode": 200, "downloader": "fund", "table": table,
                    "success": False, "rows": 0,
                    "errors": [{"name": table, "error": err}]})


# Shape of a Lambda timeout as it appears in the history: the Task fails,
# the catch's Pass state gets an error object, and the table name is gone.
_TIMEOUT_OUTPUT = {"Error": "Sandbox.Timedout",
                   "Cause": json.dumps({"errorType": "Sandbox.Timedout",
                                        "errorMessage": "Task timed out after 900.00 seconds"})}


def test_all_ok_reports_nothing(mon):
    events = [_entered("fund_daily"), _entered("fund_etf"),
              _ok("fund_daily", 24222), _ok("fund_etf", 1677)]
    failed, missing = mon._fund_partition_outcomes(events)
    assert failed == {}
    assert missing == []


def test_success_false_is_named_with_its_error(mon):
    events = [_entered("fund_etf"), _entered("fund_lof"),
              _failed("fund_etf", "502 Server Error: Bad Gateway for url: https://push2delay..."),
              _failed("fund_lof", "('Connection aborted.', RemoteDisconnected(...))")]
    failed, missing = mon._fund_partition_outcomes(events)
    assert set(failed) == {"fund_etf", "fund_lof"}
    assert failed["fund_etf"].startswith("502 Server Error")
    assert missing == []


def test_timeout_shows_up_as_missing_table(mon):
    # Concurrent Map iterations: exits do not come in entry order, and the
    # timed-out one carries no table name, so it is whatever entered but
    # never produced a named result.
    events = [_entered("fund_name"), _entered("fund_daily"), _entered("fund_etf"),
              _ok("fund_etf", 1677), _exited(_TIMEOUT_OUTPUT), _ok("fund_daily", 24222)]
    failed, missing = mon._fund_partition_outcomes(events)
    assert failed == {}
    assert missing == ["fund_name"]


def test_other_task_states_are_ignored(mon):
    events = [{"type": "TaskStateEntered",
               "stateEnteredEventDetails": {"name": "InvokeFundFallback", "input": "{}"}},
              {"type": "ExecutionStarted"},
              _entered("fund_daily"), _ok("fund_daily", 1)]
    failed, missing = mon._fund_partition_outcomes(events)
    assert (failed, missing) == ({}, [])


def test_mixed_failure_and_timeout(mon):
    events = [_entered("fund_performance"), _entered("fund_etf"), _entered("fund_name"),
              _failed("fund_etf", "502"), _ok("fund_performance", 5),
              _exited(_TIMEOUT_OUTPUT)]
    failed, missing = mon._fund_partition_outcomes(events)
    assert list(failed) == ["fund_etf"]
    assert missing == ["fund_name"]


TODAY = date(2026, 10, 9)


def test_fresh_tables_are_not_stale(mon):
    latest = {"fund_daily": date(2026, 10, 8), "fund_etf": date(2026, 10, 8)}
    assert mon._stale_tables(latest, today=TODAY, max_age_days=3) == []


def test_stale_table_is_reported_with_age(mon):
    latest = {"fund_etf": date(2026, 9, 18), "fund_lof": date(2026, 8, 26),
              "fund_daily": date(2026, 10, 8)}
    out = mon._stale_tables(latest, today=TODAY, max_age_days=3)
    assert [(t, d) for t, d, _ in out] == [("fund_lof", date(2026, 8, 26)),
                                           ("fund_etf", date(2026, 9, 18))]
    ages = {t: a for t, _, a in out}
    assert ages["fund_etf"] == 21
    assert ages["fund_lof"] == 44


def test_table_with_no_rows_is_stale(mon):
    out = mon._stale_tables({"fund_lof": None}, today=TODAY, max_age_days=3)
    assert out == [("fund_lof", None, None)]


def test_holiday_gap_within_threshold_is_not_stale(mon):
    # 10-01..10-07 is a market holiday; 10-08 data on 10-09 is 1 day old.
    # The threshold has to tolerate a long weekend plus one failed night, so
    # 3 days is the floor; the test pins that a 3-day gap passes.
    assert mon._stale_tables({"x": date(2026, 10, 6)}, today=TODAY, max_age_days=3) == []
    assert mon._stale_tables({"x": date(2026, 10, 5)}, today=TODAY, max_age_days=3) != []
