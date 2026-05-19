"""Tests for the S3-checkpoint branch added to backfill_fund_history.Progress."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws


def _load_script_module():
    """Load the backfill script as a module (it's not on a Python package path)."""
    script = Path(__file__).resolve().parent.parent / "lambda" / "backfill-runner" / "backfill_fund_history.py"
    spec = importlib.util.spec_from_file_location("backfill_fund_history", script)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["backfill_fund_history"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def backfill_mod():
    return _load_script_module()


@mock_aws
def test_progress_load_from_missing_s3_returns_empty(backfill_mod):
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
    prog = backfill_mod.Progress.load("s3://test-bucket/missing.json")
    assert prog.done == set()
    assert prog.failed == {}


@mock_aws
def test_progress_save_and_reload_via_s3(backfill_mod):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    prog = backfill_mod.Progress(done={"000001", "000002"},
                                 failed={"999999": "ak timeout"},
                                 started_at="2026-05-11T00:00:00")
    prog.save("s3://test-bucket/progress.json")

    # Re-fetch raw and assert JSON shape
    raw = json.loads(s3.get_object(Bucket="test-bucket", Key="progress.json")["Body"].read())
    assert set(raw["done"]) == {"000001", "000002"}
    assert raw["failed"] == {"999999": "ak timeout"}
    assert "updated_at" in raw

    # Reload via helper
    prog2 = backfill_mod.Progress.load("s3://test-bucket/progress.json")
    assert prog2.done == {"000001", "000002"}
    assert prog2.failed == {"999999": "ak timeout"}


@mock_aws
def test_progress_roundtrip_local_path_unchanged(backfill_mod, tmp_path):
    """Regression: local-file branch still works exactly as before."""
    p = tmp_path / "progress.json"
    prog = backfill_mod.Progress(done={"000001"}, failed={},
                                 started_at="2026-05-11T00:00:00")
    prog.save(p)
    prog2 = backfill_mod.Progress.load(p)
    assert prog2.done == {"000001"}


def test_is_s3_uri_helper(backfill_mod):
    assert backfill_mod._is_s3_uri("s3://bucket/key")
    assert not backfill_mod._is_s3_uri("/tmp/progress.json")
    assert not backfill_mod._is_s3_uri(Path("/tmp/progress.json"))
    assert not backfill_mod._is_s3_uri(None)
