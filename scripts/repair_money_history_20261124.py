"""Back up and append only 001973@2026-11-22; re-export November with guards.

Default is read-only. --apply requires the just-verified lake snapshot and
consumer object version. No full-universe Lambda or Slack monitor invocation.
"""
from __future__ import annotations

import argparse
import io
import json
import math
import sys
from datetime import date
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
import requests
from botocore.config import Config
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda"))
from shared.storage.iceberg_writer import IcebergWriter

ACCOUNT = "463470973226"
REGION = "us-east-1"
BUCKET = f"fund-data-pipeline-{ACCOUNT}-{REGION}"
KEY = "fund-data-pipeline/fund_history/trade_month=2026-11/part-0.parquet"
CODE = "001973"
DAY = date(2026, 11, 22)
WORKFLOW = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:FundDataCollectionWorkflow"
FALLBACK = "FundDataFetchStack-FundFallbackLambda82226D2E-dt1F6pJ1tsdM"
EXPORT = "FundDataFetchStack-ExportFundHistoryLambda4F255537-P4Y7z0yGykTd"
PREFIX = "fund-data-pipeline/_repairs/2026-11-24-money-history/"


def require_versions(snapshot, version, expected_snapshot, expected_version):
    if str(snapshot) != str(expected_snapshot) or version != expected_version:
        raise RuntimeError("Lake snapshot or consumer version changed; inspect a new plan")


def require_source_rows(rows):
    if len(rows) != 1 or rows[0].get("FSRQ") != DAY.isoformat():
        raise ValueError("Expected exactly the approved source fund/date, no other dates")
    value = float(rows[0]["DWJZ"])
    if not math.isfinite(value):
        raise ValueError("Non-finite source NAV")
    return value


def verify_export(before, after, expected_nav):
    keys = ["fund_code", "trade_date"]
    if before.duplicated(keys).any() or after.duplicated(keys).any():
        raise RuntimeError("Consumer export contains duplicate keys")
    old = before.set_index(keys).sort_index()
    new = after.set_index(keys).sort_index()
    if len(new) != len(old) + 1 or not old.index.isin(new.index).all():
        raise RuntimeError("Consumer export changed beyond the single approved addition")
    pd.testing.assert_frame_equal(old, new.loc[old.index], check_exact=True)
    extra = new.loc[~new.index.isin(old.index)]
    if list(extra.index) != [(CODE, DAY)] or float(extra.iloc[0].unit_nav) != expected_nav:
        raise RuntimeError("Unexpected new consumer key or value")


def read_export(s3):
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    raw = obj["Body"].read()
    frame = pq.ParquetFile(io.BytesIO(raw)).read(use_threads=False).to_pandas()
    return obj, raw, frame


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-snapshot")
    parser.add_argument("--expected-version")
    args = parser.parse_args()
    session = boto3.Session(region_name=REGION)
    if session.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise RuntimeError("Wrong AWS account")
    s3 = session.client("s3")
    sfn = session.client("stepfunctions")
    if sfn.list_executions(stateMachineArn=WORKFLOW, statusFilter="RUNNING")["executions"]:
        raise RuntimeError("Collection workflow is running; do not race its writer")
    env = session.client("lambda").get_function_configuration(
        FunctionName=FALLBACK,
    )["Environment"]["Variables"]
    # Use the same active role for boto3 and Arrow, never persist its credentials.
    credentials = session.get_credentials().get_frozen_credentials()
    catalog = load_catalog("glue", **{
        "type": "glue", "glue.region": REGION, "warehouse": env["WAREHOUSE_PATH"],
        "s3.region": REGION, "s3.access-key-id": credentials.access_key,
        "s3.secret-access-key": credentials.secret_key,
        "s3.session-token": credentials.token,
    })
    table = catalog.load_table(("fund_data_lake", "fund_daily"))
    snapshot = table.current_snapshot().snapshot_id
    names = catalog.load_table(("fund_data_lake", "fund_name")).scan(
        row_filter=And(EqualTo("fund_code", CODE),
                       GreaterThanOrEqual("snapshot_date", DAY)),
        selected_fields=("fund_code", "fund_name", "fund_type", "snapshot_date"),
    ).to_arrow().to_pandas()
    if names.empty:
        raise RuntimeError("No recent fund membership")
    name = names.sort_values("snapshot_date").iloc[-1]
    if not str(name.fund_type).startswith("货币型"):
        raise RuntimeError("Approved fund is not money-market")
    held = table.scan(
        row_filter=And(EqualTo("fund_code", CODE), EqualTo("trade_date", DAY)),
        selected_fields=("fund_code", "trade_date", "unit_nav"),
    ).to_arrow().to_pandas()
    before_obj, before_raw, before = read_export(s3)
    response = requests.get(
        "https://api.fund.eastmoney.com/f10/lsjz",
        params={"fundCode": CODE, "pageIndex": 1, "pageSize": 50,
                "startDate": DAY.isoformat(), "endDate": DAY.isoformat()},
        headers={"Referer": "https://fundf10.eastmoney.com/", "User-Agent": "Mozilla/5.0"},
        timeout=(10, 20),
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("ErrCode") not in (None, 0, "0"):
        raise ValueError("Upstream reported an error")
    source = (payload.get("Data") or {}).get("LSJZList") or []
    nav = require_source_rows(source)
    already_exported = before[
        (before.fund_code == CODE) & (before.trade_date == DAY)
    ]
    if not held.empty or not already_exported.empty:
        if (len(held) == len(already_exported) == 1
                and float(held.iloc[0].unit_nav) == nav
                and float(already_exported.iloc[0].unit_nav) == nav):
            print(json.dumps({"already_repaired": True, "nav": nav}))
            return
        raise RuntimeError("Approved key already exists in one layer; inspect before writing")
    row = source[0]
    frame = pd.DataFrame([{
        "fund_code": CODE, "fund_name": name.fund_name, "trade_date": DAY,
        "unit_nav": nav,
        "accum_nav": pd.to_numeric(row.get("LJJZ"), errors="coerce"),
        "daily_return_pct": pd.to_numeric(row.get("JZZZL"), errors="coerce"),
        "subscription_status": row.get("SGZT"), "redemption_status": row.get("SHZT"),
    }])
    plan = {
        "account": ACCOUNT, "snapshot": str(snapshot), "version": before_obj["VersionId"],
        "fund": CODE, "date": str(DAY), "nav": nav, "rows_to_append": 1,
        "consumer_before_rows": len(before), "metadata": table.metadata_location,
    }
    print(json.dumps(plan, ensure_ascii=False), flush=True)
    if not args.apply:
        return
    require_versions(snapshot, before_obj["VersionId"],
                     args.expected_snapshot, args.expected_version)
    backup = f"{PREFIX}{snapshot}/"

    def receipt(key, data):
        s3.put_object(Bucket=BUCKET, Key=backup + key,
                      Body=json.dumps(data, default=str, ensure_ascii=False).encode(),
                      ContentType="application/json")

    receipt("plan.json", plan)
    receipt("upstream-before.json", payload)
    s3.put_object(Bucket=BUCKET, Key=backup + "consumer-before.parquet",
                  Body=before_raw, ContentType="application/parquet")
    meta_bucket, meta_key = table.metadata_location.removeprefix("s3://").split("/", 1)
    s3.copy_object(Bucket=BUCKET, Key=backup + "iceberg-metadata-before.json",
                   CopySource={"Bucket": meta_bucket, "Key": meta_key})
    table.refresh()
    require_versions(table.current_snapshot().snapshot_id,
                     s3.head_object(Bucket=BUCKET, Key=KEY)["VersionId"],
                     args.expected_snapshot, args.expected_version)
    if sfn.list_executions(stateMachineArn=WORKFLOW, statusFilter="RUNNING")["executions"]:
        raise RuntimeError("Collection started during the plan; stop before append")
    writer = IcebergWriter(catalog=catalog, database="fund_data_lake",
                           warehouse=env["WAREHOUSE_PATH"], subprocess_mode=False)
    result = writer.write("fund_daily", frame, fetch_date=DAY)
    if result.get("error") or result.get("rows_appended") != 1:
        raise RuntimeError(f"Single-row append did not complete: {result}")
    table.refresh()
    repaired_snapshot = table.current_snapshot().snapshot_id
    receipt("append-verified.json", {"snapshot": str(repaired_snapshot), "result": result})
    print("Single approved row committed; exporting November", flush=True)
    invocation = session.client(
        "lambda", config=Config(read_timeout=660, retries={"max_attempts": 0}),
    ).invoke(FunctionName=EXPORT, InvocationType="RequestResponse",
             Payload=json.dumps({"month": "2026-11"}).encode())
    exported = json.loads(invocation["Payload"].read())
    if invocation.get("FunctionError") or not exported.get("success"):
        raise RuntimeError(f"Export failed after append; do not append again: {exported}")
    after_obj, _, after = read_export(s3)
    verify_export(before, after, nav)
    table.refresh()
    if table.current_snapshot().snapshot_id != repaired_snapshot:
        raise RuntimeError("Concurrent lake change after append; inspect acceptance separately")
    receipt("consumer-verified.json", {
        "snapshot": str(repaired_snapshot), "version": after_obj["VersionId"],
        "before_rows": len(before), "after_rows": len(after),
        "all_original_keys_and_values_preserved": True, "added": f"{CODE}@{DAY}",
        "nav": nav, "export": exported, "replication": after_obj.get("ReplicationStatus"),
    })
    print(json.dumps({
        "accepted": True, "snapshot": str(repaired_snapshot),
        "version": after_obj["VersionId"], "before_rows": len(before),
        "after_rows": len(after), "backup": backup,
        "replication": after_obj.get("ReplicationStatus"),
    }), flush=True)


if __name__ == "__main__":
    main()
