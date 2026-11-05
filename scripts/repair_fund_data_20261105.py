"""Back up the November 4 NAV gap, invoke the fixed fallback, and re-export.

Read-only by default. --apply requires the inspected fund_daily snapshot.
Only existing Lambdas are invoked; no workflow, IAM, schedules, deletion,
exemption upkeep or Slack notification is involved.
"""
from __future__ import annotations

import argparse
import io
import json
from datetime import date, datetime, timezone

import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

ACCOUNT = "463470973226"
REGION = "us-east-1"
BUCKET = f"fund-data-pipeline-{ACCOUNT}-{REGION}"
FALLBACK = "FundDataFetchStack-FundFallbackLambda82226D2E-dt1F6pJ1tsdM"
EXPORT = "FundDataFetchStack-ExportFundHistoryLambda4F255537-P4Y7z0yGykTd"
START = date(2026, 10, 21)
END = date(2026, 11, 4)
AUDIT = "fund-data-pipeline/_repairs/2026-11-05-fallback-timeout/"


def _json(value):
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True).encode()


def _parquet(table):
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def invoke(client, function, payload):
    response = client.invoke(
        FunctionName=function, InvocationType="RequestResponse", Payload=_json(payload),
    )
    output = json.loads(response["Payload"].read())
    if response.get("FunctionError"):
        raise RuntimeError(f"{function}: {output}")
    print(json.dumps({"function": function, "result": output},
                     ensure_ascii=False, default=str), flush=True)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-daily-snapshot", type=int)
    parser.add_argument("--expected-fallback-image",
                        help="Pinned image URI of the tested/deployed checkpointing repair")
    args = parser.parse_args()
    if args.apply and (args.expected_daily_snapshot is None or not args.expected_fallback_image):
        parser.error("--apply requires --expected-daily-snapshot and --expected-fallback-image")
    session = boto3.Session(region_name=REGION)
    if session.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise RuntimeError("Wrong account")
    frozen = session.get_credentials().get_frozen_credentials()
    catalog = load_catalog("repair", type="glue", **{
        "glue.region": REGION, "s3.region": REGION,
        "s3.access-key-id": frozen.access_key, "s3.secret-access-key": frozen.secret_key,
        "s3.session-token": frozen.token,
    })
    table = catalog.load_table(("fund_data_lake", "fund_daily"))
    snapshot = table.current_snapshot().snapshot_id
    if args.apply and snapshot != args.expected_daily_snapshot:
        raise RuntimeError("Daily snapshot changed; inspect again before applying")
    s3 = session.client("s3")
    functions = session.client(
        "lambda", config=Config(read_timeout=960, connect_timeout=10,
                                retries={"max_attempts": 0}),
    )
    exports = {}
    for month in ("2026-10", "2026-11"):
        key = f"fund-data-pipeline/fund_history/trade_month={month}/part-0.parquet"
        head = s3.head_object(Bucket=BUCKET, Key=key)
        exports[month] = {"key": key, "version": head["VersionId"], "etag": head["ETag"]}
    plan = {
        "account": ACCOUNT, "region": REGION, "daily_snapshot_before": snapshot,
        "window": [str(START), str(END)], "exports_before": exports,
        "fallback_image": functions.get_function(FunctionName=FALLBACK)["Code"]["ResolvedImageUri"],
        "fallback_payload": {"trade_date": str(END), "lookback_days": 14},
    }
    print(json.dumps(plan, ensure_ascii=False, default=str), flush=True)
    if not args.apply:
        return
    if plan["fallback_image"] != args.expected_fallback_image:
        raise RuntimeError("Fallback image changed; inspect the deployment before applying")
    # The old deployed fallback has no checkpointing. Never invoke it as a repair.
    if plan["fallback_image"].endswith(
        "sha256:af2fcf663aab4c3fe8e300d3141824cd9267100f92c8207dea5300294481164b"
    ):
        raise RuntimeError("Old timeout-prone fallback is still deployed")
    before = table.scan(row_filter=And(
        GreaterThanOrEqual("trade_date", START), LessThanOrEqual("trade_date", END),
    )).to_arrow()
    audit = AUDIT + str(snapshot) + "/"
    s3.put_object(Bucket=BUCKET, Key=audit + "daily-before.parquet",
                  Body=_parquet(before), ContentType="application/x-parquet", IfNoneMatch="*")
    s3.put_object(Bucket=BUCKET, Key=audit + "plan.json",
                  Body=_json(plan), ContentType="application/json", IfNoneMatch="*")
    for month, original in exports.items():
        s3.copy_object(
            Bucket=BUCKET, Key=audit + f"export-{month}-before.parquet",
            CopySource={"Bucket": BUCKET, "Key": original["key"],
                        "VersionId": original["version"]},
            CopySourceIfMatch=original["etag"],
        )
    # Refuse to race the normal collector between inspection and backup.
    current = catalog.load_table(("fund_data_lake", "fund_daily")).current_snapshot().snapshot_id
    if current != snapshot:
        raise RuntimeError("Daily snapshot changed while backing up")
    result = invoke(functions, FALLBACK, plan["fallback_payload"])
    s3.put_object(Bucket=BUCKET, Key=audit + "fallback-result.json",
                  Body=_json(result), ContentType="application/json", IfNoneMatch="*")
    if not result.get("success") or not result.get("complete"):
        raise RuntimeError("Repair incomplete; committed batches are preserved; inspect result")
    export_results = {
        month: invoke(functions, EXPORT, {"month": month})
        for month in ("2026-10", "2026-11")
    }
    if any(not r.get("success") or not r.get("rows") for r in export_results.values()):
        raise RuntimeError("Monthly export failed or unexpectedly empty; inspect results")
    completed = {
        **plan, "completed_at": datetime.now(timezone.utc).isoformat(),
        "fallback": result, "exports": export_results,
        "daily_snapshot_after": catalog.load_table(
            ("fund_data_lake", "fund_daily"),
        ).current_snapshot().snapshot_id,
    }
    s3.put_object(Bucket=BUCKET, Key=audit + "completed.json",
                  Body=_json(completed), ContentType="application/json", IfNoneMatch="*")
    print(json.dumps({"completed": True, "audit": audit, **completed},
                     ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
