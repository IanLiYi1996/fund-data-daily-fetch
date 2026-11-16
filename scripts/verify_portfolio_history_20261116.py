"""Read-only acceptance of the repaired portfolio consumer history."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda"))
from shared.quality.portfolio_hold import fetch_holdings  # noqa: E402

BUCKET = "fund-data-pipeline-463470973226-us-east-1"
KEY = "fund-data-pipeline/fund/_history/fund_portfolio_hold_history.parquet"
BACKUP = (
    "fund-data-pipeline/_repairs/2026-11-16-portfolio-history/"
    "3722774192629459938/consumer-before.parquet"
)
KEYS = ["fund_code", "report_date", "holding_code"]
VALUES = ["holding_name", "weight_pct", "shares", "market_value"]
TARGET = date(2026, 9, 30)


def read_reference(code):
    for attempt in range(3):
        frame = fetch_holdings(code, 2026, target_quarter=TARGET)
        if not frame.empty or attempt == 2:
            return frame
        time.sleep(5 * (attempt + 1))


def verify_frames(before, after):
    if after.duplicated(KEYS).any():
        raise AssertionError("consumer has duplicate holdings")
    if after.report_date.max() != TARGET or (after.report_date > date.today()).any():
        raise AssertionError("consumer latest report is not the completed Q3")
    joined = before.merge(after, on=KEYS, how="left", suffixes=("_old", "_new"), indicator=True)
    if not joined["_merge"].eq("both").all():
        raise AssertionError("previously published historical holdings lost")
    for name in VALUES:
        old, new = joined[name + "_old"], joined[name + "_new"]
        if not (old.eq(new) | (old.isna() & new.isna())).all():
            raise AssertionError(f"previously published history changed: {name}")
    counts = after.groupby("report_date").fund_code.nunique()
    previous = max(d for d in counts.index if d < TARGET)
    if counts[TARGET] < .9 * counts[previous]:
        raise AssertionError("Q3 fund coverage is incomplete")
    return {
        "before_rows": len(before), "before_funds": int(before.fund_code.nunique()),
        "after_rows": len(after), "after_funds": int(after.fund_code.nunique()),
        "historical_rows_preserved": len(joined),
        "latest_report_date": TARGET.isoformat(),
        "quarter_funds": {str(k): int(v) for k, v in counts.items()},
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=50)
    args = parser.parse_args()
    session = boto3.Session(region_name="us-east-1")
    if session.client("sts").get_caller_identity()["Account"] != "463470973226":
        raise RuntimeError("wrong account")
    s3 = session.client("s3")
    head = s3.head_object(Bucket=BUCKET, Key=KEY)
    if head.get("ReplicationStatus") != "COMPLETED":
        raise AssertionError(f"replication not completed: {head.get('ReplicationStatus')}")
    old_bytes = s3.get_object(Bucket=BUCKET, Key=BACKUP)["Body"].read()
    new_bytes = s3.get_object(
        Bucket=BUCKET, Key=KEY, VersionId=head["VersionId"],
    )["Body"].read()
    before = pq.ParquetFile(io.BytesIO(old_bytes)).read(use_threads=False).to_pandas()
    after = pq.ParquetFile(io.BytesIO(new_bytes)).read(use_threads=False).to_pandas()
    result = verify_frames(before, after)
    target = after[after.report_date.eq(TARGET)]
    codes = sorted(
        target.fund_code.unique(),
        key=lambda code: hashlib.sha256(("portfolio-20261116:" + code).encode()).digest(),
    )[:args.sample]

    def compare(code):
        upstream = read_reference(code)
        upstream = upstream[upstream.report_date.eq(TARGET)]
        exported = target[target.fund_code.eq(code)]
        if set(upstream.holding_code) != set(exported.holding_code):
            raise AssertionError(f"upstream stock keys differ: {code}")
        joined = exported.merge(upstream, on=KEYS, suffixes=("_file", "_source"))
        for name in VALUES:
            for left, right in zip(joined[name + "_file"], joined[name + "_source"]):
                if pd.isna(left) and pd.isna(right):
                    continue
                if name == "holding_name":
                    equal = left == right
                else:
                    equal = math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-6)
                if not equal:
                    raise AssertionError(f"upstream value differs: {code}/{name}")
        return len(joined)

    with ThreadPoolExecutor(max_workers=4) as pool:
        sample_rows = sum(pool.map(compare, codes))
    result.update({
        "sample_funds_matched": len(codes), "sample_rows_matched": sample_rows,
        "consumer_version": head["VersionId"], "replication": head["ReplicationStatus"],
        "bytes": len(new_bytes), "sha256": hashlib.sha256(new_bytes).hexdigest(),
        "old_sha256": hashlib.sha256(old_bytes).hexdigest(),
    })
    current = s3.head_object(Bucket=BUCKET, Key=KEY)
    if current["VersionId"] != head["VersionId"]:
        raise AssertionError("consumer file changed during verification")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
