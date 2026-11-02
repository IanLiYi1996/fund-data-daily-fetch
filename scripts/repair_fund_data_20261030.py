"""Scoped repair for the 2026-10-30 HK-rank / 029238 incident.

Default is read-only. --apply requires the HK snapshot printed by the
dry run. Does not fabricate historical rankings: the endpoint ignores
date1/date2 for NAV and standard returns, so 10-27..29 remain unavailable.
"""
from __future__ import annotations

import argparse
import importlib.util
import io
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, EqualTo, In

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda"))

from shared.fetchers.fund_fetcher import FundFetcher
from shared.storage.iceberg_writer import IcebergWriter
from shared.storage.s3_client import S3Client

ACCOUNT = "463470973226"
REGION = "us-east-1"
BUCKET = f"fund-data-pipeline-{ACCOUNT}-{REGION}"
PREFIX = "fund-data-pipeline/"
REPAIR_DATE = date(2026, 10, 30)
BAD_DATES = [date(2026, 10, n) for n in (27, 28, 29)]
BAD_CODES = ["人民币", "美元", "港元", "澳大利亚元"]
BACKUP_PREFIX = f"{PREFIX}_repairs/2026-10-30-hk-rank-and-029238/"


def _parquet(arrow) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(arrow, buffer)
    return buffer.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-hk-snapshot", type=int)
    args = parser.parse_args()
    if args.apply and args.expected_hk_snapshot is None:
        parser.error("--apply requires --expected-hk-snapshot from the dry run")
    # A live list can only truthfully populate TODAY's ranking snapshot.
    if date.today() != REPAIR_DATE:
        raise RuntimeError("This repair must not stamp another day's live ranking as 2026-10-30")
    session = boto3.Session(region_name=REGION)
    if session.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise RuntimeError("Wrong AWS account; refusing repair")
    credentials = session.get_credentials().get_frozen_credentials()
    catalog = load_catalog("repair", type="glue", **{
        "glue.region": REGION, "s3.region": REGION,
        "s3.access-key-id": credentials.access_key,
        "s3.secret-access-key": credentials.secret_key,
        "s3.session-token": credentials.token,
    })
    hk = catalog.load_table(("fund_data_lake", "fund_hk_rank"))
    before = hk.scan().to_arrow()
    bad_filter = And(In("fund_code", BAD_CODES), In("snapshot_date", BAD_DATES))
    bad = hk.scan(row_filter=bad_filter).to_arrow()
    invalid = [row for row in before.to_pylist() if not str(row["fund_code"]).isdigit()]
    if bad.num_rows not in (0, 12) or len(invalid) != bad.num_rows:
        raise RuntimeError("Unexpected contamination scope; refusing deletion")
    hk_snapshot = hk.current_snapshot().snapshot_id
    if args.apply and hk_snapshot != args.expected_hk_snapshot:
        raise RuntimeError("HK snapshot changed since dry run; inspect again")
    result = FundFetcher().fetch_one("fund_hk_rank")
    if not result.success or result.row_count != 191:
        raise RuntimeError(f"Unexpected live HK response: {result.error}, rows={result.row_count}")

    warehouse = f"s3://{BUCKET}/{PREFIX}iceberg/"
    os.environ["WAREHOUSE_PATH"] = warehouse
    path = Path(__file__).resolve().parents[1] / "lambda/fund-fallback-fetcher/handler.py"
    spec = importlib.util.spec_from_file_location("incident_fallback", path)
    fallback = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = fallback
    spec.loader.exec_module(fallback)
    code, fetched, error = fallback._fetch_one(
        ("029238", "华西长盈63个月封闭债券"), "2026-10-15", "2026-10-29",
    )
    if error or fetched is None:
        raise RuntimeError(f"029238 source failed: {error}")
    nav = fetched[fetched.trade_date == date(2026, 10, 28)]
    if len(nav) != 1 or nav.iloc[0].unit_nav != 1.0:
        raise RuntimeError("029238@2026-10-28 no longer matches inspected upstream value")
    daily = catalog.load_table(("fund_data_lake", "fund_daily"))
    nav_filter = And(EqualTo("fund_code", code), EqualTo("trade_date", date(2026, 10, 28)))
    existing = daily.scan(row_filter=nav_filter).to_arrow()
    values = [v for v in existing["unit_nav"].to_pylist() if v is not None]
    if values and any(v != 1.0 for v in values):
        raise RuntimeError("Unexpected existing NAV; refusing to supersede uninspected data")
    plan = {
        "account": ACCOUNT, "region": REGION, "apply": args.apply,
        "hk_snapshot_before": hk_snapshot,
        "daily_snapshot_before": daily.current_snapshot().snapshot_id,
        "remove_hk_rows": bad.num_rows,
        "unavailable_hk_dates": [d.isoformat() for d in BAD_DATES],
        "live_hk_date": REPAIR_DATE.isoformat(), "live_hk_rows": result.row_count,
        "append_nav_rows": 0 if values else 1,
        "nav": "029238@2026-10-28=1.0000",
    }
    print(json.dumps(plan, ensure_ascii=False), flush=True)
    if not args.apply:
        return

    s3 = session.client("s3")
    backup_prefix = BACKUP_PREFIX + str(hk_snapshot) + "/"
    for name, body, content_type in (
        ("hk-before.parquet", _parquet(before), "application/x-parquet"),
        ("hk-contaminated.parquet", _parquet(bad), "application/x-parquet"),
        ("plan.json", json.dumps(plan, ensure_ascii=False).encode(), "application/json"),
    ):
        s3.put_object(Bucket=BUCKET, Key=backup_prefix + name, Body=body,
                      ContentType=content_type)
    print("BACKUP", f"s3://{BUCKET}/{backup_prefix}", flush=True)

    writer = IcebergWriter(catalog=catalog, warehouse=warehouse, subprocess_mode=False)
    # Land the usable current snapshot before removing old invalid keys.
    raw_client = S3Client(BUCKET, key_prefix=PREFIX)
    raw = raw_client.upload_dataframe(
        result.data, "fund", "fund_hk_rank",
        date=datetime(2026, 10, 30),
    )
    hk_write = writer.write("fund_hk_rank", result.data, fetch_date=REPAIR_DATE)
    if hk_write.get("error"):
        raise RuntimeError(hk_write["error"])
    hk = catalog.load_table(("fund_data_lake", "fund_hk_rank"))
    if bad.num_rows:
        # Iceberg creates a new snapshot; no old metadata or data file is
        # purged. The parquet backup additionally preserves all prior rows.
        hk.delete(bad_filter, snapshot_properties={
            "repair": "2026-10-30-hk-positional-field-mapping",
        })
    nav_write = {"skipped": True}
    if not values:
        nav_write = writer.write("fund_daily", nav, fetch_date=date(2026, 10, 28))
        if nav_write.get("rows_appended") != 1:
            raise RuntimeError(f"Unexpected NAV write: {nav_write}")

    hk = catalog.load_table(("fund_data_lake", "fund_hk_rank"))
    after = hk.scan().to_arrow()
    assert all(str(row["fund_code"]).isdigit() for row in after.to_pylist())
    old_valid = before.filter(pc.and_(
        pc.invert(pc.is_in(before["fund_code"], value_set=pa.array(BAD_CODES))),
        pc.not_equal(before["snapshot_date"], pa.scalar(REPAIR_DATE)),
    )).to_pylist()
    after_old = hk.scan(row_filter=In("snapshot_date", sorted(
        {row["snapshot_date"] for row in old_valid}
    ))).to_arrow().to_pylist()
    key = lambda row: (row["snapshot_date"], row["fund_code"])
    assert sorted(old_valid, key=key) == sorted(after_old, key=key)
    today = hk.scan(row_filter=EqualTo("snapshot_date", REPAIR_DATE)).to_arrow()
    assert today.num_rows == 191 and len(set(today["fund_code"].to_pylist())) == 191
    nav_check = catalog.load_table(("fund_data_lake", "fund_daily")).scan(
        row_filter=nav_filter,
    ).to_arrow()
    assert 1.0 in nav_check["unit_nav"].to_pylist()
    completed = {
        **plan, "completed_at": datetime.now(timezone.utc).isoformat(),
        "raw": raw, "hk_write": hk_write, "nav_write": nav_write,
        "hk_snapshot_after": hk.current_snapshot().snapshot_id,
        "old_valid_rows_preserved": len(old_valid),
        "backup_prefix": f"s3://{BUCKET}/{backup_prefix}",
    }
    s3.put_object(Bucket=BUCKET, Key=backup_prefix + "completed.json",
                  Body=json.dumps(completed, ensure_ascii=False).encode(),
                  ContentType="application/json")
    print(json.dumps(completed, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
