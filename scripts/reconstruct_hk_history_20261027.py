"""Publish an isolated, labelled reconstruction for HK NAV dates 2026-10-27..29.

Default is read-only. --apply needs the snapshot and plan hash from the dry run.
Only new reconstruction/audit S3 objects are created; no Glue/Iceberg, original
raw ranking, monthly NAV export, Lambda, IAM or notification writes are made.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda"))
from shared.quality.hk_history import (
    RETURN_BASIS, build_valuations, fetch_history, history_start,
    normalize_members, replay_history, seed_frozen_anchor, validate_controls,
)

ACCOUNT = "463470973226"
REGION = "us-east-1"
BUCKET = f"fund-data-pipeline-{ACCOUNT}-{REGION}"
# A separate namespace INSIDE the existing fund/ replication rule. Never a
# YYYY-MM-DD raw folder, _history/, or an original ranking/export key.
OUTPUT_PREFIX = "fund-data-pipeline/fund/_reconstructed/fund_hk_history/"
AUDIT_PREFIX = "fund-data-pipeline/_repairs/2026-11-02-hk-history-reconstructed/"
ANCHOR_DATE = date(2026, 10, 26)
ANCHOR_KEY = f"fund-data-pipeline/fund/{ANCHOR_DATE}/fund_hk_rank.parquet"
CORE_EXPORT_KEY = "fund-data-pipeline/fund_history/trade_month=2026-10/part-0.parquet"
TARGETS = [date(2026, 10, n) for n in (27, 28, 29)]
EXPECTED_MEMBERS = 191

PANEL_SCHEMA = pa.schema([
    pa.field("fund_code", pa.string(), nullable=False),
    pa.field("hk_fund_code", pa.string(), nullable=False),
    pa.field("fund_name", pa.string(), nullable=False),
    pa.field("as_of_date", pa.date32(), nullable=False),
    pa.field("currency", pa.string()),
    pa.field("nav_date", pa.date32()),
    pa.field("unit_nav", pa.float64()),
    pa.field("daily_return_pct", pa.float64()),
    pa.field("nav_age_days", pa.int32()),
    pa.field("nav_status", pa.string(), nullable=False),
    pa.field("nav_source", pa.string(), nullable=False),
    pa.field("data_origin", pa.string(), nullable=False),
    pa.field("as_of_basis", pa.string(), nullable=False),
    pa.field("observation_time_recovered", pa.bool_(), nullable=False),
    pa.field("return_basis", pa.string(), nullable=False),
    *[
        field
        for period in ("7d", "1m", "1y")
        for field in (
            pa.field(f"baseline_{period}_date", pa.date32()),
            pa.field(f"baseline_{period}_nav", pa.float64()),
            pa.field(f"nav_change_{period}_pct", pa.float64()),
        )
    ],
], metadata={
    b"data_origin": b"reconstructed",
    b"original_snapshot_recovered": b"false",
    b"as_of_basis": b"nav_date_cutoff_not_publication_time",
    b"return_basis": RETURN_BASIS.encode(),
})


def _json(value) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, default=str,
        allow_nan=False, separators=(",", ":"),
    ).encode()


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _parquet(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def panel_parquet(rows: list[dict]) -> bytes:
    return _parquet(pa.Table.from_pylist(rows, schema=PANEL_SCHEMA))


def assert_replication_coverage(configuration: dict) -> None:
    """The opt-in consumer dataset must use an existing, untagged rule."""
    for rule in configuration.get("ReplicationConfiguration", {}).get("Rules", []):
        filter_ = rule.get("Filter", {"Prefix": rule.get("Prefix", "")})
        if (rule.get("Status") == "Enabled"
                and set(filter_) <= {"Prefix"}
                and OUTPUT_PREFIX.startswith(filter_.get("Prefix", ""))
                and rule.get("Destination", {}).get("Bucket")
                == "arn:aws:s3:::financial-dataset-mx"):
            return
    raise ValueError("Consumer prefix is not covered by existing replication")


def _get_optional(s3, key: str) -> bytes | None:
    try:
        return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        raise


def publish_bundle(s3, objects: dict[str, bytes], *, before_manifest=None) -> dict:
    """Preflight conflicts; create immutably; publish the commit marker last."""
    if any(not key.startswith((OUTPUT_PREFIX, AUDIT_PREFIX)) for key in objects):
        raise ValueError("Write outside reconstruction scope")
    existing = {}
    for key, body in objects.items():
        prior = _get_optional(s3, key)
        if prior is not None and prior != body:
            raise ValueError(f"Reconstruction conflict: refusing to overwrite {key}")
        existing[key] = prior is not None
    manifest_key = OUTPUT_PREFIX + "manifest.json"
    keys = sorted(objects, key=lambda k: k == manifest_key)
    created, skipped = [], []
    for key in keys:
        if key == manifest_key and before_manifest is not None:
            before_manifest()
        if existing[key]:
            skipped.append(key)
            continue
        body = objects[key]
        content_type = ("application/x-parquet" if key.endswith(".parquet")
                        else "application/gzip" if key.endswith(".gz")
                        else "application/json")
        try:
            s3.put_object(
                Bucket=BUCKET, Key=key, Body=body, ContentType=content_type,
                IfNoneMatch="*", Metadata={
                    "sha256": _sha(body),
                    "data_origin": ("reconstructed" if key.startswith(OUTPUT_PREFIX)
                                    else "reconstruction_audit"),
                    "published_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        except ClientError as exc:
            # A concurrent creator is acceptable only if the content agrees.
            if (exc.response.get("Error", {}).get("Code") != "PreconditionFailed"
                    or _get_optional(s3, key) != body):
                raise
        if _get_optional(s3, key) != body:
            raise ValueError(f"Published content failed read-back verification: {key}")
        created.append(key)
    return {"created": created, "existing": skipped}


def _fetch_all(members: list[dict]) -> tuple[dict, dict]:
    def one(member):
        for attempt in range(3):
            try:
                return fetch_history(member, history_start(member, min(TARGETS)), max(TARGETS))
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(attempt + 1)

    histories, evidence, errors = {}, {}, []
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending = {pool.submit(one, m): m["fund_code"] for m in members}
        for i, future in enumerate(as_completed(pending), 1):
            code = pending[future]
            try:
                histories[code], evidence[code] = future.result()
            except Exception as exc:
                errors.append({"fund_code": code, "error": str(exc)})
            if i % 25 == 0 or i == len(members):
                print(f"Historical fetch {i}/{len(members)}; errors={len(errors)}", flush=True)
    if errors:
        raise ValueError(f"No publish on incomplete/failed source reads: {_json(errors).decode()}")
    return histories, evidence


def _protected_state(catalog, s3) -> tuple[dict, bytes]:
    hk = catalog.load_table(("fund_data_lake", "fund_hk_rank"))
    daily = catalog.load_table(("fund_data_lake", "fund_daily"))
    arrow = hk.scan().to_arrow().sort_by([
        ("snapshot_date", "ascending"), ("fund_code", "ascending"),
    ])
    original = _parquet(arrow)
    counts = Counter(arrow["snapshot_date"].to_pylist())
    if any(counts[target] for target in TARGETS):
        raise ValueError("Original target dates changed; inspect rather than override")
    core = s3.head_object(Bucket=BUCKET, Key=CORE_EXPORT_KEY)
    anchor = s3.head_object(Bucket=BUCKET, Key=ANCHOR_KEY)
    return {
        "hk_snapshot": hk.current_snapshot().snapshot_id,
        "hk_rows": arrow.num_rows, "hk_sha256": _sha(original),
        "original_target_rows": {str(d): counts[d] for d in TARGETS},
        "daily_snapshot": daily.current_snapshot().snapshot_id,
        "core_export_version": core["VersionId"],
        "anchor_version": anchor["VersionId"],
    }, original


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-hk-snapshot", type=int)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--source-plan-sha256",
                        help="Reuse and revalidate immutable audited source responses")
    args = parser.parse_args()
    if args.apply and (args.expected_hk_snapshot is None or not args.expected_plan_sha256):
        parser.error("--apply requires --expected-hk-snapshot and --expected-plan-sha256")
    session = boto3.Session(region_name=REGION)
    if session.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise ValueError("Wrong AWS account")
    credentials = session.get_credentials().get_frozen_credentials()
    catalog = load_catalog("reconstruction", type="glue", **{
        "glue.region": REGION, "s3.region": REGION,
        "s3.access-key-id": credentials.access_key,
        "s3.secret-access-key": credentials.secret_key,
        "s3.session-token": credentials.token,
    })
    s3 = session.client("s3")
    assert_replication_coverage(s3.get_bucket_replication(Bucket=BUCKET))
    protected, original_hk = _protected_state(catalog, s3)
    if args.apply and protected["hk_snapshot"] != args.expected_hk_snapshot:
        raise ValueError("Original HK snapshot changed since the dry run")
    anchor_body = s3.get_object(
        Bucket=BUCKET, Key=ANCHOR_KEY, VersionId=protected["anchor_version"],
    )["Body"].read()
    members = normalize_members(
        pq.ParquetFile(io.BytesIO(anchor_body)).read(use_threads=False).to_pylist(),
    )
    if len(members) != EXPECTED_MEMBERS:
        raise ValueError(f"Unexpected historical membership: {len(members)}")
    if args.source_plan_sha256:
        if not re.fullmatch(r"[0-9a-f]{64}", args.source_plan_sha256):
            raise ValueError("Invalid source plan SHA-256")
        source_audit = AUDIT_PREFIX + args.source_plan_sha256 + "/"
        saved_plan = json.loads(s3.get_object(
            Bucket=BUCKET, Key=source_audit + "plan.json",
        )["Body"].read())
        compressed = s3.get_object(
            Bucket=BUCKET, Key=source_audit + "source-responses.json.gz",
        )["Body"].read()
        if (_sha(_json(saved_plan)) != args.source_plan_sha256
                or _sha(compressed) != saved_plan["source"]["responses_sha256"]
                or saved_plan["protected_originals"] != protected
                or saved_plan["membership"]["sha256"] != _sha(anchor_body)):
            raise ValueError("Audited source plan, hash or protected originals changed")
        evidence = json.loads(gzip.decompress(compressed))
        if set(evidence) != {m["fund_code"] for m in members}:
            raise ValueError("Audited source membership mismatch")
        histories = {
            m["fund_code"]: replay_history(
                m, history_start(m, min(TARGETS)), max(TARGETS), evidence[m["fund_code"]],
            ) for m in members
        }
        print("Revalidated immutable audited source responses for 191 funds", flush=True)
    else:
        histories, evidence = _fetch_all(members)
    source_count = sum(len(h) for h in histories.values())
    histories, frozen = seed_frozen_anchor(members, histories, ANCHOR_DATE)
    controls = validate_controls(members, histories)
    rows = build_valuations(members, histories, TARGETS)
    source_body = gzip.compress(_json(evidence), mtime=0)
    panels, coverage = {}, []
    for target in TARGETS:
        day_rows = [r for r in rows if r["as_of_date"] == target]
        if len(day_rows) != EXPECTED_MEMBERS:
            raise ValueError("Reconstruction membership incomplete")
        key = OUTPUT_PREFIX + f"as_of_date={target}/part-0.parquet"
        panels[key] = panel_parquet(day_rows)
        coverage.append({
            "as_of_date": str(target), "members": len(day_rows),
            "usable_navs": sum(r["unit_nav"] is not None for r in day_rows),
            "nav_status": dict(Counter(r["nav_status"] for r in day_rows)),
            "nav_sources": dict(Counter(r["nav_source"] for r in day_rows)),
            "no_reported_nav_codes": [r["fund_code"] for r in day_rows if r["unit_nav"] is None],
            "price_change_coverage": {
                period: sum(r[f"nav_change_{period}_pct"] is not None for r in day_rows)
                for period in ("7d", "1m", "1y")
            },
        })
    plan = {
        "dataset": "fund_hk_history_reconstructed", "format_version": 1,
        "account": ACCOUNT, "region": REGION,
        "original_snapshot_recovered": False,
        "as_of_basis": "nav_date_cutoff_not_publication_time",
        "return_basis": RETURN_BASIS,
        "source": {"api": "HKFDApi", "method": "MethodJZ", "action": "2",
                   "source_records": source_count, "responses_sha256": _sha(source_body)},
        "membership": {"key": ANCHOR_KEY, "version": protected["anchor_version"],
                       "sha256": _sha(anchor_body), "members": len(members)},
        "validated_control_navs": controls,
        "source_history_control_navs": controls - len(frozen),
        "frozen_raw_anchor_only_codes": frozen,
        "protected_originals": protected,
        "coverage": coverage,
        "files": [{"key": k, "sha256": _sha(v), "bytes": len(v)} for k, v in panels.items()],
        "limitations": [
            "Not an original observed ranking: historical publication time is unknown.",
            "Price changes are not dividend-reinvested ranking returns.",
            "Null NAV/return is retained when upstream has no qualifying record/baseline.",
            "Known stale NAV dates remain stale; none is stamped as a new observation.",
            "Reads a bounded baseline window, not the entire inception-to-date history.",
        ],
    }
    plan_hash = _sha(_json(plan))
    print(_json({"mode": "apply" if args.apply else "read-only",
                 "plan_sha256": plan_hash, **plan}).decode(), flush=True)
    if not args.apply:
        return
    if plan_hash != args.expected_plan_sha256:
        raise ValueError("Reconstruction source/plan changed since the dry run")

    def assert_protected():
        current, _ = _protected_state(catalog, s3)
        if current != protected:
            raise ValueError("Original data changed; refusing to publish the manifest")

    assert_protected()
    audit = AUDIT_PREFIX + plan_hash + "/"
    manifest = {**plan, "plan_sha256": plan_hash, "audit_prefix": audit}
    objects = {
        audit + "hk-original-before.parquet": original_hk,
        audit + "membership-original.parquet": anchor_body,
        audit + "source-responses.json.gz": source_body,
        audit + "plan.json": _json(plan),
        **panels,
        OUTPUT_PREFIX + "manifest.json": _json(manifest),
    }
    result = publish_bundle(s3, objects, before_manifest=assert_protected)
    for key in panels:
        table = pq.ParquetFile(io.BytesIO(
            s3.get_object(Bucket=BUCKET, Key=key)["Body"].read(),
        )).read(use_threads=False)
        if (table.num_rows != EXPECTED_MEMBERS or table.schema != PANEL_SCHEMA
                or set(table["fund_code"].to_pylist()) != {m["fund_code"] for m in members}):
            raise ValueError(f"Consumer parquet validation failed: {key}")
    assert_protected()
    print(_json({"published": True, "plan_sha256": plan_hash,
                 "originals_unchanged": True, **result}).decode(), flush=True)


if __name__ == "__main__":
    main()
