"""Backfill / refresh fund_portfolio_hold Iceberg table via akshare.

akshare's fund_portfolio_hold_em(symbol, date=YEAR) returns per-fund
quarterly top-holdings for a given year. This script iterates every
fund_code in the pipeline universe, calls the endpoint for each requested
year (default: current year + last year), and appends normalized rows
to the Iceberg table fund_data_lake.fund_portfolio_hold.

Runtime characteristics (measured):
- Serial per-fund latency: 1-2s
- 4-worker parallel: ~30 funds/min
- Full universe (~19k equity-like funds) × 2 years: ~10-15h wall clock

Called via Fargate (task family FundHistoryBackfill) with:
    --mode portfolio [--years 2026,2025] [--limit N]

Resume skips only fund_codes already present for the target quarter.
Source errors fail the run without republishing a partial consumer file.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
# Same layout as backfill_fund_history: shared/ ships alongside in the image
_here = Path(__file__).resolve().parent
for _cand in (_here.parent, _here / "..", Path("lambda"), Path("/app")):
    _cand = _cand.resolve() if hasattr(_cand, "resolve") else _cand
    if (_cand / "shared").is_dir() and str(_cand) not in sys.path:
        sys.path.insert(0, str(_cand))
        break
from shared.storage.iceberg_writer import IcebergWriter  # noqa: E402
from shared.quality.portfolio_hold import fetch_holdings, quarter_end  # noqa: E402
from pyiceberg.catalog import load_catalog  # noqa: E402
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, In  # noqa: E402


DEFAULT_BUCKET = os.environ.get(
    "S3_BUCKET", "fund-data-pipeline-463470973226-us-east-1"
)
DEFAULT_S3_PREFIX = os.environ.get("S3_PREFIX", "fund-data-pipeline/")
DEFAULT_WAREHOUSE = os.environ.get(
    "WAREHOUSE_PATH", f"s3://{DEFAULT_BUCKET}/{DEFAULT_S3_PREFIX}iceberg/"
)
DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")
MIN_EXPORT_FUNDS = 14000

_QUARTER_RE = re.compile(r"^(\d{4})年(\d)季度")


def _parse_quarter(s: str):
    m = _QUARTER_RE.match(str(s))
    if not m:
        return None
    y, q = int(m.group(1)), int(m.group(2))
    return quarter_end(y, q)


def _retry_delay(error, attempt):
    """Bounded network retries; honor rate caps without changing identities."""
    if not isinstance(error, requests.RequestException):
        return None
    response = getattr(error, "response", None)
    capped = response is not None and response.status_code in (429, 514)
    if response is not None and response.status_code < 500 and not capped:
        return None
    delay = (30 if capped else 5) * 2 ** attempt
    if response is not None and response.headers.get("Retry-After"):
        value = response.headers["Retry-After"]
        try:
            seconds = float(value)
        except ValueError:
            try:
                seconds = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return None
        if not 0 <= seconds <= 120:
            # Fail closed instead of retrying earlier than a long source cap.
            return None
        delay = max(delay, seconds)
    return delay


def _fetch_one(code_name, years, target_quarter=None):
    code, name = code_name
    frames = []
    err = None
    for yr in years:
        for attempt in range(3):
            try:
                df = fetch_holdings(code, yr, target_quarter=target_quarter)
                for period, cutoff in df.attrs.get("observation_cutoffs", {}).items():
                    if period != cutoff:
                        print("SOURCE_CUTOFF " + json.dumps({
                            "fund_code": code, "report_period": period,
                            "observation_cutoff": cutoff,
                        }), flush=True)
                if target_quarter is not None:
                    df = df[df.report_date == target_quarter]
                if not df.empty:
                    frames.append(df)
                break
            except Exception as e:
                delay = _retry_delay(e, attempt)
                if attempt == 2 or delay is None:
                    err = f"{type(e).__name__}: {str(e)[:160]}"
                    break
                else:
                    time.sleep(delay)
    if not frames:
        return code, None, err or "no-data"
    return code, pd.concat(frames, ignore_index=True), err


def _load_failure_codes(uri: str) -> set[str]:
    """Read an audited failure map, never expand a retry into a universe scan."""
    import boto3
    from urllib.parse import urlparse

    source = urlparse(uri)
    if (
        source.scheme != "s3" or source.netloc != DEFAULT_BUCKET
        or not source.path.startswith(f"/{DEFAULT_S3_PREFIX}_repairs/")
        or source.query or source.fragment
    ):
        raise ValueError("failure plan must be in this pipeline's private repair prefix")
    body = boto3.client("s3", region_name=DEFAULT_REGION).get_object(
        Bucket=source.netloc, Key=source.path.lstrip("/"),
    )["Body"].read(1_000_001)
    if len(body) > 1_000_000:
        raise ValueError("failure plan too large")
    failures = json.loads(body)
    if not isinstance(failures, dict) or not 1 <= len(failures) <= 1000:
        raise ValueError("failure plan must contain 1..1000 failed codes")
    if any(not re.fullmatch(r"\d{6}", code) or not isinstance(error, str)
           or not error for code, error in failures.items()):
        raise ValueError("invalid failure plan code/error")
    return set(failures)


def _build_universe(warehouse: str) -> pd.DataFrame:
    """Load fund_name latest snapshot, filter to equity-like fund names.

    Money-market / pure-bond / REITs funds do not have "股票投资明细"
    disclosures; skipping them avoids ~40% wasted API calls.
    """
    cat = load_catalog("glue", **{
        "type": "glue", "glue.region": DEFAULT_REGION, "warehouse": warehouse,
    })
    tbl = cat.load_table(("fund_data_lake", "fund_name"))
    fn = tbl.scan(
        row_filter=GreaterThanOrEqual("snapshot_date", date.today() - timedelta(days=7)),
        selected_fields=("fund_code", "fund_name", "fund_type", "snapshot_date"),
    ).to_arrow().to_pandas()
    if fn.empty:
        raise RuntimeError("no recent fund universe; refusing stale full-history scan")
    latest = fn.snapshot_date.max()
    fn_latest = (fn[fn.snapshot_date == latest]
                 .drop_duplicates("fund_code")
                 .reset_index(drop=True))

    # Names like "增利" and "转债" do not establish that a fund has no stocks.
    # Only exclude unambiguously non-stock product types.
    equity = fn_latest[~fn_latest.fund_type.fillna("").str.contains(
        "货币|纯债|REIT", regex=True, case=False,
    )]
    return equity[["fund_code", "fund_name"]].reset_index(drop=True)


def _load_done_set(
    catalog, table_name: str, target_quarter: Optional[date] = None,
) -> set[str]:
    """Return the set of fund_codes already loaded for the target quarter.

    Historical quarters are stable disclosures — once a fund_code has
    landed a row for a given `report_date`, that pair is frozen. The
    only interesting resume-skip is per-quarter: codes that already have
    a row for the newest quarter don't need re-fetching, but codes only
    seen in older quarters DO (they may have disclosed the new quarter).

    When ``target_quarter`` is None (legacy behavior), skip any code
    with any row.
    """
    try:
        tbl = catalog.load_table(("fund_data_lake", table_name))
        if target_quarter is None:
            codes = tbl.scan(
                selected_fields=("fund_code",),
            ).to_arrow().to_pandas()
        else:
            codes = tbl.scan(
                row_filter=EqualTo("report_date", target_quarter),
                selected_fields=("fund_code",),
            ).to_arrow().to_pandas()
        return set(codes.fund_code.unique())
    except Exception as e:
        raise RuntimeError("cannot read portfolio resume set; refusing blind refresh") from e


def _refresh_existing(catalog, codes, years, target_quarter, *, workers=4):
    """Revalidate preloaded disclosures and atomically replace changed fund periods.

    Row-key upsert retains stocks removed from a revised disclosure. A verified
    disclosure replaces the entire fund/quarter, never older periods. Missing
    or failed source responses are not permission to delete existing holdings.
    """
    import io
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not codes:
        return {"validated_funds": 0, "changed_funds": 0}
    if target_quarter is None or target_quarter > date.today():
        raise ValueError("existing refresh requires a completed target quarter")
    table = catalog.load_table(("fund_data_lake", "fund_portfolio_hold"))
    snapshot = table.current_snapshot().snapshot_id
    scope = And(EqualTo("report_date", target_quarter), In("fund_code", sorted(codes)))
    before_arrow = table.scan(row_filter=scope).to_arrow()
    before = before_arrow.to_pandas()
    if set(before.fund_code) != set(codes):
        raise RuntimeError("existing-quarter refresh scope changed before source validation")

    def validate_existing(code):
        for attempt in range(3):
            result = _fetch_one((code, ""), years, target_quarter)
            # An already loaded disclosure is expected to exist. A transient
            # empty page must be retried, not mistaken for a deletion.
            if result[2] != "no-data" or attempt == 2:
                return result
            time.sleep(5 * (attempt + 1))

    frames, errors = {}, {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(validate_existing, code): code
            for code in sorted(codes)
        }
        for i, future in enumerate(as_completed(futures), 1):
            code, frame, error = future.result()
            if error or frame is None or frame.empty:
                errors[code] = error or "existing disclosure absent from source"
            else:
                frames[code] = frame
            if i % 100 == 0:
                print(f"EXISTING_VALIDATE {i}/{len(codes)} errors={len(errors)}", flush=True)
    if errors:
        print("SOURCE_ERRORS " + json.dumps(errors, ensure_ascii=False), flush=True)
        raise RuntimeError("existing-quarter source validation failed; no rows replaced")

    fields = list(before.columns)
    changed = []
    for code, source in frames.items():
        old = before[before.fund_code.eq(code)].sort_values("holding_code").reset_index(drop=True)
        new = source[fields].sort_values("holding_code").reset_index(drop=True)
        try:
            pd.testing.assert_frame_equal(
                old, new, check_dtype=False, check_exact=False, rtol=1e-12, atol=1e-6,
            )
        except AssertionError:
            changed.append(code)
    if not changed:
        print(f"EXISTING_VERIFIED {len(codes)} funds; no replacement needed", flush=True)
        return {"validated_funds": len(codes), "changed_funds": 0}
    merged = pd.concat([frames[code] for code in sorted(changed)], ignore_index=True)
    if not merged.report_date.eq(target_quarter).all():
        raise RuntimeError("existing replacement escaped its target quarter")
    if merged.duplicated(["fund_code", "report_date", "holding_code"]).any():
        raise RuntimeError("existing replacement has duplicate keys")
    replacement = pa.Table.from_pandas(
        merged[fields], preserve_index=False,
    ).cast(table.schema().as_arrow())
    table.refresh()
    if table.current_snapshot().snapshot_id != snapshot:
        raise RuntimeError("portfolio snapshot changed during existing source validation")
    backup = (
        f"{DEFAULT_S3_PREFIX}_repairs/portfolio-quarter-refresh/"
        f"{target_quarter}/{snapshot}/"
    )
    s3 = boto3.client("s3", region_name=DEFAULT_REGION)
    for suffix, data in (
        ("existing-quarter-before.parquet", before_arrow),
        ("validated-replacement.parquet", replacement),
    ):
        body = io.BytesIO()
        pq.write_table(data, body, compression="snappy")
        s3.put_object(Bucket=DEFAULT_BUCKET, Key=backup + suffix, Body=body.getvalue())
    receipt = {
        "before_snapshot": snapshot, "report_date": str(target_quarter),
        "validated_funds": len(codes), "changed_funds": len(changed),
        "changed_codes": sorted(changed), "backup_prefix": backup,
        "old_changed_rows": int(before.fund_code.isin(changed).sum()),
        "new_changed_rows": len(replacement),
    }
    s3.put_object(
        Bucket=DEFAULT_BUCKET, Key=backup + "replacement-plan.json",
        Body=json.dumps(receipt).encode(), ContentType="application/json",
    )
    table.refresh()
    if table.current_snapshot().snapshot_id != snapshot:
        raise RuntimeError("portfolio snapshot changed after replacement backup")
    count_before = int(table.current_snapshot().summary.model_dump()["total-records"])
    replace_scope = And(
        EqualTo("report_date", target_quarter), In("fund_code", sorted(changed)),
    )
    table.overwrite(replacement, overwrite_filter=replace_scope)
    table.refresh()
    count_after = int(table.current_snapshot().summary.model_dump()["total-records"])
    if count_after != count_before - receipt["old_changed_rows"] + len(replacement):
        raise RuntimeError("existing replacement affected an unexpected record count")
    actual = table.scan(row_filter=replace_scope).to_arrow().to_pandas()
    pd.testing.assert_frame_equal(
        actual.sort_values(["fund_code", "holding_code"]).reset_index(drop=True),
        merged[fields].sort_values(["fund_code", "holding_code"]).reset_index(drop=True),
        check_dtype=False, check_exact=False, rtol=1e-12, atol=1e-6,
    )
    receipt["after_snapshot"] = table.current_snapshot().snapshot_id
    s3.put_object(
        Bucket=DEFAULT_BUCKET, Key=backup + "replacement-verified.json",
        Body=json.dumps(receipt).encode(), ContentType="application/json",
    )
    print("EXISTING_REPLACED " + json.dumps(receipt), flush=True)
    return receipt


def _resolve_target_quarter(spec: Optional[str]) -> date:
    """Return the report_date (quarter-end) implied by ``spec`` or today.

    ``spec`` accepts ``YYYY-QN`` (e.g. ``2026-Q3``). Absent, we pick the
    most recent quarter whose disclosure window has opened — the
    quarterly EventBridge rule runs on the 5th of Feb/May/Aug/Nov, after
    the previous quarter's ordinary disclosure window.
    """
    if spec:
        m = re.match(r"^(\d{4})-?[Qq]([1-4])$", spec)
        if not m:
            raise ValueError(f"invalid --target-quarter: {spec}")
        y, q = int(m.group(1)), int(m.group(2))
        result = quarter_end(y, q)
        if result > date.today():
            raise ValueError("target quarter is in the future")
        return result

    today = date.today()
    year = today.year
    if today.month <= 3:
        return quarter_end(year - 1, 4)
    if today.month <= 6:
        return quarter_end(year, 1)
    if today.month <= 9:
        return quarter_end(year, 2)
    return quarter_end(year, 3)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    p.add_argument("--database", default="fund_data_lake")
    p.add_argument("--years", default=None,
                   help="Comma-separated years (default: current + last)")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--batch", type=int, default=100,
                   help="Flush to Iceberg every N completed funds")
    p.add_argument("--limit", type=int, default=0,
                   help="Cap total funds processed (0=all)")
    p.add_argument("--force", action="store_true",
                   help="Ignore resume set and re-fetch every fund")
    p.add_argument("--export-only", action="store_true")
    p.add_argument("--no-export", action="store_true",
                   help="Commit a canary/bounded batch without publishing a consumer file")
    p.add_argument("--failure-plan",
                   help="Private S3 JSON code/error map; retry only these failed funds")
    p.add_argument("--refresh-existing", action="store_true",
                   help="Validate and replace preloaded target-quarter disclosures before resuming")
    p.add_argument("--target-quarter", default=None,
                   help=("Quarter to refresh, e.g. '2026-Q3'. Resume skips "
                         "fund_codes that already have a row for this quarter; "
                         "codes missing only THIS quarter's disclosure get "
                         "re-fetched even if older rows are present. Default: "
                         "derive from today (last quarter whose window has "
                         "opened). Pass '--target-quarter all' to fall back "
                         "to full-universe backfill semantics."))
    args = p.parse_args()
    if not 1 <= args.workers <= 16 or args.batch < 1:
        p.error("workers must be 1..16 and batch must be positive")
    if args.failure_plan and (args.force or args.limit or args.export_only):
        p.error("failure-plan retry cannot use force, limit, or export-only")

    if args.target_quarter and args.target_quarter.lower() == "all":
        target_quarter: Optional[date] = None
        print("target_quarter: (full history)", flush=True)
    else:
        target_quarter = _resolve_target_quarter(args.target_quarter)
        print(f"target_quarter: {target_quarter}", flush=True)

    if args.years:
        years = [int(x.strip()) for x in args.years.split(",") if x.strip()]
    elif target_quarter is not None:
        # Only the year containing the target quarter needs to be fetched;
        # older quarters are stable and already in the table.
        years = [target_quarter.year]
    else:
        cur = date.today().year
        years = [cur, cur - 1]
    print(f"years: {years}", flush=True)
    if args.export_only:
        _export_flat_parquet(args.warehouse, target_quarter)
        return 0

    if args.failure_plan:
        failed = sorted(_load_failure_codes(args.failure_plan))
        universe = pd.DataFrame({"fund_code": failed, "fund_name": [""] * len(failed)})
        print(f"bounded failure-plan retry: {len(failed)}", flush=True)
    else:
        universe = _build_universe(args.warehouse)
    print(f"universe (equity-like): {len(universe):,}", flush=True)

    catalog = load_catalog("glue", **{
        "type": "glue", "glue.region": DEFAULT_REGION,
        "warehouse": args.warehouse,
    })
    done = (
        set() if args.force
        else _load_done_set(catalog, "fund_portfolio_hold", target_quarter)
    )
    print(f"already loaded for target (skip): {len(done):,}", flush=True)
    if args.refresh_existing:
        _refresh_existing(
            catalog, done.intersection(set(universe.fund_code)),
            years, target_quarter, workers=args.workers,
        )

    todo = universe[~universe.fund_code.isin(done)]
    if args.limit:
        todo = todo.head(args.limit)
    print(f"todo: {len(todo):,}", flush=True)

    if todo.empty:
        print("nothing to do", flush=True)
        if not args.no_export:
            _export_flat_parquet(args.warehouse, target_quarter)
        return 0

    writer = IcebergWriter(
        database=args.database, warehouse=args.warehouse, subprocess_mode=False,
    )

    pending: list[pd.DataFrame] = []
    ok = empty = total_rows = 0
    errors = {}
    t0 = time.time()

    items = list(zip(todo.fund_code, todo.fund_name))
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(_fetch_one, item, years, target_quarter): item for item in items}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                code, df, _err = fut.result(timeout=60)
            except Exception as e:
                errors[futs[fut][0]] = f"{type(e).__name__}: {str(e)[:160]}"
                continue
            if _err and _err != "no-data":
                errors[code] = _err
            if df is None or df.empty:
                empty += 1
            else:
                ok += 1
                total_rows += len(df)
                pending.append(df)
            if i % 20 == 0:
                elapsed = time.time() - t0
                eta_min = (len(items) - i) * elapsed / i / 60
                print(f"  [{i}/{len(items)}] ok={ok} empty={empty} rows={total_rows} "
                      f"elapsed={elapsed:.0f}s ETA={eta_min:.0f}min", flush=True)
            if i % args.batch == 0 and pending:
                merged = pd.concat(pending, ignore_index=True)
                r = _commit_batch(writer, merged, target_quarter, args.force)
                if "error" in r:
                    raise RuntimeError(f"portfolio batch commit failed: {r}")
                print(f"  flush {i}: {len(merged)} rows → {r}", flush=True)
                pending = []

    if pending:
        merged = pd.concat(pending, ignore_index=True)
        r = _commit_batch(writer, merged, target_quarter, args.force)
        if "error" in r:
            raise RuntimeError(f"portfolio final commit failed: {r}")
        print(f"  final: {len(merged)} rows → {r}", flush=True)

    print(f"\nDONE ok={ok} empty={empty} errors={len(errors)} rows={total_rows} "
          f"elapsed={time.time()-t0:.0f}s", flush=True)
    if errors:
        print("SOURCE_ERRORS " + json.dumps(errors, ensure_ascii=False), flush=True)
        return 1
    if not args.no_export:
        _export_flat_parquet(args.warehouse, target_quarter)
    return 0


def _commit_batch(writer, frame, target_quarter, force):
    # Resume excluded every existing fund/quarter and only target rows are
    # fetched. Appending those new keys avoids rescanning 1.4M historical
    # holdings for every 100 funds. Force/full-history retains upsert semantics.
    if target_quarter is not None and not force:
        if not frame.report_date.eq(target_quarter).all():
            raise ValueError("portfolio batch escaped its target quarter")
        result = writer._write_inline(
            "fund_portfolio_hold", frame, date.today(), force_append=True,
        )
        if result.get("rows_appended") != len(frame):
            raise RuntimeError(f"portfolio append did not commit all rows: {result}")
        return result
    result = writer.write("fund_portfolio_hold", frame, fetch_date=date.today())
    if result.get("skipped") or "error" in result:
        raise RuntimeError(f"portfolio upsert failed: {result}")
    return result


def _export_flat_parquet(warehouse: str, target_quarter=None) -> None:
    """Dedup + write flat parquet under fund/_history/ so it replicates
    to financial-dataset-mx alongside fund_manager_history /
    fund_scale_history. This is the file Mengxin's team reads directly.
    """
    import tempfile
    import boto3
    import duckdb
    from botocore.exceptions import ClientError

    bucket = DEFAULT_BUCKET
    prefix = DEFAULT_S3_PREFIX  # "fund-data-pipeline/"
    key = f"{prefix}fund/_history/fund_portfolio_hold_history.parquet"

    # Read the snapshot's file list, never a directory glob. Overwrite-based
    # operations (compaction, cleanup) leave superseded data files in place,
    # so a glob unions current and orphaned rows. That defect shipped stale
    # NAVs to the consumer via the fund_history export; this table has only
    # ever been appended to so it is currently unaffected, but relying on
    # that is fragile.
    catalog = load_catalog("glue", **{
        "type": "glue", "glue.region": DEFAULT_REGION, "warehouse": warehouse,
    })
    table = catalog.load_table(("fund_data_lake", "fund_portfolio_hold"))
    live_files = [task.file.file_path for task in table.scan().plan_files()]
    if not live_files:
        print("  no live data files; skipping export", flush=True)
        return

    print(f"\nExporting flat parquet ({len(live_files)} live files) "
          f"→ s3://{bucket}/{key}", flush=True)
    con = duckdb.connect()
    con.sql(f"CREATE SECRET s3 (TYPE s3, PROVIDER credential_chain, REGION '{DEFAULT_REGION}');")

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = f.name
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        baseline = f.name
    s3 = boto3.client("s3", region_name=DEFAULT_REGION)
    baseline_head = None
    try:
        # Keep all previously published historical keys and their values.
        # Only the explicitly refreshed quarter can supersede baseline rows.
        # This also protects older funds no longer in the live name universe.
        columns = ("fund_code, report_date, holding_code, holding_name, "
                   "weight_pct, shares, market_value")
        source = f"SELECT {columns}, 0 AS precedence FROM read_parquet({live_files!r})"
        try:
            baseline_head = s3.head_object(Bucket=bucket, Key=key)
            extra = {"VersionId": baseline_head["VersionId"]} if baseline_head.get("VersionId") else {}
            s3.download_file(bucket, key, baseline, ExtraArgs=extra)
            where = (
                f" WHERE report_date <> DATE '{target_quarter}'"
                if target_quarter is not None else ""
            )
            # Preserve older history, but the refreshed quarter comes only
            # from the live snapshot. Otherwise deleted stocks reappear from
            # the baseline even after an authoritative period replacement.
            source += (
                f" UNION ALL SELECT {columns}, 1 AS precedence "
                f"FROM read_parquet('{baseline}'){where}"
            )
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
        con.sql(f"""
            COPY (
                WITH ranked AS (
                    SELECT *, row_number() OVER (
                        PARTITION BY fund_code, report_date, holding_code
                        ORDER BY precedence DESC, weight_pct DESC NULLS LAST,
                                 shares DESC NULLS LAST, market_value DESC NULLS LAST
                    ) AS rn
                    FROM ({source})
                    WHERE report_date <= DATE '{date.today()}'
                )
                SELECT fund_code, report_date, holding_code, holding_name,
                       weight_pct, shares, market_value
                FROM ranked WHERE rn = 1
            ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        rows = con.sql(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchall()[0][0]
        funds = con.sql(f"SELECT count(DISTINCT fund_code) FROM read_parquet('{tmp}')").fetchall()[0][0]
        if funds < MIN_EXPORT_FUNDS:
            raise RuntimeError(f"partial portfolio export: {funds} funds < {MIN_EXPORT_FUNDS}")
        if target_quarter is not None:
            quarter_counts = dict(con.sql(
                f"SELECT report_date, count(DISTINCT fund_code) "
                f"FROM read_parquet('{tmp}') GROUP BY report_date"
            ).fetchall())
            previous = max((q for q in quarter_counts if q < target_quarter), default=None)
            if previous is None or quarter_counts.get(target_quarter, 0) < .9 * quarter_counts[previous]:
                raise RuntimeError(f"target quarter coverage incomplete: {quarter_counts}")
        size = os.path.getsize(tmp)
        print(f"  {rows:,} rows / {funds:,} funds / {size:,} bytes", flush=True)

        if baseline_head is not None:
            current_head = s3.head_object(Bucket=bucket, Key=key)
            if current_head["ETag"] != baseline_head["ETag"]:
                raise RuntimeError("portfolio consumer file changed during export; refusing overwrite")
        metadata = {"rows": str(rows), "funds": str(funds)}
        if target_quarter is not None:
            metadata.update({
                "target-quarter": target_quarter.isoformat(),
                "target-funds": str(quarter_counts[target_quarter]),
            })
        s3.upload_file(tmp, bucket, key, ExtraArgs={"Metadata": metadata})
        print(f"  uploaded: s3://{bucket}/{key}", flush=True)
    finally:
        con.close()
        for path in (tmp, baseline):
            try:
                os.unlink(path)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())
