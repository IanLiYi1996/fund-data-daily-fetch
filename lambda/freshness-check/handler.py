"""Daily data-freshness gate — catches silent failures.

The 2026-06-12 and 2026-07-29 outages both had the same shape: the
workflow's own status was misleading (SUCCEEDED while writing nowhere;
then FAILED at a late step after the data had landed), and the real
signal — "did the consumer actually get fresh data today?" — was only
noticed when 孟老板 complained days later.

This Lambda asserts the consumer-visible contract directly, then posts
to Slack. It runs on its own schedule after the daily workflow window,
so it fires whether or not the workflow reported success.

Checks (each independent; all failures collected into ONE message — the
channel gets one post a day, so silence means the monitor itself stopped):

1. **workflow status** — the latest collection execution SUCCEEDED, and
   started within 30h. A schedule that stops firing produces no failure
   at all, so staleness has to be checked separately from status.
2. **fund_daily freshness** — newest ``trade_date`` within
   ``MAX_LAG_DAYS`` (weekends/holidays make "== today" too noisy).
3. **coverage** — share of the ACTIVE universe (fund_name minus known-dead
   funds) present on the last settled trading day. An absolute floor is
   blind to real regressions: at 26.7k active codes and a 20k floor,
   losing 2,000 live funds still passes.
4. **export + replication** — the current month's parquet was written
   within ``MAX_EXPORT_LAG_HOURS`` and its ``ReplicationStatus`` is
   COMPLETED, i.e. it reached the consumer bucket.
5. **upstream agreement** — sampled diff of stored NAVs and date sets
   against 天天基金. Checks 1-4 only inspect our own output, so a
   transform bug is invisible to them; the weekend date-offset defect
   shipped for two months with all of them green.
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import boto3
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual

from shared.storage.iceberg_writer import IcebergWriter
from shared.utils.logger import get_logger

logger = get_logger(__name__)

BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "fund-data-pipeline/")
WAREHOUSE = os.environ["WAREHOUSE_PATH"]
REGION = os.environ.get("AWS_REGION", "us-east-1")


_SECRET_ARN = os.environ["SLACK_WEBHOOK_SECRET_ARN"]
_webhook_cache: Optional[str] = None


def _webhook_url() -> str:
    """Resolve the Slack webhook from Secrets Manager, cached per container.

    The URL is a credential (anyone holding it can post to the channel), so
    it is never baked into source or Lambda env vars.
    """
    global _webhook_cache
    if _webhook_cache is None:
        client = boto3.client("secretsmanager", region_name=REGION)
        _webhook_cache = client.get_secret_value(SecretId=_SECRET_ARN)["SecretString"].strip()
    return _webhook_cache

# A-share holidays can stack up to 4 non-trading days (国庆/春节 run
# longer, but those windows also stall the upstream so alerting is
# still correct — it just needs a human to ack).
MAX_LAG_DAYS = int(os.environ.get("MAX_LAG_DAYS", "4"))
MIN_FUNDS = int(os.environ.get("MIN_FUNDS", "20000"))
MAX_EXPORT_LAG_HOURS = int(os.environ.get("MAX_EXPORT_LAG_HOURS", "30"))
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]

# Coverage is measured against the ACTIVE universe (fund_name minus known-dead
# funds), because ~680 codes are delisted REITs, liquidated periodic-open
# products, folded share classes and overseas-denominated QDII shares that
# will never report again. Judging on a raw count instead hides regressions:
# at a 20k floor, losing 500 live funds still passes.
MIN_COVERAGE_PCT = float(os.environ.get("MIN_COVERAGE_PCT", "97.0"))
# A code silent this long with nothing upstream is treated as inactive.
INACTIVE_AFTER_DAYS = int(os.environ.get("INACTIVE_AFTER_DAYS", "30"))
# Cap the per-run age-in probe so the check stays well inside its timeout.
MAX_AGE_IN_PROBES = int(os.environ.get("MAX_AGE_IN_PROBES", "60"))


# See slack-notifier/handler.py — the workflow's only variable is `text`,
# and HTTP 200 from the trigger does not prove the message was delivered.
_PAYLOAD_KEYS = [
    k.strip()
    for k in os.environ.get("SLACK_PAYLOAD_KEYS", "text").split(",")
    if k.strip()
]


def _post(text: str) -> None:
    body = json.dumps({k: text for k in _PAYLOAD_KEYS}).encode("utf-8")
    req = urllib.request.Request(
        _webhook_url(), data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        logger.info(f"slack post status={resp.status}")


def _catalog():
    return load_catalog("glue", **{
        "type": "glue", "glue.region": REGION, "warehouse": WAREHOUSE,
    })


def _active_universe(catalog) -> tuple[set, set]:
    """Return (universe, exempt) fund_code sets from the latest snapshots."""
    fn = catalog.load_table(("fund_data_lake", "fund_name")).scan(
        selected_fields=("fund_code", "snapshot_date"),
    ).to_arrow().to_pandas()
    if fn.empty:
        return set(), set()
    latest = fn.snapshot_date.max()
    universe = set(fn[fn.snapshot_date == latest].fund_code)

    try:
        inactive = catalog.load_table(("fund_data_lake", "fund_inactive")).scan(
            selected_fields=("fund_code",),
        ).to_arrow().to_pandas()
        exempt = set(inactive.fund_code)
    except Exception:
        exempt = set()  # table not created yet
    return universe, exempt


def _check_fund_daily(problems: List[str], facts: List[str]) -> None:
    catalog = _catalog()
    tbl = catalog.load_table(("fund_data_lake", "fund_daily"))

    # Only scan the recent window — a full scan is ~30M rows.
    since = date.today() - timedelta(days=MAX_LAG_DAYS + 3)
    arrow = tbl.scan(
        row_filter=GreaterThanOrEqual("trade_date", since),
        selected_fields=("fund_code", "trade_date"),
    ).to_arrow()

    if arrow.num_rows == 0:
        problems.append(
            f"fund_daily 在最近 {MAX_LAG_DAYS + 3} 天内**没有任何数据**"
        )
        return

    dates = arrow["trade_date"].to_pylist()
    newest = max(dates)
    lag = (date.today() - newest).days
    n_funds = len({
        c for c, d in zip(arrow["fund_code"].to_pylist(), dates) if d == newest
    })

    facts.append(
        f"fund_daily 最新 `{newest}`（滞后 {lag} 天）/ {n_funds:,} 只基金"
    )
    if lag > MAX_LAG_DAYS:
        problems.append(
            f"fund_daily 最新数据是 `{newest}`，滞后 {lag} 天"
            f"（阈值 {MAX_LAG_DAYS} 天）"
        )
    # Only score trading days. On weekends just the ~570 money-market funds
    # accrue, so a 20k floor reports a correct Sunday as a partial write —
    # which it did on 2026-08-09. The coverage check below already filters
    # to trading days; this older floor did not.
    if newest.weekday() < 5 and n_funds < MIN_FUNDS:
        problems.append(
            f"fund_daily `{newest}` 只有 {n_funds:,} 只基金"
            f"（阈值 {MIN_FUNDS:,}）— 可能是部分写入"
        )

    _check_coverage(catalog, arrow, problems, facts)


# Days whose count is a small fraction of a weekday are non-trading: only
# money-market funds accrue then, so they must not be scored for coverage.
_TRADING_DAY_MIN_FUNDS = 5000


def _check_coverage(catalog, arrow, problems: List[str], facts: List[str]) -> None:
    """Coverage on the most recent SETTLED trading day.

    Two adjustments make this meaningful, both learned by measuring:

    - Score a trading day, not the newest row date. Weekends carry only
      money-market accrual (~800 of 26.7k codes), so scoring them reads as
      a 3% collapse.
    - Score the previous settled day, not the newest one. ETFs and money
      funds disclose T+1, so on the newest day ~2.9k codes legitimately
      have nothing yet — measured at 89% on the newest day versus
      98.3-99.9% one day back.
    """
    universe, exempt = _active_universe(catalog)
    if not universe:
        return

    by_day: Dict[date, set] = {}
    for code, day in zip(arrow["fund_code"].to_pylist(),
                         arrow["trade_date"].to_pylist()):
        by_day.setdefault(day, set()).add(code)

    trading = sorted(d for d, s in by_day.items()
                     if len(s) >= _TRADING_DAY_MIN_FUNDS)
    if len(trading) < 2:
        facts.append("覆盖率: 交易日样本不足，跳过")
        return

    target = trading[-2]  # newest is still mid-disclosure
    baseline = set().union(*(by_day[d] for d in trading if d < target)) - exempt
    if not baseline:
        return

    covered = by_day[target] & baseline
    pct = 100.0 * len(covered) / len(baseline)
    facts.append(
        f"覆盖率 {pct:.1f}% @ `{target}`（活跃 {len(baseline):,} = "
        f"universe {len(universe):,} − 豁免 {len(exempt):,} − 未出现）"
    )
    if pct < MIN_COVERAGE_PCT:
        problems.append(
            f"`{target}` 覆盖率 {pct:.1f}% 低于阈值 {MIN_COVERAGE_PCT}% — "
            f"{len(baseline) - len(covered):,} 只活跃基金缺数据"
        )


def _check_export(problems: List[str], facts: List[str]) -> None:
    today = date.today()
    key = (
        f"{S3_PREFIX}fund_history/"
        f"trade_month={today.year:04d}-{today.month:02d}/part-0.parquet"
    )
    s3 = boto3.client("s3", region_name=REGION)
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
    except s3.exceptions.ClientError as e:
        problems.append(f"本月导出文件不存在或不可读: `{key}` ({e.response['Error']['Code']})")
        return

    modified = head["LastModified"]
    age_h = (datetime.now(timezone.utc) - modified).total_seconds() / 3600
    repl = head.get("ReplicationStatus", "(none)")
    size_mb = head["ContentLength"] / 1024 / 1024

    facts.append(
        f"导出 `{key.rsplit('/', 2)[-2]}` {size_mb:.1f} MB / "
        f"{age_h:.1f}h 前更新 / 复制状态 `{repl}`"
    )

    if age_h > MAX_EXPORT_LAG_HOURS:
        problems.append(
            f"本月导出已 {age_h:.1f} 小时未更新"
            f"（阈值 {MAX_EXPORT_LAG_HOURS}h）— 孟老板拿不到新数据"
        )
    if repl not in ("COMPLETED", "REPLICA"):
        problems.append(
            f"导出文件复制状态是 `{repl}`，未确认送达消费方账号"
        )


def _check_workflow(problems: List[str], facts: List[str]) -> None:
    """Report on today's collection run so the heartbeat says how it went.

    Freshness alone doesn't answer "did tonight's run work" — a partial
    failure can leave yesterday's data looking fine. Surface the actual
    execution status and duration.
    """
    sfn = boto3.client("stepfunctions", region_name=REGION)
    execs = sfn.list_executions(
        stateMachineArn=STATE_MACHINE_ARN, maxResults=1
    )["executions"]
    if not execs:
        problems.append("找不到任何 FundDataCollectionWorkflow 执行记录")
        return

    e = execs[0]
    status = e["status"]
    started = e["startDate"]
    mins = (
        (e["stopDate"] - started).total_seconds() / 60
        if e.get("stopDate") else None
    )
    dur = f"{mins:.0f} 分钟" if mins is not None else "进行中"
    facts.append(
        f"采集工作流 `{status}` / {started.strftime('%m-%d %H:%M')} UTC 启动 / {dur}"
    )
    if status not in ("SUCCEEDED", "RUNNING"):
        problems.append(f"最近一次采集工作流状态是 `{status}`")

    # A run that started well before today's window means the schedule
    # stopped firing — the workflow isn't failing, it just isn't running.
    age_h = (datetime.now(timezone.utc) - started).total_seconds() / 3600
    if age_h > 30:
        problems.append(
            f"最近一次采集是 {age_h:.0f} 小时前 — 定时触发可能已停止"
        )


def _has_upstream_series(fund_code: str) -> bool:
    """True if 天天基金 still publishes a NAV history for this code."""
    try:
        r = urllib.request.Request(
            f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={fund_code}"
            f"&pageIndex=1&pageSize=1",
            headers={
                "Referer": "https://fundf10.eastmoney.com/",
                "User-Agent": "Mozilla/5.0",
            },
        )
        with urllib.request.urlopen(r, timeout=10) as resp:
            payload = json.loads(resp.read())
        return bool(payload.get("Data", {}).get("LSJZList"))
    except Exception:
        # On a probe error, assume the fund is still alive: wrongly exempting
        # a live fund hides a real gap, while a missed exemption is only noise.
        return True


def _maintain_exemptions(facts: List[str]) -> None:
    """Age codes into and out of fund_inactive.

    Hand-maintained lists rot. The estimate that ~900 money-market funds were
    missing sat in an issue for weeks after the fallback Lambda had already
    fixed all but 29 of them, and that stale number drove a whole spike down
    the wrong path. So this is derived from the data on every run.

    Age IN:  absent from fund_daily for INACTIVE_AFTER_DAYS and no upstream
             series. Probes are capped per run; the backlog drains over
             successive days.
    Age OUT: reappeared in fund_daily. Removed immediately — a resumed fund
             must not stay invisible to the coverage check.
    """
    import pandas as pd

    catalog = _catalog()
    universe, exempt = _active_universe(catalog)
    if not universe:
        return

    since = date.today() - timedelta(days=INACTIVE_AFTER_DAYS)
    recent = catalog.load_table(("fund_data_lake", "fund_daily")).scan(
        row_filter=GreaterThanOrEqual("trade_date", since),
        selected_fields=("fund_code",),
    ).to_arrow()
    seen = set(recent["fund_code"].to_pylist())

    revived = sorted(exempt & seen)
    candidates = sorted((universe - seen) - exempt)

    writer = IcebergWriter(
        database="fund_data_lake", warehouse=WAREHOUSE, subprocess_mode=False,
    )

    if revived:
        table = catalog.load_table(("fund_data_lake", "fund_inactive"))
        keep = table.scan().to_arrow().to_pandas()
        keep = keep[~keep.fund_code.isin(revived)]
        arrow = __import__("pyarrow").Table.from_pandas(
            keep, preserve_index=False,
        ).cast(table.schema().as_arrow())
        table.overwrite(arrow)
        logger.info(f"aged out {len(revived)}: {revived[:10]}")

    added = []
    for code in candidates[:MAX_AGE_IN_PROBES]:
        if not _has_upstream_series(code):
            added.append(code)

    if added:
        writer.write("fund_inactive", pd.DataFrame({
            "fund_code": added,
            "reason": ["no_upstream_series"] * len(added),
            "last_seen_date": [None] * len(added),
            "verified_at": [date.today()] * len(added),
        }), fetch_date=date.today())
        logger.info(f"aged in {len(added)}")

    parts = []
    if added:
        parts.append(f"新增豁免 {len(added)}")
    if revived:
        parts.append(f"复活移出 {len(revived)}")
    pending = max(0, len(candidates) - MAX_AGE_IN_PROBES)
    if pending:
        parts.append(f"待探测 {pending}")
    if parts:
        facts.append("豁免名单维护: " + " / ".join(parts))


def _check_upstream(problems: List[str], facts: List[str]) -> None:
    """Sampled value+date diff against upstream — the only check that asks
    whether the data is RIGHT rather than merely present.

    Folded into the heartbeat rather than run as its own Lambda so the
    channel gets one message a day. Sampling is capped and rotates by date,
    so coverage accumulates across runs instead of re-checking the same
    names.
    """
    from shared.quality.reconcile import reconcile

    r = reconcile(_catalog())
    if not r.get("checked"):
        facts.append("上游对账: 样本为空，跳过")
        return

    facts.append(
        f"上游对账 {r['passed']}/{r['checked']} 只一致"
        + (f"（探测失败 {r['probe_errors']}）" if r["probe_errors"] else "")
    )
    if r["extra_dates"]:
        problems.append(
            f"我们多出 {len(r['extra_dates'])} 个日期（上游无）: "
            + ", ".join(r["extra_dates"][:5])
        )
    if r["missing_dates"]:
        problems.append(
            f"我们缺少 {len(r['missing_dates'])} 个日期（上游有）: "
            + ", ".join(r["missing_dates"][:5])
        )
    if r["value_diffs"]:
        problems.append(
            f"净值与上游不符 {len(r['value_diffs'])} 处: "
            + "; ".join(r["value_diffs"][:5])
        )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    problems: List[str] = []
    facts: List[str] = []

    for check in (_check_workflow, _check_fund_daily, _check_export,
                  _check_upstream):
        try:
            check(problems, facts)
        except Exception as e:
            problems.append(f"{check.__name__} 自身报错: {type(e).__name__}: {e}")

    # List upkeep is best-effort: a failure here must not mask the checks
    # above, so it only logs.
    try:
        _maintain_exemptions(facts)
    except Exception as e:
        logger.warning(f"exemption upkeep failed: {type(e).__name__}: {e}")

    # Post every run, pass or fail. Staying silent on success looked tidy
    # but made "everything is fine" and "the monitor itself is dead"
    # indistinguishable from the outside — on 2026-08-03 this check ran
    # clean and sent nothing, which read as an outage. A daily heartbeat
    # turns absence of a message into a signal instead of an ambiguity.
    # Set quiet_when_healthy to suppress the green ping for ad-hoc runs.
    if problems:
        body = "\n".join(
            [f"🔴 *fund-data 数据新鲜度检查未通过* ({len(problems)} 项)"]
            + [f"• {p}" for p in problems]
            + ["", "*当前状态*"]
            + [f"• {f}" for f in facts]
        )
        _post(body)
        logger.error(f"{len(problems)} problems: {problems}")
    elif not event.get("quiet_when_healthy"):
        body = "\n".join(
            ["✅ *fund-data 每日检查通过*"] + [f"• {f}" for f in facts]
        )
        _post(body)
        logger.info("healthy; heartbeat sent")

    return {
        "statusCode": 200,
        "healthy": not problems,
        "problems": problems,
        "facts": facts,
    }
