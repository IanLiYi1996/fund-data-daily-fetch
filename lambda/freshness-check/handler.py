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
5. **history exports** — age AND distinct-fund count of the three flat
   files under ``fund/_history/``. Checks 1-4 only look at fund_daily and
   the monthly export, which is why fund_scale_history could stall for a
   month and fund_manager_history could shed ~1,000 funds per weekly run
   with every check green (both found 2026-08-10).
6. **upstream agreement** — sampled diff of stored NAVs and date sets
   against 天天基金. The other checks only inspect our own output, so a
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

# Coverage is measured against DAILY-CADENCE funds, not the whole active
# universe. Judging on a raw count hides regressions (at a 20k floor, losing
# 500 live funds still passes), but judging on every code ever seen produces
# the opposite failure: ~1,700 weekly and hold-period products are absent on
# any given day by design, which fired a 95.8% false alarm on 2026-08-11.
#
# 99% not 97%: on the cadence-scoped denominator the metric measured
# 99.81-99.93% across 12 trading days (median 99.86%, no weekday effect), so
# 99% leaves ~0.8pp ≈ 180 funds of headroom while catching a 200-fund
# regression. Verified by injection: dropping 200 daily funds reads 98.9%.
MIN_COVERAGE_PCT = float(os.environ.get("MIN_COVERAGE_PCT", "99.0"))
# A fund counts as daily-cadence if it disclosed on at least this share of
# recent trading days.
#
# 90%, not the 80% first shipped. 80% excluded weekly publishers (20-60%) but
# still admitted FOFs that publish roughly 4 days in 5, and those made the
# denominator itself unstable: as the window slid past a low-coverage stretch,
# ~4,150 of them crossed the bar at once (21,580 -> 25,737 between 2026-08-14
# and 08-17), which invalidated the threshold calibrated against the smaller
# population and fired at 96.34% on 08-19. Of the 941 funds reported missing
# that day, 931 sat in the 80-85% band and 927 were FOF, and upstream had no
# value for 20 of 20 sampled — again a metric asking funds to do something they
# never do.
#
# 90% is where the instability collapses, measured over 15 trading days:
#
#     cadence bar   denominator span   coverage min
#          80%           4,250            93.50%
#          85%           3,325            97.01%
#          90%             103            99.93%
#          95%             134            99.95%
#
# The knee is sharp because the genuinely-daily population really is ~21.4k;
# everything between 80% and 90% is hold-period products. Anything >=90% gives
# the same answer, so 90% is chosen as the loosest stable setting.
DAILY_CADENCE_MIN_PCT = float(os.environ.get("DAILY_CADENCE_MIN_PCT", "90"))
# Calendar days of history to scan when classifying cadence.
CADENCE_WINDOW_DAYS = int(os.environ.get("CADENCE_WINDOW_DAYS", "40"))
# Below this many trading days of history, cadence can't be judged and the
# coverage check abstains rather than guessing.
_MIN_CADENCE_DAYS = 10
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

    # Scan a cadence-length window, not just the freshness window: the
    # coverage check below has to know each fund's disclosure frequency, and
    # that cannot be inferred from 7 days. Still far short of a full ~30M-row
    # scan — 40 days is roughly 1M rows over three columns.
    since = date.today() - timedelta(days=CADENCE_WINDOW_DAYS)
    arrow = tbl.scan(
        row_filter=GreaterThanOrEqual("trade_date", since),
        selected_fields=("fund_code", "trade_date", "unit_nav"),
    ).to_arrow()

    if arrow.num_rows == 0:
        problems.append(
            f"fund_daily 在最近 {CADENCE_WINDOW_DAYS} 天内**没有任何数据**"
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
    """Coverage on the most recent SETTLED trading day, scored only against
    funds that actually disclose daily.

    Three adjustments make this meaningful, all learned by measuring:

    - Score a trading day, not the newest row date. Weekends carry only
      money-market accrual (~800 of 26.7k codes), so scoring them reads as
      a 3% collapse.
    - Score the previous settled day, not the newest one. ETFs and money
      funds disclose T+1, so on the newest day ~2.9k codes legitimately
      have nothing yet — measured at 89% on the newest day versus
      98.3-99.9% one day back.
    - Score only DAILY-cadence funds. "Any code seen on an earlier trading
      day" is the wrong denominator: plenty of bond funds and FOFs publish
      weekly or on a hold-period schedule, so they are absent most days by
      design. On 2026-08-11 this fired at 95.8% claiming "1,136 active funds
      missing data" — of the 1,762 codes absent on the target day, 1,035
      disclosed on 20-60% of trading days and 675 on under 20%, while only
      **5** were genuinely daily-cadence. Upstream had no value for the
      sampled ones either (0 of 8). The alert was unactionable by
      construction: it asked ~1,700 funds to do something they never do.

    Restricting the denominator to funds disclosing on at least
    ``DAILY_CADENCE_MIN_PCT`` of recent trading days takes the same day from
    95.8% to 99.9%, and the metric measured 99.81-99.93% across 12 trading
    days with no weekday effect — a tight enough band to alarm at 99%, which
    catches a ~200-fund regression. The old 97% floor on the noisy
    denominator needed a 2,000-fund loss to fire.
    """
    universe, exempt = _active_universe(catalog)
    if not universe:
        return

    # Presence means a usable NAV. A row with a null NAV is not coverage —
    # that distinction is what the fallback fetcher keys on too.
    by_day: Dict[date, set] = {}
    for code, day, nav in zip(arrow["fund_code"].to_pylist(),
                              arrow["trade_date"].to_pylist(),
                              arrow["unit_nav"].to_pylist()):
        if nav is not None:
            by_day.setdefault(day, set()).add(code)

    trading = sorted(d for d, s in by_day.items()
                     if len(s) >= _TRADING_DAY_MIN_FUNDS)
    if len(trading) < 2:
        facts.append("覆盖率: 交易日样本不足，跳过")
        return

    target = trading[-2]  # newest is still mid-disclosure
    history = [d for d in trading if d < target]
    if len(history) < _MIN_CADENCE_DAYS:
        facts.append(
            f"覆盖率: 交易日样本仅 {len(history)} 天，不足以判定披露频率，跳过"
        )
        return

    appearances: Dict[str, int] = {}
    for d in history:
        for code in by_day[d]:
            appearances[code] = appearances.get(code, 0) + 1

    threshold = DAILY_CADENCE_MIN_PCT / 100.0 * len(history)
    daily = {
        c for c, n in appearances.items()
        if n >= threshold and c not in exempt
    }
    if not daily:
        return

    covered = by_day[target] & daily
    pct = 100.0 * len(covered) / len(daily)
    facts.append(
        f"覆盖率 {pct:.2f}% @ `{target}`（日频基金 {len(daily):,}，"
        f"按近 {len(history)} 个交易日 ≥{DAILY_CADENCE_MIN_PCT:.0f}% 披露判定）"
    )
    if pct < MIN_COVERAGE_PCT:
        problems.append(
            f"`{target}` 日频基金覆盖率 {pct:.2f}% 低于阈值 {MIN_COVERAGE_PCT}% — "
            f"{len(daily) - len(covered):,} 只日频基金缺数据"
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


# The three flat files under fund/_history/ that the consumer reads directly,
# with the age at which each is genuinely stale. These are refreshed on their
# own schedules (manager weekly, scale monthly, portfolio quarterly), so a
# single threshold would either nag or miss.
#
# Nothing watched these until 2026-08-10, and both defects found that day were
# invisible for exactly that reason: fund_scale_history stopped updating after
# 2026-07-04 and went unnoticed for over a month (consumers screening
# liquidation risk read 3/31 figures), while fund_manager_history silently
# dropped ~1,000 rate-limited funds every weekly run. Checks 1-5 all stayed
# green throughout — they only ever looked at fund_daily and the monthly
# export.
# (max age in days, minimum distinct funds). Both numbers are calibrated by
# replaying the two 2026-08-10 defects against them; a first pass at
# (40 days, 24000 funds) would have MISSED both, which is the whole point of
# the check:
#
# - scale stalled after 2026-07-04 and was 37 days old when found — under a
#   40-day threshold. A monthly refresh should never be more than ~35 days
#   old, so 33 catches a single missed run without nagging.
# - manager shed 988 funds to 26,299 — above a 24,000 floor. The floor has to
#   sit just under the real population (~27.3k) to catch a partial refresh,
#   not at a round number well below it.
#
# The fund floor is per-file, not shared: the portfolio file legitimately
# covers only ~15.3k funds because money-market and pure-bond products have no
# 股票投资明细 to disclose and the backfill skips them on purpose. A single 20k
# floor fires a false alarm on it every day — measured before shipping.
_HISTORY_FILES = {
    "fund_manager_history": (10, 26500),          # weekly refresh, ~27.3k funds
    "fund_scale_history": (33, 26000),            # monthly refresh, ~27.0k funds
    "fund_portfolio_hold_history": (100, 14000),  # quarterly, equity-like only
}


def _check_history_exports(problems: List[str], facts: List[str]) -> None:
    """Age + row-count check on the flat history files.

    Row count matters as much as age here: the manager-history defect
    republished the file every week, so it always looked fresh while ~1,000
    funds were missing from it. A sudden drop in distinct funds is the only
    consumer-visible symptom.
    """
    import io

    import pandas as pd

    s3 = boto3.client("s3", region_name=REGION)
    for base, (max_age_days, min_funds) in _HISTORY_FILES.items():
        key = f"{S3_PREFIX}fund/_history/{base}.parquet"
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
        except Exception as e:
            problems.append(f"历史文件缺失或不可读: `{base}` ({type(e).__name__})")
            continue

        age_d = (datetime.now(timezone.utc) - head["LastModified"]).days
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            raw = io.BytesIO(obj["Body"].read())
            # The two Lambda-produced files keep upstream's Chinese headers;
            # the portfolio file is built by the Fargate backfill and carries
            # normalized English ones. Resolve the name from the footer schema
            # so only the one needed column is decoded.
            import pyarrow.parquet as pq

            names = pq.ParquetFile(raw).schema_arrow.names
            code_col = next(
                (c for c in ("基金代码", "fund_code") if c in names), None
            )
            if code_col is None:
                problems.append(f"`{base}` 找不到基金代码列（列: {names[:6]}）")
                continue
            raw.seek(0)
            n_funds = pd.read_parquet(raw, columns=[code_col])[code_col].nunique()
        except Exception as e:
            problems.append(f"`{base}` 无法解析: {type(e).__name__}: {e}")
            continue

        facts.append(f"历史 `{base}` {n_funds:,} 只 / {age_d} 天前更新")

        if age_d > max_age_days:
            problems.append(
                f"`{base}` 已 {age_d} 天未更新（阈值 {max_age_days} 天）"
            )
        # A drop below the floor means a refresh published a partial file —
        # the failure mode that cost 988 funds on 2026-08-09.
        if n_funds < min_funds:
            problems.append(
                f"`{base}` 只有 {n_funds:,} 只基金"
                f"（阈值 {min_funds:,}）— 可能是部分刷新覆盖了完整数据"
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


# Lifecycle status values published to consumers. The distinction consumers
# asked for (2026-08-10) is 停更 vs 已终止: a fund that has merely paused
# disclosure may resume and should stay in a screening universe, while a
# liquidated/delisted one must be dropped. `reason` alone could not express
# that — it recorded how we detected silence, not what the silence means.
STATUS_ACTIVE = "active"        # disclosed within INACTIVE_AFTER_DAYS
STATUS_STALLED = "stalled"      # 停更: silent, but upstream still lists a series
STATUS_TERMINATED = "terminated"  # 已终止: silent AND no upstream series
STATUS_NEVER = "never_reported"  # in fund_name, never produced a NAV
# Silent a long time and not yet probed. Reported separately from `stalled`
# because calling these "may resume" is misleading: measured 2026-08-10, 31 of
# 52 unprobed silent codes had been quiet for over a YEAR (max 3,603 days —
# ~10 years). No consumer should keep those in a screening universe, but we
# also haven't confirmed them dead, so they must not be labelled terminated.
STATUS_PRESUMED_DEAD = "presumed_terminated"
# Beyond this, silence stops being plausibly a disclosure cadence. The longest
# legitimate gap is a quarterly-hold product plus reporting lag; a year is far
# outside that.
PRESUMED_DEAD_AFTER_DAYS = int(os.environ.get("PRESUMED_DEAD_AFTER_DAYS", "365"))


def _lifecycle_status(has_series: bool, last_seen: Optional[date]) -> str:
    """Classify a silent fund.

    Upstream presence is the discriminator, not elapsed time: a 3-month-hold
    FOF can be silent for 90 days and still be perfectly alive, so ageing
    alone would wrongly declare it terminated. 天天基金 drops the NAV series
    for liquidated and delisted products, which is what makes this checkable.
    """
    if last_seen is None and not has_series:
        return STATUS_TERMINATED
    if last_seen is None:
        return STATUS_NEVER
    return STATUS_STALLED if has_series else STATUS_TERMINATED


def _last_nav_dates(catalog, codes: List[str]) -> Dict[str, date]:
    """Newest trade_date holding a real NAV, per code, over all history.

    Scans the full table rather than a recent window on purpose: these codes
    are silent by definition, so a windowed scan returns nothing for exactly
    the funds being classified. Only the two needed columns are read.
    """
    if not codes:
        return {}
    import pyarrow.compute as pc
    from pyiceberg.expressions import In

    # Push the code filter into the scan and aggregate in Arrow. Materializing
    # ~30M rows here (even column-projected) is what OOM-killed a 1 GB Lambda.
    arrow = catalog.load_table(("fund_data_lake", "fund_daily")).scan(
        row_filter=In("fund_code", codes),
        selected_fields=("fund_code", "trade_date", "unit_nav"),
    ).to_arrow()
    if arrow.num_rows == 0:
        return {}
    arrow = arrow.filter(pc.is_valid(arrow["unit_nav"]))
    if arrow.num_rows == 0:
        return {}
    grouped = arrow.group_by("fund_code").aggregate([("trade_date", "max")])
    return dict(zip(grouped["fund_code"].to_pylist(),
                    grouped["trade_date_max"].to_pylist()))


def _maintain_exemptions(facts: List[str]) -> None:
    """Age codes into and out of fund_inactive, and classify their lifecycle.

    Hand-maintained lists rot. The estimate that ~900 money-market funds were
    missing sat in an issue for weeks after the fallback Lambda had already
    fixed all but 29 of them, and that stale number drove a whole spike down
    the wrong path. So this is derived from the data on every run.

    Age IN:  absent from fund_daily for INACTIVE_AFTER_DAYS and no upstream
             series. Probes are capped per run; the backlog drains over
             successive days.
    Age OUT: reappeared in fund_daily. Removed immediately — a resumed fund
             must not stay invisible to the coverage check.

    Also fills ``last_seen_date`` and ``status``. Both were left null/absent,
    which is exactly what blocked consumers from telling 停更 from 已终止 —
    they could see a fund was silent but not whether it would ever return.
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

    # Last date each candidate produced a real NAV, over all history — the
    # field consumers need to judge how long a fund has been quiet. It was
    # written as None for every row until 2026-08-10, so the table could say
    # "inactive" but not "since when".
    last_seen = _last_nav_dates(catalog, candidates[:MAX_AGE_IN_PROBES])

    rows = []
    for code in candidates[:MAX_AGE_IN_PROBES]:
        has_series = _has_upstream_series(code)
        seen = last_seen.get(code)
        status = _lifecycle_status(has_series, seen)
        # A stalled fund is NOT exempted from the coverage check: it is still
        # expected to report, so hiding it would suppress a real gap. Only
        # genuinely dead codes earn an exemption.
        if status in (STATUS_TERMINATED, STATUS_NEVER):
            rows.append({
                "fund_code": code,
                "reason": "no_upstream_series",
                "last_seen_date": seen,
                "verified_at": date.today(),
                "status": status,
            })

    if rows:
        writer.write("fund_inactive", pd.DataFrame(rows), fetch_date=date.today())
        logger.info(f"aged in {len(rows)}")

    parts = []
    if rows:
        parts.append(f"新增豁免 {len(rows)}")
    if revived:
        parts.append(f"复活移出 {len(revived)}")
    pending = max(0, len(candidates) - MAX_AGE_IN_PROBES)
    if pending:
        parts.append(f"待探测 {pending}")
    if parts:
        facts.append("豁免名单维护: " + " / ".join(parts))

    _export_lifecycle(catalog, facts)


def _export_lifecycle(catalog, facts: List[str]) -> None:
    """Publish fund lifecycle status to the consumer-readable prefix.

    The table existed and was maintained daily, but lived only in our Iceberg
    warehouse — consumers had no way to read it, which is why they asked for a
    status field they in effect already had. Writing it under fund/_history/
    puts it in the same replicated location as the other flat files.

    Covers the WHOLE universe, not just the exempt list: a consumer building a
    screening universe needs a verdict for every code, and "absent from this
    file" is not a usable answer.
    """
    import io

    import pandas as pd

    fn = catalog.load_table(("fund_data_lake", "fund_name")).scan(
        selected_fields=("fund_code", "fund_name", "fund_type", "snapshot_date"),
    ).to_arrow().to_pandas()
    if fn.empty:
        return
    fn = (fn[fn.snapshot_date == fn.snapshot_date.max()]
          .drop_duplicates("fund_code").reset_index(drop=True))

    # Aggregate in Arrow, never via pandas. fund_daily is ~30M rows; calling
    # .to_pandas() on it used 1,025 MB of a 1,024 MB Lambda and the runtime
    # was killed. group_by on the Arrow table keeps this bounded.
    import pyarrow.compute as pc

    arrow = catalog.load_table(("fund_data_lake", "fund_daily")).scan(
        selected_fields=("fund_code", "trade_date", "unit_nav"),
    ).to_arrow()
    arrow = arrow.filter(pc.is_valid(arrow["unit_nav"])).drop_columns(["unit_nav"])
    grouped = arrow.group_by("fund_code").aggregate([("trade_date", "max")])
    last = pd.Series(
        grouped["trade_date_max"].to_pylist(),
        index=grouped["fund_code"].to_pylist(),
    )
    del arrow, grouped
    newest = last.max()

    try:
        inactive = catalog.load_table(
            ("fund_data_lake", "fund_inactive")
        ).scan().to_arrow().to_pandas().set_index("fund_code")
    except Exception:
        inactive = pd.DataFrame()

    out = fn[["fund_code", "fund_name", "fund_type"]].copy()
    out["last_nav_date"] = out.fund_code.map(last)
    # trade_date arrives as python date objects, so subtraction yields
    # timedelta objects rather than a datetime64 series — .dt is unavailable.
    out["lag_days"] = out.last_nav_date.map(
        lambda d: None if pd.isna(d) else (newest - d).days
    )

    def _status(row) -> str:
        if pd.isna(row.last_nav_date):
            # Recorded terminations win over "never seen": a code we probed and
            # found dead is a stronger statement than absence from fund_daily.
            if not inactive.empty and row.fund_code in inactive.index:
                return str(inactive.loc[row.fund_code].get("status") or STATUS_TERMINATED)
            return STATUS_NEVER
        if row.lag_days <= INACTIVE_AFTER_DAYS:
            return STATUS_ACTIVE
        if not inactive.empty and row.fund_code in inactive.index:
            return str(inactive.loc[row.fund_code].get("status") or STATUS_TERMINATED)
        # Silent past the window but never probed. Not `terminated` — that
        # would quietly evict live hold-period products (a 3-month-hold FOF is
        # legitimately silent). But not plain `stalled` either once the silence
        # runs to a year: see STATUS_PRESUMED_DEAD.
        if row.lag_days is not None and row.lag_days > PRESUMED_DEAD_AFTER_DAYS:
            return STATUS_PRESUMED_DEAD
        return STATUS_STALLED

    out["status"] = out.apply(_status, axis=1)
    out["as_of"] = newest

    buf = io.BytesIO()
    out.to_parquet(buf, engine="pyarrow", index=False)
    key = f"{S3_PREFIX}fund/_history/fund_lifecycle.parquet"
    boto3.client("s3", region_name=REGION).put_object(
        Bucket=BUCKET, Key=key, Body=buf.getvalue(),
        ContentType="application/x-parquet",
    )
    counts = out.status.value_counts().to_dict()
    facts.append(
        "生命周期导出: "
        + " / ".join(f"{k} {v:,}" for k, v in sorted(counts.items()))
    )
    logger.info(f"wrote {key}: {len(out):,} rows {counts}")


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
                  _check_history_exports, _check_upstream):
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
