"""Daily data-freshness gate — catches silent failures.

The 2026-06-12 and 2026-07-29 outages both had the same shape: the
workflow's own status was misleading (SUCCEEDED while writing nowhere;
then FAILED at a late step after the data had landed), and the real
signal — "did the consumer actually get fresh data today?" — was only
noticed when 孟老板 complained days later.

This Lambda asserts the consumer-visible contract directly, then posts
to Slack. It runs on its own schedule after the daily workflow window,
so it fires whether or not the workflow reported success.

Checks (each independent; all failures collected into one message):

1. **fund_daily freshness** — the newest ``trade_date`` in the Iceberg
   table is within ``MAX_LAG_DAYS`` of today (weekends/holidays make a
   strict "== today" check too noisy).
2. **fund_daily coverage** — that newest day has at least
   ``MIN_FUNDS`` distinct fund_codes, so a partial write is caught.
3. **export freshness** — the current month's
   ``fund_history/trade_month=YYYY-MM/part-0.parquet`` was modified
   within ``MAX_EXPORT_LAG_HOURS``.
4. **replication** — that object's ``ReplicationStatus`` is COMPLETED,
   i.e. it actually reached the consumer bucket.
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


def _check_fund_daily(problems: List[str], facts: List[str]) -> None:
    catalog = load_catalog("glue", **{
        "type": "glue", "glue.region": REGION, "warehouse": WAREHOUSE,
    })
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

    facts.append(f"fund_daily 最新 `{newest}`（滞后 {lag} 天）/ {n_funds:,} 只基金")

    if lag > MAX_LAG_DAYS:
        problems.append(
            f"fund_daily 最新数据是 `{newest}`，滞后 {lag} 天"
            f"（阈值 {MAX_LAG_DAYS} 天）"
        )
    if n_funds < MIN_FUNDS:
        problems.append(
            f"fund_daily `{newest}` 只有 {n_funds:,} 只基金"
            f"（阈值 {MIN_FUNDS:,}）— 可能是部分写入"
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


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    problems: List[str] = []
    facts: List[str] = []

    for check in (_check_fund_daily, _check_export):
        try:
            check(problems, facts)
        except Exception as e:
            problems.append(f"{check.__name__} 自身报错: {type(e).__name__}: {e}")

    if problems:
        body = "\n".join(
            [f"🔴 *fund-data 数据新鲜度检查未通过* ({len(problems)} 项)"]
            + [f"• {p}" for p in problems]
            + ["", "*当前状态*"]
            + [f"• {f}" for f in facts]
        )
        _post(body)
        logger.error(f"{len(problems)} problems: {problems}")
    elif event.get("always_notify"):
        body = "\n".join(
            ["✅ *fund-data 数据新鲜度检查通过*"] + [f"• {f}" for f in facts]
        )
        _post(body)

    return {
        "statusCode": 200,
        "healthy": not problems,
        "problems": problems,
        "facts": facts,
    }
