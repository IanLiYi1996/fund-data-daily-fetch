"""Post pipeline alerts to Slack.

Two entry shapes:

1. **SNS message** (CloudWatch Alarm → SNS → here). Parses the alarm
   payload and posts a formatted Chinese message.

2. **Direct invoke** with ``{"message": "..."}`` — used by the
   freshness-check path and for ad-hoc notification from other Lambdas.

The webhook is a Slack Workflow trigger, so the payload key is
``Content`` (not the usual ``text`` of an Incoming Webhook).
"""
from __future__ import annotations

import json
import os
import urllib.request
from typing import Any, Dict

from shared.utils.logger import get_logger

logger = get_logger(__name__)

WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
REGION = os.environ.get("AWS_REGION", "us-east-1")

# CloudWatch alarm name → human-readable Chinese label, so the Slack
# message says what actually broke rather than a CFN-ish identifier.
_ALARM_LABELS = {
    "FundDataCollectionWorkflowFailed": "每日数据采集工作流失败",
    "FundDataCollectionWorkflowTimedOut": "每日数据采集工作流超时",
    "FundHistoryWorkflowFailed": "基金历史回填工作流失败",
}

_STATE_EMOJI = {"ALARM": "🔴", "OK": "✅", "INSUFFICIENT_DATA": "⚠️"}

# Variable names a Slack Workflow webhook trigger might declare for the
# message body. Override with SLACK_PAYLOAD_KEYS (comma-separated) once the
# actual name is confirmed in the Slack UI.
_PAYLOAD_KEYS = [
    k.strip()
    for k in os.environ.get(
        "SLACK_PAYLOAD_KEYS", "Content,content,text,message,Message"
    ).split(",")
    if k.strip()
]


def _payload(text: str) -> Dict[str, str]:
    return {k: text for k in _PAYLOAD_KEYS}


def _post(text: str) -> int:
    # Slack Workflow triggers accept any JSON and return ok:true regardless
    # of whether the declared variables matched, so a name mismatch shows up
    # only as "there's a problem with the workflow's setup" INSIDE Slack.
    # Send the same text under every plausible variable name — unreferenced
    # keys are ignored by the trigger, so this is a no-cost hedge.
    body = json.dumps(_payload(text)).encode("utf-8")
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.status


def _console_url(alarm_name: str) -> str:
    return (
        f"https://{REGION}.console.aws.amazon.com/cloudwatch/home"
        f"?region={REGION}#alarmsV2:alarm/{alarm_name}"
    )


def _format_alarm(alarm: Dict[str, Any]) -> str:
    name = alarm.get("AlarmName", "(unknown)")
    state = alarm.get("NewStateValue", "ALARM")
    reason = alarm.get("NewStateReason", "")
    label = _ALARM_LABELS.get(name, name)
    emoji = _STATE_EMOJI.get(state, "❓")

    lines = [
        f"{emoji} *fund-data 管道告警* — {label}",
        f"状态: `{state}`",
    ]
    if reason:
        lines.append(f"原因: {reason}")
    lines.append(f"控制台: {_console_url(name)}")
    return "\n".join(lines)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # Direct invoke — caller already composed the message.
    if "message" in event:
        status = _post(str(event["message"]))
        return {"statusCode": status, "posted": 1}

    posted = 0
    for record in event.get("Records", []):
        raw = record.get("Sns", {}).get("Message", "")
        try:
            alarm = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            # Not an alarm JSON — forward the raw SNS body so nothing is
            # silently dropped.
            _post(f"⚠️ *fund-data 告警*\n{raw}")
            posted += 1
            continue

        # Suppress OK transitions: the pipeline recovers on its own most
        # days and an OK ping per alarm per morning is noise.
        if alarm.get("NewStateValue") == "OK":
            logger.info(f"skipping OK transition for {alarm.get('AlarmName')}")
            continue

        _post(_format_alarm(alarm))
        posted += 1

    return {"statusCode": 200, "posted": posted}
