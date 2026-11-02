"""Calendar window shared by monthly NAV export and its consumer monitor."""
from __future__ import annotations

import re
from datetime import date, timedelta


def export_months(today: date) -> tuple[str, str]:
    """Refresh last month too: newly published NAVs can still belong to it."""
    previous = today.replace(day=1) - timedelta(days=1)
    return previous.strftime("%Y-%m"), today.strftime("%Y-%m")


def month_start(month: str) -> date:
    """Validate an explicit month before it reaches SQL or a local/S3 path."""
    if not isinstance(month, str) or not re.fullmatch(r"\d{4}-\d{2}", month):
        raise ValueError("month must be YYYY-MM")
    try:
        return date.fromisoformat(f"{month}-01")
    except ValueError as exc:
        raise ValueError("month must be a valid YYYY-MM") from exc
