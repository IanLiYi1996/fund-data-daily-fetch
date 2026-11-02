"""Reconstruct HK valuations by NAV date, not by unobservable publication time.

These records MUST stay separate from fund_hk_rank. Neither an as-of valuation
nor a price change is an original observed ranking / dividend-reinvested return.
"""
from __future__ import annotations

import calendar
import math
import re
from bisect import bisect_right
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Callable

import requests

HISTORY_URL = "https://overseas.1234567.com.cn/overseasapi/OpenApiHander.ashx"
RETURN_BASIS = "unit_nav_price_change_no_dividend_reinvestment"
_BLANKS = {"", "--", "---", "-"}


def _day(value, label: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return date.fromisoformat(value)


def _number(value, label: str, *, nav: bool = False) -> float | None:
    if value is None or (isinstance(value, str) and value.strip() in _BLANKS):
        return None
    if isinstance(value, bool):
        raise ValueError(f"Invalid {label}: boolean")
    number = float(value)
    if not math.isfinite(number) or (nav and number <= 0):
        raise ValueError(f"Invalid {label}: non-finite or non-positive NAV")
    return number


def _subtract_months(day: date, months: int) -> date:
    year, month0 = divmod(day.year * 12 + day.month - 1 - months, 12)
    month = month0 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def normalize_members(rows: list[dict]) -> list[dict]:
    """Keep the pre-incident population, including funds with no reported NAV."""
    required = {"基金代码", "香港基金代码", "基金简称", "币种", "日期", "单位净值"}
    members = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or not required.issubset(row):
            raise ValueError("Membership missing required fields")
        code, hk_code = str(row["基金代码"]), str(row["香港基金代码"])
        if not re.fullmatch(r"\d{6}", code):
            raise ValueError(f"Invalid fund code: {code}")
        if not re.fullmatch(r"\d{10}", hk_code):
            raise ValueError(f"Invalid HK fund code: {hk_code}")
        if code in seen:
            raise ValueError(f"Membership contains duplicate fund code: {code}")
        seen.add(code)
        nav = _number(row["单位净值"], "membership NAV", nav=True)
        nav_date = None if row["日期"] is None else _day(row["日期"], "membership date")
        if (nav is None) != (nav_date is None):
            raise ValueError(f"Membership NAV/date disagree for {code}")
        members.append({
            "fund_code": code, "hk_fund_code": hk_code,
            "fund_name": str(row["基金简称"]), "currency": str(row["币种"]),
            "nav_date": nav_date, "unit_nav": nav,
            "daily_return_pct": _number(row.get("日增长率"), "membership daily return"),
        })
    if not members:
        raise ValueError("Empty membership")
    return sorted(members, key=lambda m: m["fund_code"])


def history_start(member: dict, first_target: date) -> date:
    # Frozen funds can have a last NAV years before the incident. Their
    # baseline must be fetched around that date, not today's date.
    anchor = member["nav_date"] or first_target
    return _subtract_months(min(anchor, first_target), 13)


def normalize_history(
    rows: list[dict], code: str, hk_code: str, start: date, end: date,
) -> list[dict]:
    required = {"FCODE", "HKFCODE", "PDATE", "NAV", "CURRENCY"}
    if not isinstance(rows, list):
        raise ValueError("History Data must be a list")
    normalized, seen = [], set()
    for row in rows:
        if not isinstance(row, dict) or not required.issubset(row):
            raise ValueError(f"History missing required fields for {code}")
        if str(row["FCODE"]) != code or str(row["HKFCODE"]) != hk_code:
            raise ValueError(f"History identity mismatch for {code}")
        day = _day(row["PDATE"], "history NAV date")
        if not start <= day <= end:
            raise ValueError(f"History date outside requested bounds for {code}: {day}")
        if day in seen:
            raise ValueError(f"History duplicate NAV date for {code}: {day}")
        seen.add(day)
        normalized.append({
            "fund_code": code, "hk_fund_code": hk_code, "nav_date": day,
            "unit_nav": _number(row["NAV"], "history NAV", nav=True),
            "daily_return_pct": _number(row.get("NAVCHGRT"), "history daily return"),
            "currency": str(row["CURRENCY"]), "nav_source": "upstream_history",
        })
    return sorted(normalized, key=lambda r: r["nav_date"])


def fetch_history(
    member: dict, start: date, end: date, *, get: Callable | None = None,
) -> tuple[list[dict], list[dict]]:
    """Paginate the historical endpoint completely; preserve original responses."""
    if start > end:
        raise ValueError("Invalid history window")
    get = get or requests.get
    raw_rows, evidence, total = [], [], None
    for page in range(100):
        params = {
            "api": "HKFDApi", "m": "MethodJZ", "action": "2",
            "hkfcode": member["hk_fund_code"], "pageindex": str(page),
            "pagesize": "1000", "date1": start.isoformat(), "date2": end.isoformat(),
        }
        response = get(
            HISTORY_URL, params=params,
            headers={"User-Agent": "Mozilla/5.0",
                     "Referer": "https://overseas.1234567.com.cn/"},
            timeout=20,
        )
        response.raise_for_status()
        body = response.json()
        if str(body.get("Code")) != "1" or not isinstance(body.get("Data"), list):
            raise ValueError(f"History API failed for {member['fund_code']}")
        count = body.get("TotalCount")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError(f"History API invalid TotalCount for {member['fund_code']}")
        if total is None:
            total = count
        if count != total:
            raise ValueError(f"History pagination changed for {member['fund_code']}")
        evidence.append({"request": params, "response": body})
        data = body["Data"]
        if not data and len(raw_rows) != total:
            raise ValueError(f"History pagination incomplete for {member['fund_code']}")
        raw_rows.extend(data)
        if len(raw_rows) > total:
            raise ValueError(f"History pagination overrun for {member['fund_code']}")
        if len(raw_rows) == total:
            return normalize_history(
                raw_rows, member["fund_code"], member["hk_fund_code"], start, end,
            ), evidence
    raise ValueError(f"History pagination exceeded safety limit for {member['fund_code']}")


def replay_history(
    member: dict, start: date, end: date, evidence: list[dict],
) -> list[dict]:
    """Revalidate a captured request/response set without changing its meaning."""
    class CapturedResponse:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            pass  # Only successfully parsed HTTP responses are captured.

        def json(self):
            return self.body

    consumed = []

    def get(url, *, params, **_):
        page = int(params["pageindex"])
        if page >= len(evidence) or evidence[page]["request"] != params:
            raise ValueError("Captured history request mismatch")
        consumed.append(page)
        return CapturedResponse(evidence[page]["response"])

    records, _ = fetch_history(member, start, end, get=get)
    if len(consumed) != len(evidence):
        raise ValueError("Captured history has unconsumed pages")
    return records


def seed_frozen_anchor(
    members: list[dict], histories: dict[str, list[dict]], observed: date,
) -> tuple[dict[str, list[dict]], list[str]]:
    """Retain an actual old NAV when a long-frozen fund's history was removed.

    Never fills a recent source failure, overrides a conflicting value, or
    changes the NAV date. The source observation date remains explicit.
    """
    result = {code: list(rows) for code, rows in histories.items()}
    seeded = []
    for member in members:
        code = member["fund_code"]
        if (code in result and not result[code] and member["unit_nav"] is not None
                and member["nav_date"] < observed - timedelta(days=365)):
            result[code] = [{
                "fund_code": code, "hk_fund_code": member["hk_fund_code"],
                "nav_date": member["nav_date"], "unit_nav": member["unit_nav"],
                "daily_return_pct": member["daily_return_pct"],
                "currency": member["currency"], "nav_source": "known_good_raw_snapshot",
                "source_observation_date": observed,
            }]
            seeded.append(code)
    return result, sorted(seeded)


def validate_controls(members: list[dict], histories: dict[str, list[dict]]) -> int:
    """Validate reconstructed source NAVs against the known good raw snapshot."""
    checked = 0
    for member in members:
        code = member["fund_code"]
        if code not in histories:
            raise ValueError(f"No successful history fetch for {code}")
        if member["unit_nav"] is None:
            continue
        matches = [r for r in histories[code] if r["nav_date"] == member["nav_date"]]
        if (len(matches) != 1 or matches[0]["unit_nav"] is None
                or abs(matches[0]["unit_nav"] - member["unit_nav"]) > 1e-9):
            raise ValueError(f"Historical control NAV mismatch for {code}@{member['nav_date']}")
        checked += 1
    return checked


def build_valuations(
    members: list[dict], histories: dict[str, list[dict]], targets: list[date],
) -> list[dict]:
    """Build an explicit NAV-date-cutoff panel, never an observed snapshot."""
    if not targets or len(set(targets)) != len(targets):
        raise ValueError("Empty or duplicate target dates")
    if set(histories) != {m["fund_code"] for m in members}:
        raise ValueError("Every member requires a successful history fetch")
    out = []
    for target in sorted(targets):
        for member in members:
            history = sorted(
                (r for r in histories[member["fund_code"]] if r["unit_nav"] is not None
                 and r.get("source_observation_date", target) <= target),
                key=lambda r: r["nav_date"],
            )
            dates = [r["nav_date"] for r in history]
            index = bisect_right(dates, target) - 1
            current = history[index] if index >= 0 else None
            row = {
                "fund_code": member["fund_code"], "hk_fund_code": member["hk_fund_code"],
                "fund_name": member["fund_name"], "as_of_date": target,
                "currency": current["currency"] if current else member["currency"],
                "nav_date": current["nav_date"] if current else None,
                "unit_nav": current["unit_nav"] if current else None,
                "daily_return_pct": current["daily_return_pct"] if current else None,
                "nav_age_days": (target - current["nav_date"]).days if current else None,
                "nav_status": ("same_date" if current["nav_date"] == target
                               else "carried_forward") if current else "no_reported_nav",
                "nav_source": current["nav_source"] if current else "none",
                "data_origin": "reconstructed", "as_of_basis": "nav_date_cutoff",
                "observation_time_recovered": False, "return_basis": RETURN_BASIS,
            }
            for period in ("7d", "1m", "1y"):
                baseline, change = None, None
                if current:
                    day = current["nav_date"]
                    cutoff = (day - timedelta(days=7) if period == "7d"
                              else _subtract_months(day, 1 if period == "1m" else 12))
                    base_index = bisect_right(dates, cutoff) - 1
                    if (base_index >= 0
                            and history[base_index]["currency"] == current["currency"]):
                        baseline = history[base_index]
                        change = round(float(
                            (Decimal(str(current["unit_nav"]))
                             / Decimal(str(baseline["unit_nav"])) - 1) * 100
                        ), 4)
                row[f"baseline_{period}_date"] = baseline["nav_date"] if baseline else None
                row[f"baseline_{period}_nav"] = baseline["unit_nav"] if baseline else None
                row[f"nav_change_{period}_pct"] = change
            out.append(row)
    return out
