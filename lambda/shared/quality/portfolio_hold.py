"""Read Eastmoney's public stock-holding disclosures without executing JS."""
from __future__ import annotations

import calendar
import math
import re
from datetime import date

import pandas as pd
import requests
from akshare.utils import demjson
from bs4 import BeautifulSoup

FIELDS = [
    "fund_code", "report_date", "holding_code", "holding_name",
    "weight_pct", "shares", "market_value",
]
_WRAPPER = re.compile(r"\A\s*var\s+apidata\s*=\s*(\{.*\})\s*;?\s*\Z", re.S)
_QUARTER = re.compile(r"(\d{4})年([1-4])季度")
_COLUMNS = {
    "股票代码": "holding_code", "股票名称": "holding_name",
    "占净值比例": "weight_pct", "持股数（万股）": "shares",
    "持仓市值（万元）": "market_value",
    "持仓市值（万元人民币）": "market_value",
}


def quarter_end(year: int, quarter: int) -> date:
    month = quarter * 3
    return date(year, month, calendar.monthrange(year, month)[1])


def _number(text: str, *, percent: bool = False):
    value = text.strip().replace(",", "")
    if value in ("", "-", "--", "---", "—"):
        return None
    if percent:
        value = value.removesuffix("%")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError("invalid non-finite or negative holding value")
    if percent and number > 100:
        raise ValueError("holding weight exceeds 100%")
    return number


def parse_holdings(
    text: str, code: str, year: int, *, as_of: date | None = None,
    target_quarter: date | None = None,
) -> pd.DataFrame:
    match = _WRAPPER.fullmatch(text)
    if not match:
        raise ValueError("unexpected holdings response (not apidata)")
    payload = demjson.decode(match.group(1))
    content = payload.get("content")
    if not isinstance(content, str):
        raise ValueError("holdings response has no content string")
    if not content.strip():
        return pd.DataFrame(columns=FIELDS)
    soup = BeautifulSoup(content, "lxml")
    headings = soup.select("h4.t")
    if not headings:
        if "暂无数据" in soup.get_text() or "暂无股票投资明细" in soup.get_text():
            return pd.DataFrame(columns=FIELDS)
        raise ValueError("holdings response has no quarterly disclosure")
    rows = []
    observation_cutoffs = {}
    for heading in headings:
        title = heading.get_text(" ", strip=True)
        quarter = _QUARTER.search(title)
        if not quarter:
            raise ValueError("unrecognized holding quarter")
        report_date = quarter_end(int(quarter[1]), int(quarter[2]))
        if report_date.year != year:
            raise ValueError("holding disclosure year differs from request")
        if report_date > (as_of or date.today()):
            continue
        if target_quarter is not None and report_date != target_quarter:
            continue
        cutoff = re.search(r"截止至[：:]\s*(\d{4}-\d{2}-\d{2})", title)
        if cutoff:
            observed = date.fromisoformat(cutoff[1])
            if (observed.year, (observed.month - 1) // 3) != (
                report_date.year, (report_date.month - 1) // 3,
            ) or observed > (as_of or date.today()):
                raise ValueError("holding cutoff differs from quarter")
            # report_date identifies the disclosure period, not the actual
            # observation day. Retain that day as parser provenance.
            observation_cutoffs[report_date.isoformat()] = observed.isoformat()
        table = heading.parent.find("table")
        if table is None:
            raise ValueError("holding disclosure has no table")
        columns = [
            _COLUMNS.get(re.sub(r"\s+", "", cell.get_text("", strip=True)))
            for cell in table.select("thead th")
        ]
        if not set(FIELDS[2:]).issubset(set(columns)):
            raise ValueError("holding table missing required column")
        for tr in table.select("tbody tr"):
            cells = tr.find_all("td", recursive=False)
            if len(cells) != len(columns):
                raise ValueError("holding row does not match header")
            row = {"fund_code": code, "report_date": report_date}
            for column, cell in zip(columns, cells):
                if column:
                    row[column] = cell.get_text("", strip=True)
            if not row["holding_code"] or not row["holding_name"]:
                raise ValueError("holding row has empty stock identity")
            row["weight_pct"] = _number(row["weight_pct"], percent=True)
            for field in ("shares", "market_value"):
                value = _number(row[field])
                row[field] = None if value is None else value * 10000
            rows.append(row)
    frame = pd.DataFrame(rows, columns=FIELDS)
    keys = ["fund_code", "report_date", "holding_code"]
    if frame.duplicated(keys).any():
        if frame.drop_duplicates().duplicated(keys).any():
            raise ValueError("conflicting duplicate holding rows")
        frame = frame.drop_duplicates(keys)
    frame.attrs["observation_cutoffs"] = observation_cutoffs
    return frame


def fetch_holdings(
    code: str, year: int, *, get=None, target_quarter: date | None = None,
) -> pd.DataFrame:
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("invalid fund code")
    response = (get or requests.get)(
        "https://fundf10.eastmoney.com/FundArchivesDatas.aspx",
        params={
            "type": "jjcc", "code": code, "topline": "10000",
            "year": str(year), "month": "", "rt": "0.913877030254846",
        },
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": f"https://fundf10.eastmoney.com/ccmx_{code}.html",
        },
        timeout=(20, 45),
    )
    response.raise_for_status()
    return parse_holdings(response.text, code, year, target_quarter=target_quarter)
