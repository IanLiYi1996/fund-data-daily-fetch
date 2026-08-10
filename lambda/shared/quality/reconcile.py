"""Sampled reconciliation of stored NAVs against upstream.

Every other check inspects our own output, so a transform bug is invisible
to all of them. Two shipped because of that:

- Weekend phantom rows: the writer stamped rows with the run date instead of
  the date upstream labelled the values with, fabricating ~23.6k rows every
  Sat/Sun for two months. Freshness, coverage and replication all passed —
  the rows existed and were recent, the DATES were invented.
- Stale export: the export globbed the Iceberg data directory and served
  superseded files, sending Friday's NAV out as Monday's.

Comparing values AND the set of dates back to source catches both. A date
present on one side only is never acceptable.

Scope: our NAVs originate from 天天基金, so this validates the path from
upstream to consumer rather than upstream's own correctness. That path is
exactly where both failures occurred.
"""
from __future__ import annotations

import json
import os
import random
import urllib.request
from datetime import date, timedelta
from typing import Dict, List, Optional

from pyiceberg.expressions import GreaterThanOrEqual

from shared.utils.logger import get_logger

logger = get_logger(__name__)

DATABASE = "fund_data_lake"
SAMPLE_SIZE = int(os.environ.get("RECONCILE_SAMPLE_SIZE", "60"))
COMPARE_DAYS = int(os.environ.get("RECONCILE_COMPARE_DAYS", "5"))
# NAVs are published to 4 decimals; larger gaps are real disagreements.
NAV_TOLERANCE = float(os.environ.get("RECONCILE_NAV_TOLERANCE", "0.0001"))

_HEADERS = {
    "Referer": "https://fundf10.eastmoney.com/",
    "User-Agent": "Mozilla/5.0",
}

# Sample across fund types, not just the big open-end names. Both shipped
# bugs hit money-market and ETF rows hardest, and a naive random sample is
# ~85% plain open-end funds — it would likely have missed them.
#
# Keyed on fund_name.fund_type, NOT name keywords. Matching names misfires:
# "现金" hits 华夏国证自由现金流ETF (an equity ETF) and "增利" hits
# 华泰柏瑞稳本增利债券 (a bond fund), which mislabelled 252 funds as
# money-market and made a weekend cleanup skip them.
_STRATA_PREFIXES = {
    "money_market": ("货币型",),
    "index_etf": ("指数型",),
    "fof": ("FOF",),
    "qdii": ("QDII",),
    "bond": ("债券型",),
    "equity": ("股票型", "混合型"),
}


def _stratum(fund_type: str) -> str:
    t = fund_type or ""
    for label, prefixes in _STRATA_PREFIXES.items():
        if any(t.startswith(x) for x in prefixes):
            return label
    return "other"


def _upstream(code: str, lo: date, hi: date) -> Optional[Dict[date, float]]:
    """NAV by date from lsjz, or None if the call itself failed.

    None and {} mean different things: a transport failure must not be
    reported as "upstream has no data", or a flaky network would look like
    a pile of data defects.
    """
    url = (f"https://api.fund.eastmoney.com/f10/lsjz?fundCode={code}"
           f"&pageIndex=1&pageSize=40&startDate={lo}&endDate={hi}")
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as r:
            rows = json.loads(r.read()).get("Data", {}).get("LSJZList") or []
    except Exception as e:
        logger.warning(f"{code}: upstream error {type(e).__name__}")
        return None
    out: Dict[date, float] = {}
    for x in rows:
        raw = (x.get("DWJZ") or "").strip()
        if not raw:
            continue
        try:
            out[date.fromisoformat(x["FSRQ"])] = float(raw)
        except (ValueError, KeyError):
            continue
    return out


def _build_sample(catalog, rng: random.Random) -> List[tuple]:
    """Stratified sample of (code, name, stratum), rotated by RNG seed."""
    fn = catalog.load_table((DATABASE, "fund_name")).scan(
        selected_fields=("fund_code", "fund_name", "fund_type", "snapshot_date"),
    ).to_arrow().to_pandas()
    if fn.empty:
        return []
    fn = fn[fn.snapshot_date == fn.snapshot_date.max()].drop_duplicates("fund_code")

    buckets: Dict[str, list] = {}
    for code, name, ftype in zip(fn.fund_code, fn.fund_name, fn.fund_type):
        buckets.setdefault(_stratum(ftype), []).append((code, name))

    # Even split across strata so small-but-risky types are always represented.
    per = max(1, SAMPLE_SIZE // max(1, len(buckets)))
    sample: List[tuple] = []
    for label, items in sorted(buckets.items()):
        picked = rng.sample(items, min(per, len(items)))
        sample.extend((c, n, label) for c, n in picked)
    return sample[:SAMPLE_SIZE]




def _latest_loaded_date(catalog) -> Optional[date]:
    """Newest trade_date present in fund_daily.

    Same reasoning as the fallback fetcher's copy: collection runs at 17:00
    UTC, so between 00:00 and 17:00 "today" holds nothing and using it as the
    upper bound makes a full day look missing.
    """
    since = date.today() - timedelta(days=10)
    arrow = catalog.load_table((DATABASE, "fund_daily")).scan(
        row_filter=GreaterThanOrEqual("trade_date", since),
        selected_fields=("trade_date",),
    ).to_arrow()
    days = arrow["trade_date"].to_pylist()
    return max(days) if days else None


def reconcile(catalog, seed: Optional[int] = None) -> Dict:
    """Diff a stratified sample against upstream. Returns a summary dict."""
    rng = random.Random(seed if seed is not None else date.today().toordinal())
    sample = _build_sample(catalog, rng)
    if not sample:
        return {"checked": 0, "problems": 0, "note": "empty sample"}

    # Bound the window by what we have actually loaded, not by the wall clock.
    # date.today() guaranteed a false alarm on every run before the 17:00 UTC
    # collection: on 2026-08-10 the check reported "144 dates missing (upstream
    # has them)" and agreement fell to 30/160, when in fact every one of those
    # dates was today's not-yet-collected data. Comparing against a day we
    # never claimed to have is not a defect signal.
    hi = _latest_loaded_date(catalog) or date.today()
    lo = hi - timedelta(days=COMPARE_DAYS + 4)  # pad for weekends
    codes = {c for c, _, _ in sample}

    # Snapshot read, never a directory glob — otherwise this inherits the
    # very defect it exists to detect.
    arrow = catalog.load_table((DATABASE, "fund_daily")).scan(
        row_filter=GreaterThanOrEqual("trade_date", lo),
        selected_fields=("fund_code", "trade_date", "unit_nav"),
    ).to_arrow()
    mine: Dict[str, Dict[date, float]] = {}
    for c, d, v in zip(arrow["fund_code"].to_pylist(),
                       arrow["trade_date"].to_pylist(),
                       arrow["unit_nav"].to_pylist()):
        if c in codes and v is not None:
            mine.setdefault(c, {})[d] = v

    extra: List[str] = []
    missing: List[str] = []
    diffs: List[str] = []
    checked = probe_errors = passed = 0

    for code, _name, _stratum in sample:
        up = _upstream(code, lo, hi)
        if up is None:
            probe_errors += 1
            continue
        ours = mine.get(code, {})
        if not ours and not up:
            continue
        checked += 1
        ok = True
        for d in sorted(set(ours) - set(up)):
            extra.append(f"{code}@{d}")
            ok = False
        for d in sorted(set(up) - set(ours)):
            missing.append(f"{code}@{d}")
            ok = False
        for d in sorted(set(ours) & set(up)):
            if abs(ours[d] - up[d]) > NAV_TOLERANCE:
                diffs.append(f"{code}@{d} 我们={ours[d]} 上游={up[d]}")
                ok = False
        passed += int(ok)

    return {
        "checked": checked,
        "passed": passed,
        "probe_errors": probe_errors,
        "problems": len(extra) + len(missing) + len(diffs),
        "extra_dates": extra,
        "missing_dates": missing,
        "value_diffs": diffs,
    }
