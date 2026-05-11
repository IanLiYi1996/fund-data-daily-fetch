"""Normalize akshare DataFrames into canonical Iceberg-friendly schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Iterable, Literal, Optional

import pandas as pd

DateDtype = Literal["date", "timestamp"]


@dataclass
class DateColumnSpec:
    """Maps one or more akshare source columns to a canonical date/time column."""

    source_candidates: list[str]
    target: str
    dtype: DateDtype = "date"


_DATE_FORMATS = ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d", "%Y/%m/%-d", "%Y-%m-%-d"]


def coerce_date_column(s: pd.Series, dtype: DateDtype) -> pd.Series:
    """Coerce a series to date or timestamp; unparseable values become NaT."""
    parsed = pd.to_datetime(s, errors="coerce", format="mixed")
    if dtype == "date":
        return parsed.dt.date.where(parsed.notna(), other=pd.NaT)
    return parsed


def normalize(
    df: pd.DataFrame,
    date_specs: Iterable[DateColumnSpec],
    fallback_date: Optional[date] = None,
) -> pd.DataFrame:
    """Rename + coerce date columns; drop rows where required date is NaT."""
    out = df.copy()
    for spec in date_specs:
        if spec.target in out.columns:
            out[spec.target] = coerce_date_column(out[spec.target], spec.dtype)
            continue
        source = next(
            (c for c in spec.source_candidates if c in out.columns), None
        )
        if source is None:
            if fallback_date is not None:
                out[spec.target] = fallback_date
                continue
            raise KeyError(
                f"None of {spec.source_candidates} present in DataFrame; "
                f"cannot populate {spec.target!r}"
            )
        out[spec.target] = coerce_date_column(out[source], spec.dtype)
    # Drop rows where any target date is null (unparseable rows)
    target_cols = [s.target for s in date_specs]
    if target_cols:
        out = out.dropna(subset=target_cols).reset_index(drop=True)
    return out
