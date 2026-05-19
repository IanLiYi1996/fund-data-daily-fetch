"""Tests for shared.schemas.normalizers."""
from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from shared.schemas.normalizers import (
    DateColumnSpec,
    coerce_date_column,
    normalize,
)


def _spec(source: str = "净值日期", target: str = "trade_date", dtype: str = "date"):
    return DateColumnSpec(source_candidates=[source], target=target, dtype=dtype)


class TestCoerceDateColumn:
    def test_iso_date_string(self):
        s = pd.Series(["2026-05-09", "2026-05-10"])
        out = coerce_date_column(s, dtype="date")
        assert list(out) == [date(2026, 5, 9), date(2026, 5, 10)]

    def test_compact_date_string(self):
        s = pd.Series(["20260509", "20260510"])
        out = coerce_date_column(s, dtype="date")
        assert list(out) == [date(2026, 5, 9), date(2026, 5, 10)]

    def test_slash_date_string(self):
        s = pd.Series(["2026/5/9", "2026/05/10"])
        out = coerce_date_column(s, dtype="date")
        assert list(out) == [date(2026, 5, 9), date(2026, 5, 10)]

    def test_unparseable_becomes_nat(self):
        s = pd.Series(["2026-05-09", "not-a-date"])
        out = coerce_date_column(s, dtype="date")
        assert out.iloc[0] == date(2026, 5, 9)
        assert pd.isna(out.iloc[1])

    def test_timestamp_dtype(self):
        s = pd.Series(["2026-05-09 14:30:00"])
        out = coerce_date_column(s, dtype="timestamp")
        assert out.iloc[0] == datetime(2026, 5, 9, 14, 30, 0)


class TestNormalize:
    def test_renames_chinese_to_target(self):
        df = pd.DataFrame({"净值日期": ["2026-05-09"], "v": [1]})
        out = normalize(df, date_specs=[_spec()])
        assert "trade_date" in out.columns
        assert out["trade_date"].iloc[0] == date(2026, 5, 9)

    def test_drops_rows_with_unparseable_date(self):
        df = pd.DataFrame({"净值日期": ["2026-05-09", "garbage"], "v": [1, 2]})
        out = normalize(df, date_specs=[_spec()])
        assert len(out) == 1
        assert out["v"].iloc[0] == 1

    def test_pass_through_when_target_already_present(self):
        df = pd.DataFrame({"trade_date": [date(2026, 5, 9)], "v": [1]})
        out = normalize(df, date_specs=[_spec()])
        assert len(out) == 1

    def test_falls_back_to_provided_date_when_column_missing(self):
        df = pd.DataFrame({"v": [1, 2]})
        out = normalize(
            df, date_specs=[_spec()], fallback_date=date(2026, 5, 9)
        )
        assert (out["trade_date"] == date(2026, 5, 9)).all()

    def test_missing_column_no_fallback_raises(self):
        df = pd.DataFrame({"v": [1]})
        with pytest.raises(KeyError, match="trade_date"):
            normalize(df, date_specs=[_spec()])

    def test_first_matching_candidate_wins(self):
        df = pd.DataFrame({"数据日期": ["2026-05-09"], "v": [1]})
        spec = DateColumnSpec(
            source_candidates=["净值日期", "数据日期", "交易日"],
            target="trade_date",
            dtype="date",
        )
        out = normalize(df, date_specs=[spec])
        assert out["trade_date"].iloc[0] == date(2026, 5, 9)
