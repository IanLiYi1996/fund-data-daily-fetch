"""Tests for BaseFetcher.dual_write — raw + Iceberg with error isolation."""
from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from shared.fetchers.base_fetcher import BaseFetcher, FetchResult


class _Stub(BaseFetcher):
    @property
    def category(self):
        return "fund"

    def fetch_all(self):
        raise NotImplementedError


@pytest.fixture
def fetcher():
    return _Stub()


@pytest.fixture
def df():
    return pd.DataFrame({"a": [1, 2]})


def test_dual_write_both_succeed(fetcher, df):
    s3 = MagicMock()
    s3.upload_dataframe.return_value = {"key": "fund/...", "rows": 2, "size": 100}
    iceberg = MagicMock()
    iceberg.write.return_value = {"rows_inserted": 2, "rows_updated": 0}
    result = FetchResult(name="fund_daily", data=df, success=True)

    out = fetcher.dual_write(result, s3, iceberg, category="fund")

    assert out["raw"]["rows"] == 2
    assert out["iceberg"]["rows_inserted"] == 2
    s3.upload_dataframe.assert_called_once()
    iceberg.write.assert_called_once_with("fund_daily", df)


def test_iceberg_failure_does_not_block_raw(fetcher, df):
    s3 = MagicMock()
    s3.upload_dataframe.return_value = {"key": "fund/...", "rows": 2}
    iceberg = MagicMock()
    iceberg.write.side_effect = RuntimeError("Glue throttle")
    result = FetchResult(name="fund_daily", data=df, success=True)

    out = fetcher.dual_write(result, s3, iceberg, category="fund")

    assert out["raw"]["rows"] == 2
    assert "error" in out["iceberg"]
    assert "Glue throttle" in out["iceberg"]["error"]


def test_raw_failure_propagates_but_iceberg_still_attempts(fetcher, df):
    s3 = MagicMock()
    s3.upload_dataframe.side_effect = RuntimeError("S3 throttle")
    iceberg = MagicMock()
    iceberg.write.return_value = {"rows_inserted": 2}
    result = FetchResult(name="fund_daily", data=df, success=True)

    out = fetcher.dual_write(result, s3, iceberg, category="fund")

    assert "error" in out["raw"]
    assert out["iceberg"]["rows_inserted"] == 2


def test_unsuccessful_fetch_skips_both(fetcher):
    s3 = MagicMock()
    iceberg = MagicMock()
    result = FetchResult(name="fund_daily", success=False, error="akshare timeout")

    out = fetcher.dual_write(result, s3, iceberg, category="fund")

    assert out == {"raw": None, "iceberg": None, "skipped": True}
    s3.upload_dataframe.assert_not_called()
    iceberg.write.assert_not_called()


def test_unknown_table_in_iceberg_recorded_as_error(fetcher, df):
    """Iceberg writer raising KeyError for unregistered tables is isolated."""
    s3 = MagicMock()
    s3.upload_dataframe.return_value = {"key": "...", "rows": 2}
    iceberg = MagicMock()
    iceberg.write.side_effect = KeyError("unregistered_table")
    result = FetchResult(name="unregistered_table", data=df, success=True)

    out = fetcher.dual_write(result, s3, iceberg, category="fund")

    assert "error" in out["iceberg"]
    assert "unregistered_table" in out["iceberg"]["error"]
