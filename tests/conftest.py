"""Shared pytest fixtures for fund-data-daily-fetch tests."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Iterator

import pandas as pd
import pytest


@pytest.fixture
def tmp_warehouse(tmp_path: Path) -> Iterator[str]:
    """Filesystem warehouse path for an in-memory/SQL Iceberg catalog."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    yield f"file://{warehouse}"


@pytest.fixture
def sample_fund_daily_df() -> pd.DataFrame:
    """Minimal sample of akshare fund_open_fund_daily_em output."""
    return pd.DataFrame(
        {
            "基金代码": ["000001", "000002"],
            "基金简称": ["华夏成长", "华夏成长A"],
            "净值日期": ["2026-05-09", "2026-05-09"],
            "单位净值": [1.234, 2.345],
            "累计净值": [3.456, 4.567],
            "日增长率": [0.5, -0.3],
            "申购状态": ["开放申购", "开放申购"],
            "赎回状态": ["开放赎回", "开放赎回"],
            "手续费": ["0.15%", "0.15%"],
        }
    )
