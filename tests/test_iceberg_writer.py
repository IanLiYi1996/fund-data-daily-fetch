"""Tests for shared.storage.iceberg_writer.IcebergWriter."""
from __future__ import annotations

from datetime import date

import pandas as pd
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from shared.schemas.registry import TABLES
from shared.storage.iceberg_writer import IcebergWriter


@pytest.fixture
def catalog(tmp_path):
    """Local SQL+filesystem Iceberg catalog for tests."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    cat = SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    cat.create_namespace("fund_data_lake")
    return cat


@pytest.fixture
def writer(catalog):
    return IcebergWriter(catalog=catalog, database="fund_data_lake")


class TestUpsert:
    def test_first_write_inserts_all_rows(self, writer):
        df = pd.DataFrame({
            "基金代码": ["000001", "000002"],
            "基金简称": ["A", "B"],
            "净值日期": ["2026-05-09", "2026-05-09"],
            "单位净值": [1.0, 2.0],
            "累计净值": [1.0, 2.0],
            "日增长率": [0.0, 0.0],
            "申购状态": ["开放", "开放"],
            "赎回状态": ["开放", "开放"],
            "手续费": ["0%", "0%"],
        })
        result = writer.write("fund_daily", df)
        assert result["rows_inserted"] == 2
        assert result.get("rows_updated", 0) == 0

    def test_second_write_same_pk_updates(self, writer):
        df1 = pd.DataFrame({"基金代码": ["000001"], "基金简称": ["A"],
                            "净值日期": ["2026-05-09"], "单位净值": [1.0],
                            "累计净值": [1.0], "日增长率": [0.0],
                            "申购状态": ["开放"], "赎回状态": ["开放"], "手续费": ["0%"]})
        df2 = pd.DataFrame({"基金代码": ["000001"], "基金简称": ["A"],
                            "净值日期": ["2026-05-09"], "单位净值": [9.99],
                            "累计净值": [9.99], "日增长率": [0.0],
                            "申购状态": ["开放"], "赎回状态": ["开放"], "手续费": ["0%"]})
        writer.write("fund_daily", df1)
        result = writer.write("fund_daily", df2)
        assert result["rows_updated"] == 1
        assert result["rows_inserted"] == 0

    def test_empty_dataframe_returns_skipped(self, writer):
        result = writer.write("fund_daily", pd.DataFrame())
        assert result == {"skipped": True, "reason": "empty"}

    def test_drops_internal_duplicates_keep_last(self, writer):
        df = pd.DataFrame({
            "基金代码": ["000001", "000001"],
            "基金简称": ["A", "A"],
            "净值日期": ["2026-05-09", "2026-05-09"],
            "单位净值": [1.0, 2.0],
            "累计净值": [1.0, 2.0],
            "日增长率": [0.0, 0.0],
            "申购状态": ["开放", "开放"],
            "赎回状态": ["开放", "开放"],
            "手续费": ["0%", "0%"],
        })
        result = writer.write("fund_daily", df)
        assert result["rows_inserted"] == 1
        table = writer.catalog.load_table(("fund_data_lake", "fund_daily"))
        out = table.scan().to_pandas()
        assert out["unit_nav"].iloc[0] == 2.0


class TestAppendMode:
    def test_event_table_appends(self, writer):
        df = pd.DataFrame({
            "基金代码": ["000001"],
            "基金简称": ["A"],
            "除息日": ["2026-05-09"],
            "发放日": ["2026-05-10"],
            "分红金额": [0.5],
        })
        result = writer.write("fund_dividend", df)
        # append mode returns rows_appended; at least the one row should land
        assert result.get("rows_appended", 0) >= 1


class TestCreateIfNotExists:
    def test_table_created_on_first_write(self, writer):
        assert ("fund_data_lake", "fund_daily") not in writer.catalog.list_tables("fund_data_lake")
        df = pd.DataFrame({
            "基金代码": ["000001"], "基金简称": ["A"],
            "净值日期": ["2026-05-09"], "单位净值": [1.0],
            "累计净值": [1.0], "日增长率": [0.0],
            "申购状态": ["开放"], "赎回状态": ["开放"], "手续费": ["0%"],
        })
        writer.write("fund_daily", df)
        assert ("fund_data_lake", "fund_daily") in writer.catalog.list_tables("fund_data_lake")
