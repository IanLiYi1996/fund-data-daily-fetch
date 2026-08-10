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
    """Upsert semantics, exercised on an actually-upsert table.

    These used to write to fund_daily, which moved to append mode in bb1fdbf
    (pyiceberg's upsert reads the whole table to dedup, and at ~30M rows it
    blew Lambda's 900s timeout). The tests kept asserting `rows_inserted` and
    so had been failing rather than testing upsert at all. fund_name is a real
    upsert table keyed on (fund_code, snapshot_date).
    """

    def test_first_write_inserts_all_rows(self, writer):
        df = pd.DataFrame({
            "基金代码": ["000001", "000002"],
            "基金简称": ["A", "B"],
            "基金类型": ["混合型-灵活", "债券型-长债"],
            "数据日期": ["2026-05-09", "2026-05-09"],
        })
        result = writer.write("fund_name", df)
        assert result["rows_inserted"] == 2
        assert result.get("rows_updated", 0) == 0

    def test_second_write_same_pk_updates(self, writer):
        base = {"基金代码": ["000001"], "数据日期": ["2026-05-09"]}
        writer.write("fund_name", pd.DataFrame(
            {**base, "基金简称": ["A"], "基金类型": ["混合型-灵活"]}))
        result = writer.write("fund_name", pd.DataFrame(
            {**base, "基金简称": ["A 改名"], "基金类型": ["混合型-偏股"]}))
        assert result["rows_updated"] == 1
        assert result["rows_inserted"] == 0

    def test_empty_dataframe_returns_skipped(self, writer):
        result = writer.write("fund_daily", pd.DataFrame())
        assert result == {"skipped": True, "reason": "empty"}

    def test_drops_internal_duplicates_keep_last(self, writer):
        df = pd.DataFrame({
            "基金代码": ["000001", "000001"],
            "基金简称": ["旧名", "新名"],
            "基金类型": ["混合型-灵活", "混合型-偏股"],
            "数据日期": ["2026-05-09", "2026-05-09"],
        })
        result = writer.write("fund_name", df)
        assert result["rows_inserted"] == 1
        out = writer.catalog.load_table(
            ("fund_data_lake", "fund_name")
        ).scan().to_pandas()
        assert out["fund_name"].iloc[0] == "新名"

    def test_fund_daily_appends_rather_than_upserting(self, writer):
        """Pins the append switch that these tests previously contradicted.

        fund_daily must NOT report upsert counters: the fallback fetcher adds
        rows for codes the main snapshot missed, and weekly compaction dedups
        on (fund_code, trade_date) instead.
        """
        df = pd.DataFrame({
            "基金代码": ["000001"], "基金简称": ["A"],
            "净值日期": ["2026-05-09"], "单位净值": [1.0],
            "累计净值": [1.0], "日增长率": [0.0],
            "申购状态": ["开放"], "赎回状态": ["开放"], "手续费": ["0%"],
        })
        result = writer.write("fund_daily", df)
        assert result["rows_appended"] == 1
        assert "rows_updated" not in result


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
