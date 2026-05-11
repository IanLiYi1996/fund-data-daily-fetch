"""Iceberg TableSpec registry for fund_data_lake.

This file declares a single source of truth for every Iceberg table:
its schema, partition spec, primary key (identifier fields), and
how new data should be merged in (upsert vs append).
"""
from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform, YearTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    NestedField,
    StringType,
    TimestampType,
)

from shared.schemas.normalizers import DateColumnSpec


@dataclass(frozen=True)
class TableSpec:
    """Single source of truth for an Iceberg table."""

    name: str
    schema: Schema
    partition_spec: PartitionSpec
    identifier_fields: list[str]
    write_mode: str  # "upsert" | "append"
    source_category: str  # "fund" | "kline_a" | "macro_cn" ...
    date_specs: list[DateColumnSpec]


# ---------- fund_daily (open-end fund daily NAV; upsert by month) ----------
_fund_daily_schema = Schema(
    NestedField(1, "fund_code", StringType(), required=True),
    NestedField(2, "fund_name", StringType()),
    NestedField(3, "trade_date", DateType(), required=True),
    NestedField(4, "unit_nav", DoubleType()),
    NestedField(5, "accum_nav", DoubleType()),
    NestedField(6, "daily_return_pct", DoubleType()),
    NestedField(7, "subscription_status", StringType()),
    NestedField(8, "redemption_status", StringType()),
    NestedField(9, "fee", StringType()),
    identifier_field_ids=[1, 3],
)
_fund_daily_partition = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=MonthTransform(), name="trade_month")
)

# ---------- fund_name (fund metadata SCD-1; upsert by year) ----------
_fund_name_schema = Schema(
    NestedField(1, "fund_code", StringType(), required=True),
    NestedField(2, "fund_name", StringType()),
    NestedField(3, "fund_type", StringType()),
    NestedField(4, "snapshot_date", DateType(), required=True),
    identifier_field_ids=[1, 4],
)
_fund_name_partition = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=YearTransform(), name="snapshot_year")
)

# ---------- fund_dividend (event-driven; append by year) ----------
_fund_dividend_schema = Schema(
    NestedField(1, "fund_code", StringType(), required=True),
    NestedField(2, "fund_name", StringType()),
    NestedField(3, "dividend_amount", DoubleType()),
    NestedField(4, "event_date", DateType(), required=True),
    NestedField(5, "payment_date", DateType()),
    identifier_field_ids=[1, 4],
)
_fund_dividend_partition = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=YearTransform(), name="event_year")
)


TABLES: dict[str, TableSpec] = {
    "fund_daily": TableSpec(
        name="fund_daily",
        schema=_fund_daily_schema,
        partition_spec=_fund_daily_partition,
        identifier_fields=["fund_code", "trade_date"],
        write_mode="upsert",
        source_category="fund",
        date_specs=[
            DateColumnSpec(["净值日期", "交易日"], "trade_date", "date"),
        ],
    ),
    "fund_name": TableSpec(
        name="fund_name",
        schema=_fund_name_schema,
        partition_spec=_fund_name_partition,
        identifier_fields=["fund_code", "snapshot_date"],
        write_mode="upsert",
        source_category="fund",
        date_specs=[
            DateColumnSpec(["数据日期", "snapshot_date"], "snapshot_date", "date"),
        ],
    ),
    "fund_dividend": TableSpec(
        name="fund_dividend",
        schema=_fund_dividend_schema,
        partition_spec=_fund_dividend_partition,
        identifier_fields=["fund_code", "event_date"],
        write_mode="append",
        source_category="fund",
        date_specs=[
            DateColumnSpec(["除息日"], "event_date", "date"),
            DateColumnSpec(["发放日"], "payment_date", "date"),
        ],
    ),
}
