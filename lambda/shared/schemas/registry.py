"""Iceberg TableSpec registry for fund_data_lake.

This file declares a single source of truth for every Iceberg table:
its schema, partition spec, primary key (identifier fields), and
how new data should be merged in (upsert vs append).
"""
from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    DayTransform,
    IdentityTransform,
    MonthTransform,
    YearTransform,
)
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


# Helper: build a simple TableSpec quickly (only for tables matching the
# common pattern: fund_code PK + a date PK + N data columns).
def _simple_daily(
    name: str,
    extra_data_columns: list[tuple[str, type]],
    partition_transform=MonthTransform(),
    pk_date_field: str = "trade_date",
    pk_date_aliases: tuple[str, ...] = ("净值日期", "交易日", "数据日期"),
    write_mode: str = "upsert",
    extra_pk: tuple[str, ...] = (),
) -> TableSpec:
    """Build a TableSpec with (fund_code, <date>) PK plus data columns."""
    fields = [
        NestedField(1, "fund_code", StringType(), required=True),
        NestedField(2, "fund_name", StringType()),
        NestedField(3, pk_date_field, DateType(), required=True),
    ]
    next_id = 4
    for col_name, col_type in extra_data_columns:
        fields.append(NestedField(next_id, col_name, col_type()))
        next_id += 1
    extra_pk_ids: list[int] = []
    for extra in extra_pk:
        fields.append(NestedField(next_id, extra, StringType(), required=True))
        extra_pk_ids.append(next_id)
        next_id += 1
    schema = Schema(*fields, identifier_field_ids=[1, 3, *extra_pk_ids])
    partition = PartitionSpec(
        PartitionField(
            source_id=3, field_id=1000, transform=partition_transform,
            name=f"{pk_date_field}_part",
        )
    )
    pk_names = ["fund_code", pk_date_field, *extra_pk]
    return TableSpec(
        name=name,
        schema=schema,
        partition_spec=partition,
        identifier_fields=pk_names,
        write_mode=write_mode,
        source_category="fund",
        date_specs=[DateColumnSpec(list(pk_date_aliases), pk_date_field, "date")],
    )


# Domain 1: high-frequency daily (7 more — fund_daily already defined above)
TABLES["fund_etf"] = _simple_daily(
    "fund_etf",
    [("latest_price", DoubleType), ("change_pct", DoubleType),
     ("volume", DoubleType), ("turnover", DoubleType)],
    pk_date_aliases=("数据日期", "交易日"),
)
TABLES["fund_lof"] = _simple_daily(
    "fund_lof",
    [("latest_price", DoubleType), ("change_pct", DoubleType),
     ("unit_nav", DoubleType), ("premium_pct", DoubleType)],
    pk_date_aliases=("数据日期", "交易日"),
)
TABLES["fund_money_daily"] = _simple_daily(
    "fund_money_daily",
    [("ten_thousand_yield", DoubleType), ("annual_yield_7d", DoubleType)],
)
TABLES["fund_financial_daily"] = _simple_daily(
    "fund_financial_daily",
    [("ten_thousand_yield", DoubleType), ("annual_yield_7d", DoubleType)],
)
TABLES["fund_etf_daily"] = _simple_daily(
    "fund_etf_daily",
    [("unit_nav", DoubleType), ("accum_nav", DoubleType),
     ("market_price", DoubleType), ("premium_pct", DoubleType)],
)
TABLES["fund_graded_daily"] = _simple_daily(
    "fund_graded_daily",
    [("unit_nav", DoubleType), ("premium_pct", DoubleType)],
)
TABLES["fund_value_estimation"] = TableSpec(
    name="fund_value_estimation",
    schema=Schema(
        NestedField(1, "fund_code", StringType(), required=True),
        NestedField(2, "fund_name", StringType()),
        NestedField(3, "estimated_nav", DoubleType()),
        NestedField(4, "estimated_change_pct", DoubleType()),
        NestedField(5, "snapshot_time", TimestampType(), required=True),
        identifier_field_ids=[1, 5],
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="snapshot_day")
    ),
    identifier_fields=["fund_code", "snapshot_time"],
    write_mode="upsert",
    source_category="fund",
    date_specs=[DateColumnSpec(["估算时间"], "snapshot_time", "timestamp")],
)

# Domain 2: closed / REITs / FOF (new)
TABLES["fund_close_daily"] = _simple_daily(
    "fund_close_daily",
    [("latest_price", DoubleType), ("unit_nav", DoubleType),
     ("premium_pct", DoubleType)],
)
TABLES["fund_fof_daily"] = _simple_daily(
    "fund_fof_daily",
    [("unit_nav", DoubleType), ("accum_nav", DoubleType),
     ("daily_return_pct", DoubleType)],
)
TABLES["fund_reits_daily"] = _simple_daily(
    "fund_reits_daily",
    [("latest_price", DoubleType), ("change_pct", DoubleType),
     ("volume", DoubleType), ("turnover", DoubleType)],
    pk_date_aliases=("数据日期", "交易日"),
)


# Domain 3: rankings / ratings — date is the snapshot_date
def _ranking(name: str) -> TableSpec:
    return TableSpec(
        name=name,
        schema=Schema(
            NestedField(1, "fund_code", StringType(), required=True),
            NestedField(2, "fund_name", StringType()),
            NestedField(3, "snapshot_date", DateType(), required=True),
            NestedField(4, "unit_nav", DoubleType()),
            NestedField(5, "weekly_return", DoubleType()),
            NestedField(6, "monthly_return", DoubleType()),
            NestedField(7, "yearly_return", DoubleType()),
            NestedField(8, "fee", StringType()),
            identifier_field_ids=[1, 3],
        ),
        partition_spec=PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=MonthTransform(), name="snapshot_month")
        ),
        identifier_fields=["fund_code", "snapshot_date"],
        write_mode="upsert",
        source_category="fund",
        date_specs=[DateColumnSpec(["数据日期"], "snapshot_date", "date")],
    )


for _rk in ("fund_performance", "fund_exchange_rank", "fund_money_rank",
            "fund_hk_rank", "fund_dividend_rank"):
    TABLES[_rk] = _ranking(_rk)

# fund_rating has agency as part of PK
TABLES["fund_rating"] = TableSpec(
    name="fund_rating",
    schema=Schema(
        NestedField(1, "fund_code", StringType(), required=True),
        NestedField(2, "fund_name", StringType()),
        NestedField(3, "snapshot_date", DateType(), required=True),
        NestedField(4, "rating_agency", StringType(), required=True),
        NestedField(5, "rating", StringType()),
        identifier_field_ids=[1, 3, 4],
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=MonthTransform(), name="snapshot_month")
    ),
    identifier_fields=["fund_code", "snapshot_date", "rating_agency"],
    write_mode="upsert",
    source_category="fund",
    date_specs=[DateColumnSpec(["数据日期"], "snapshot_date", "date")],
)

# Domain 4: low-frequency
TABLES["fund_split"] = TableSpec(
    name="fund_split",
    schema=Schema(
        NestedField(1, "fund_code", StringType(), required=True),
        NestedField(2, "fund_name", StringType()),
        NestedField(3, "split_ratio", StringType()),
        NestedField(4, "event_date", DateType(), required=True),
        identifier_field_ids=[1, 4],
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=4, field_id=1000, transform=YearTransform(), name="event_year")
    ),
    identifier_fields=["fund_code", "event_date"],
    write_mode="append",
    source_category="fund",
    date_specs=[DateColumnSpec(["拆分日期"], "event_date", "date")],
)
TABLES["fund_purchase"] = _ranking("fund_purchase")
TABLES["fund_index_info"] = _ranking("fund_index_info")
TABLES["fund_portfolio_hold"] = TableSpec(
    name="fund_portfolio_hold",
    schema=Schema(
        NestedField(1, "fund_code", StringType(), required=True),
        NestedField(2, "report_date", DateType(), required=True),
        NestedField(3, "holding_code", StringType(), required=True),
        NestedField(4, "holding_name", StringType()),
        NestedField(5, "weight_pct", DoubleType()),
        NestedField(6, "shares", DoubleType()),
        NestedField(7, "market_value", DoubleType()),
        identifier_field_ids=[1, 2, 3],
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=YearTransform(), name="report_year")
    ),
    identifier_fields=["fund_code", "report_date", "holding_code"],
    write_mode="upsert",
    source_category="fund",
    date_specs=[DateColumnSpec(["报告期"], "report_date", "date")],
)
TABLES["fund_manager"] = TableSpec(
    name="fund_manager",
    schema=Schema(
        NestedField(1, "manager_id", StringType(), required=True),
        NestedField(2, "manager_name", StringType()),
        NestedField(3, "company", StringType()),
        NestedField(4, "aum", DoubleType()),
        NestedField(5, "tenure_return", DoubleType()),
        NestedField(6, "snapshot_date", DateType(), required=True),
        identifier_field_ids=[1, 6],
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=6, field_id=1000, transform=YearTransform(), name="snapshot_year")
    ),
    identifier_fields=["manager_id", "snapshot_date"],
    write_mode="upsert",
    source_category="fund",
    date_specs=[DateColumnSpec(["数据日期"], "snapshot_date", "date")],
)


# Domain 5: K-line (3 markets)
def _kline(name: str, source_category: str) -> TableSpec:
    return TableSpec(
        name=name,
        schema=Schema(
            NestedField(1, "code", StringType(), required=True),
            NestedField(2, "freq", StringType(), required=True),
            NestedField(3, "trade_date", DateType(), required=True),
            NestedField(4, "open", DoubleType()),
            NestedField(5, "high", DoubleType()),
            NestedField(6, "low", DoubleType()),
            NestedField(7, "close", DoubleType()),
            NestedField(8, "volume", DoubleType()),
            NestedField(9, "turnover", DoubleType()),
            identifier_field_ids=[1, 2, 3],
        ),
        partition_spec=PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="freq"),
            PartitionField(source_id=3, field_id=1001, transform=YearTransform(), name="trade_year"),
        ),
        identifier_fields=["code", "freq", "trade_date"],
        write_mode="upsert",
        source_category=source_category,
        date_specs=[DateColumnSpec(["日期", "交易日"], "trade_date", "date")],
    )


TABLES["kline_a"] = _kline("kline_a", "kline_a")
TABLES["kline_hk"] = _kline("kline_hk", "kline_hk")
TABLES["kline_us"] = _kline("kline_us", "kline_us")
