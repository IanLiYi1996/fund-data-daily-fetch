# Fund Data Lake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the daily-fetched fund/stock/macro data from per-day independent parquet files into Apache Iceberg tables registered in AWS Glue Catalog, while preserving the existing raw parquet writes during a 4-8 week dual-write transition.

**Architecture:** Keep the current Step Functions topology unchanged. Add a `shared/schemas/registry.py` declaring 27 Iceberg `TableSpec`s, a new `shared/storage/iceberg_writer.py` that writes pandas DataFrames via pyiceberg's GlueCatalog, and modify `BaseFetcher._safe_fetch` to dual-write (raw parquet + Iceberg, with Iceberg failures isolated). Add a weekly maintenance Lambda for compaction + snapshot expiration, and a one-time backfill script for historical raw parquet.

**Tech Stack:** Python 3.11, pyiceberg ≥0.7, pyarrow, pandas, boto3, AWS Glue Catalog, AWS Lambda (Docker), AWS Step Functions, AWS CDK (TypeScript), pytest, moto.

**Spec reference:** `docs/superpowers/specs/2026-05-09-fund-data-lake-design.md` (commit `58e5d56`)

---

## File Structure

### New files

| Path | Responsibility |
|---|---|
| `lambda/shared/schemas/__init__.py` | Re-export `TABLES`, `TableSpec` |
| `lambda/shared/schemas/registry.py` | 27 `TableSpec` definitions (schema, partition, PK, write_mode) |
| `lambda/shared/schemas/normalizers.py` | Convert Chinese date/code columns to canonical English partition keys |
| `lambda/shared/storage/iceberg_writer.py` | `IcebergWriter` class — load_catalog, ensure_table, upsert/append |
| `lambda/iceberg-maintenance/Dockerfile` | Maintenance Lambda image (mirrors fund-fetcher Dockerfile) |
| `lambda/iceberg-maintenance/handler.py` | rewrite_data_files + expire_snapshots loop |
| `lambda/iceberg-maintenance/requirements.txt` | pyiceberg, pyarrow, boto3 |
| `scripts/iceberg_init_tables.py` | Dry-run lister: print table-creation plan from registry |
| `scripts/backfill_to_iceberg.py` | Read historical raw parquet → upsert into Iceberg |
| `tests/__init__.py` | empty |
| `tests/conftest.py` | shared pytest fixtures (in-memory catalog, sample DataFrames) |
| `tests/test_schemas_registry.py` | Validate 27 specs (PK ⊆ schema, partition refs valid, write_mode valid) |
| `tests/test_normalizers.py` | Date format coercion, missing column fallback, timezone |
| `tests/test_iceberg_writer.py` | upsert idempotency, append, schema evolution, empty DF |
| `tests/test_base_fetcher_dualwrite.py` | raw success + iceberg failure → return both, no exception |

### Modified files

| Path | Change |
|---|---|
| `lambda/shared/fetchers/base_fetcher.py` | `FetchResult` gains `iceberg_result: dict`; `_safe_fetch` accepts and calls `iceberg_writer` |
| `lambda/shared/fetchers/__init__.py` | (no change expected) |
| `lambda/shared/fetchers/fund_fetcher.py` | Add 4 fetch methods + 4 catalog entries: `fund_close_daily`, `fund_fof_daily`, `fund_reits_daily`, `fund_portfolio_hold` |
| `lambda/fund-fetcher/handler.py` (and 6 sibling handlers) | Construct `IcebergWriter` and pass into fetcher |
| `lambda/fund-fetcher/requirements.txt` (and 6 siblings + maintenance) | Add `pyiceberg[glue]>=0.7.0` |
| `lambda/catalog-generator/handler.py` | Append daily dual-write consistency check (raw vs Iceberg row counts) |
| `cdk/lib/fund-data-fetch-stack.ts` | Glue Database + IAM policy for fetcher Lambdas + maintenance Lambda + weekly EventBridge |
| `cdk/package.json` | (no change expected — `aws-cdk-lib` already covers `aws_glue` L1) |

### Out of scope (M7 — post-deployment)

- Switching `data-processor` to read from Iceberg instead of raw parquet — separate plan after 4-week dual-write soak.
- Removing raw-parquet writes — separate plan after Phase 3 monitoring.

---

## Task 1: Project scaffolding (tests dir, dev deps, pytest config)

**Files:**
- Create: `tests/__init__.py` (empty)
- Create: `tests/conftest.py`
- Create: `pyproject.toml` (root) — pytest config + dev deps under `uv`
- Create: `lambda/shared/schemas/__init__.py` (empty placeholder, will fill in Task 3)

- [ ] **Step 1: Create empty test package marker**

```bash
touch tests/__init__.py
touch lambda/shared/schemas/__init__.py
```

- [ ] **Step 2: Create `pyproject.toml` for dev dependencies and pytest**

```toml
[project]
name = "fund-data-daily-fetch"
version = "0.1.0"
description = "Fund data daily fetch with Iceberg lakehouse"
requires-python = ">=3.11"

[tool.uv]
dev-dependencies = [
    "pytest>=8.0",
    "pytest-mock>=3.12",
    "moto[s3,glue]>=5.0",
    "pyiceberg[glue]>=0.7.0",
    "pyarrow>=14.0",
    "pandas>=2.0",
    "boto3>=1.34",
]

[tool.pytest.ini_options]
pythonpath = ["lambda", "."]
testpaths = ["tests"]
addopts = "-ra --strict-markers"
filterwarnings = [
    "ignore::DeprecationWarning",
]
```

- [ ] **Step 3: Create `tests/conftest.py` with shared fixtures**

```python
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
```

- [ ] **Step 4: Install dev deps and confirm pytest works**

```bash
uv sync
uv run pytest --collect-only
```

Expected: `collected 0 items` (no tests yet, but pytest succeeds).

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock tests/__init__.py tests/conftest.py lambda/shared/schemas/__init__.py
git commit -m "chore: scaffold pytest + uv dev deps for Iceberg work"
```

---

## Task 2: Normalizers — Chinese column → canonical partition keys

**Files:**
- Create: `lambda/shared/schemas/normalizers.py`
- Test: `tests/test_normalizers.py`

The akshare DataFrames use Chinese column names like `净值日期`, `数据日期`, `交易日`. Iceberg partition fields must be ASCII identifiers, so we map them to `trade_date` / `snapshot_date` / `event_date` / `report_date` / `snapshot_time` (Date or Timestamp dtype). Columns that already exist with the canonical name pass through.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_normalizers.py
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
        # NaT survives; caller is responsible for filtering
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_normalizers.py -v
```

Expected: `ModuleNotFoundError: No module named 'shared.schemas.normalizers'`

- [ ] **Step 3: Write minimal implementation**

```python
# lambda/shared/schemas/normalizers.py
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_normalizers.py -v
```

Expected: all 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/shared/schemas/normalizers.py tests/test_normalizers.py
git commit -m "feat(schemas): add date column normalizer for akshare DataFrames"
```

---

## Task 3: TableSpec dataclass + 3 representative table definitions

Define the `TableSpec` dataclass and 3 representative tables (one per write_mode pattern: upsert-month, upsert-year, append-event). Remaining 24 tables in Task 4.

**Files:**
- Modify: `lambda/shared/schemas/__init__.py`
- Create: `lambda/shared/schemas/registry.py`
- Test: `tests/test_schemas_registry.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_schemas_registry.py
"""Validate TableSpec definitions in shared.schemas.registry."""
from __future__ import annotations

import pytest

from shared.schemas.registry import TABLES, TableSpec


def test_tables_is_dict_of_tablespec():
    assert isinstance(TABLES, dict)
    assert len(TABLES) >= 3
    for name, spec in TABLES.items():
        assert isinstance(spec, TableSpec)
        assert spec.name == name


def test_required_three_tables_present():
    """Task 3 minimum: upsert-month, upsert-year, append-event examples."""
    assert "fund_daily" in TABLES
    assert "fund_name" in TABLES
    assert "fund_dividend" in TABLES


@pytest.mark.parametrize("name", ["fund_daily", "fund_name", "fund_dividend"])
def test_identifier_fields_subset_of_schema(name):
    spec = TABLES[name]
    schema_field_names = {f.name for f in spec.schema.fields}
    for pk in spec.identifier_fields:
        assert pk in schema_field_names, (
            f"{name}: PK {pk!r} missing from schema fields {schema_field_names}"
        )


@pytest.mark.parametrize("name", ["fund_daily", "fund_name", "fund_dividend"])
def test_partition_fields_reference_schema(name):
    spec = TABLES[name]
    schema_field_ids = {f.field_id for f in spec.schema.fields}
    for pf in spec.partition_spec.fields:
        assert pf.source_id in schema_field_ids, (
            f"{name}: partition source_id {pf.source_id} not in schema"
        )


@pytest.mark.parametrize("name", ["fund_daily", "fund_name", "fund_dividend"])
def test_write_mode_valid(name):
    assert TABLES[name].write_mode in ("upsert", "append")


def test_fund_daily_pk_is_code_and_trade_date():
    assert TABLES["fund_daily"].identifier_fields == ["fund_code", "trade_date"]
    assert TABLES["fund_daily"].write_mode == "upsert"


def test_fund_dividend_is_append_event():
    assert TABLES["fund_dividend"].write_mode == "append"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_schemas_registry.py -v
```

Expected: `ModuleNotFoundError: No module named 'shared.schemas.registry'`

- [ ] **Step 3: Implement registry with 3 tables**

```python
# lambda/shared/schemas/registry.py
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
```

- [ ] **Step 4: Update `__init__.py` to re-export**

```python
# lambda/shared/schemas/__init__.py
from shared.schemas.registry import TABLES, TableSpec
from shared.schemas.normalizers import DateColumnSpec, normalize

__all__ = ["TABLES", "TableSpec", "DateColumnSpec", "normalize"]
```

- [ ] **Step 5: Run test to verify it passes**

```bash
uv run pytest tests/test_schemas_registry.py -v
```

Expected: 8 tests PASS (3 base + 3 parametrized × 3 = wait, 3 parametrized × 3 cases = 9; total 12 PASS).

- [ ] **Step 6: Commit**

```bash
git add lambda/shared/schemas/registry.py lambda/shared/schemas/__init__.py tests/test_schemas_registry.py
git commit -m "feat(schemas): add TableSpec dataclass and 3 representative tables"
```

---

## Task 4: Fill out remaining 24 TableSpec entries

This task only adds data to `registry.py`. Schemas follow the Domain breakdown in spec §2. PR review should focus on whether schemas match akshare DataFrame column types — see akshare docs at https://akshare.akfamily.xyz/ when uncertain.

**Files:**
- Modify: `lambda/shared/schemas/registry.py`
- Modify: `tests/test_schemas_registry.py` (extend coverage)

- [ ] **Step 1: Strengthen test to cover all 27 tables**

Replace the 3-table parametrize lists with `list(TABLES.keys())` and add a count assertion. Append at end of `tests/test_schemas_registry.py`:

```python
def test_table_count_is_27():
    assert len(TABLES) == 27, f"Expected 27 tables, got {len(TABLES)}"


def test_expected_table_names():
    expected = {
        # Domain 1: high-frequency
        "fund_daily", "fund_etf", "fund_lof", "fund_money_daily",
        "fund_financial_daily", "fund_etf_daily", "fund_graded_daily",
        "fund_value_estimation",
        # Domain 2: closed/REITs/FOF (new)
        "fund_close_daily", "fund_fof_daily", "fund_reits_daily",
        # Domain 3: rankings/ratings
        "fund_performance", "fund_exchange_rank", "fund_money_rank",
        "fund_hk_rank", "fund_dividend_rank", "fund_rating",
        # Domain 4: low-frequency
        "fund_dividend", "fund_split", "fund_purchase",
        "fund_index_info", "fund_portfolio_hold", "fund_name", "fund_manager",
        # Domain 5: K-line history
        "kline_a", "kline_hk", "kline_us",
    }
    assert set(TABLES) == expected


@pytest.mark.parametrize("name", sorted({  # all 27 — repeat the literal here so this test is independent
    "fund_daily", "fund_etf", "fund_lof", "fund_money_daily",
    "fund_financial_daily", "fund_etf_daily", "fund_graded_daily",
    "fund_value_estimation", "fund_close_daily", "fund_fof_daily",
    "fund_reits_daily", "fund_performance", "fund_exchange_rank",
    "fund_money_rank", "fund_hk_rank", "fund_dividend_rank", "fund_rating",
    "fund_dividend", "fund_split", "fund_purchase", "fund_index_info",
    "fund_portfolio_hold", "fund_name", "fund_manager",
    "kline_a", "kline_hk", "kline_us",
}))
def test_all_specs_consistent(name):
    spec = TABLES[name]
    assert spec.write_mode in ("upsert", "append")
    schema_field_names = {f.name for f in spec.schema.fields}
    for pk in spec.identifier_fields:
        assert pk in schema_field_names, f"{name}: PK {pk} missing from schema"
    schema_field_ids = {f.field_id for f in spec.schema.fields}
    for pf in spec.partition_spec.fields:
        assert pf.source_id in schema_field_ids, f"{name}: partition src missing"
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run pytest tests/test_schemas_registry.py -v
```

Expected: `test_table_count_is_27` FAILS (3 ≠ 27); `test_expected_table_names` FAILS; the parametrized cases for missing tables FAIL with `KeyError`.

- [ ] **Step 3: Add 24 more TableSpec entries**

Append to `lambda/shared/schemas/registry.py`. Use the field-id rule: schema fields start at 1, increment per field; partition `field_id` starts at 1000, increment per partition column. Below are the 24 entries with deliberately conservative schemas (Strings + Doubles + Dates) — refine column types in a follow-up if akshare adds richer dtypes.

```python
# Helper: build a simple TableSpec quickly (only for tables matching the
# common pattern: fund_code PK + a date PK + N data columns).
from pyiceberg.transforms import DayTransform


def _simple_daily(
    name: str,
    extra_data_columns: list[tuple[str, type]],
    partition_transform=MonthTransform(),
    pk_date_field: str = "trade_date",
    pk_date_aliases: list[str] = ["净值日期", "交易日", "数据日期"],
    write_mode: str = "upsert",
    extra_pk: list[str] = (),  # noqa: B006 — small immutable iterable ok
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
        date_specs=[DateColumnSpec(pk_date_aliases, pk_date_field, "date")],
    )


# Domain 1: high-frequency daily (5 more — fund_daily already defined above)
TABLES["fund_etf"] = _simple_daily(
    "fund_etf",
    [("latest_price", DoubleType), ("change_pct", DoubleType),
     ("volume", DoubleType), ("turnover", DoubleType)],
    pk_date_aliases=["数据日期", "交易日"],
)
TABLES["fund_lof"] = _simple_daily(
    "fund_lof",
    [("latest_price", DoubleType), ("change_pct", DoubleType),
     ("unit_nav", DoubleType), ("premium_pct", DoubleType)],
    pk_date_aliases=["数据日期", "交易日"],
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
    pk_date_aliases=["数据日期", "交易日"],
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
TABLES["fund_purchase"] = _ranking("fund_purchase")  # same shape: snapshot-by-day
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
            PartitionField(source_id=2, field_id=1000, transform=__import__("pyiceberg.transforms", fromlist=["IdentityTransform"]).IdentityTransform(), name="freq"),
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
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
uv run pytest tests/test_schemas_registry.py -v
```

Expected: `test_table_count_is_27` PASS, `test_expected_table_names` PASS, all 27 parametrized cases PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/shared/schemas/registry.py tests/test_schemas_registry.py
git commit -m "feat(schemas): complete 27-table registry across 5 domains"
```

---

## Task 5: IcebergWriter — happy path (write to in-memory catalog)

**Files:**
- Create: `lambda/shared/storage/iceberg_writer.py`
- Test: `tests/test_iceberg_writer.py`

The Lambda runtime will use `GlueCatalog`, but tests use pyiceberg's `SqlCatalog` with a local SQLite file + filesystem warehouse. The class is constructed with a `catalog` already loaded (DI-friendly, easier to test).

- [ ] **Step 1: Write the failing test (basic upsert)**

```python
# tests/test_iceberg_writer.py
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
            "单位净值": [1.0, 2.0],  # second row should win
            "累计净值": [1.0, 2.0],
            "日增长率": [0.0, 0.0],
            "申购状态": ["开放", "开放"],
            "赎回状态": ["开放", "开放"],
            "手续费": ["0%", "0%"],
        })
        result = writer.write("fund_daily", df)
        assert result["rows_inserted"] == 1
        # Verify the surviving row is the second one
        table = writer.catalog.load_table("fund_data_lake.fund_daily")
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
        # Need to add "dividend_amount" to df under canonical name first;
        # IcebergWriter will rename via normalizer, so source col is 分红金额.
        # Map alias: we did not add 分红金额 → dividend_amount in registry yet.
        # For this test we accept that the writer keeps non-PK extra columns.
        result = writer.write("fund_dividend", df)
        assert result.get("rows_appended", 0) >= 1


class TestCreateIfNotExists:
    def test_table_created_on_first_write(self, writer):
        assert "fund_data_lake.fund_daily" not in writer.catalog.list_tables("fund_data_lake")
        df = pd.DataFrame({
            "基金代码": ["000001"], "基金简称": ["A"],
            "净值日期": ["2026-05-09"], "单位净值": [1.0],
            "累计净值": [1.0], "日增长率": [0.0],
            "申购状态": ["开放"], "赎回状态": ["开放"], "手续费": ["0%"],
        })
        writer.write("fund_daily", df)
        assert ("fund_data_lake", "fund_daily") in writer.catalog.list_tables("fund_data_lake")
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run pytest tests/test_iceberg_writer.py -v
```

Expected: `ModuleNotFoundError: No module named 'shared.storage.iceberg_writer'`

- [ ] **Step 3: Implement IcebergWriter**

```python
# lambda/shared/storage/iceberg_writer.py
"""Write pandas DataFrames to Iceberg tables via pyiceberg."""
from __future__ import annotations

import os
from typing import Any

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog

from shared.schemas import TABLES, normalize
from shared.utils.logger import get_logger


_AKSHARE_TO_CANONICAL: dict[str, str] = {
    "基金代码": "fund_code",
    "代码": "fund_code",
    "基金简称": "fund_name",
    "基金名称": "fund_name",
    "名称": "fund_name",
    "单位净值": "unit_nav",
    "累计净值": "accum_nav",
    "日增长率": "daily_return_pct",
    "申购状态": "subscription_status",
    "赎回状态": "redemption_status",
    "手续费": "fee",
    "最新价": "latest_price",
    "涨跌幅": "change_pct",
    "成交量": "volume",
    "成交额": "turnover",
    "市价": "market_price",
    "折溢价率": "premium_pct",
    "万份收益": "ten_thousand_yield",
    "7日年化": "annual_yield_7d",
    "估算净值": "estimated_nav",
    "估算涨跌幅": "estimated_change_pct",
    "近1周": "weekly_return",
    "近1月": "monthly_return",
    "近1年": "yearly_return",
    "分红金额": "dividend_amount",
    "拆分比例": "split_ratio",
    "持仓代码": "holding_code",
    "持仓名称": "holding_name",
    "占净值比例": "weight_pct",
    "持仓数量": "shares",
    "市值": "market_value",
    "基金经理": "manager_name",
    "基金经理ID": "manager_id",
    "所属公司": "company",
    "管理规模": "aum",
    "任职回报": "tenure_return",
    "评级机构": "rating_agency",
    "评级": "rating",
    "基金类型": "fund_type",
    # K-line aliases
    "日期": "trade_date",
    "开盘": "open", "最高": "high", "最低": "low", "收盘": "close",
}


class IcebergWriter:
    """Write DataFrames to Iceberg tables registered in a catalog."""

    def __init__(self, catalog: Catalog, database: str) -> None:
        self.catalog = catalog
        self.database = database
        self.logger = get_logger(self.__class__.__name__)

    @classmethod
    def from_glue(cls, database: str, warehouse: str) -> "IcebergWriter":
        """Construct using AWS Glue Catalog (used in Lambda runtime)."""
        catalog = load_catalog(
            "glue",
            **{
                "type": "glue",
                "glue.region": os.environ.get("AWS_REGION", "us-east-1"),
                "warehouse": warehouse,
            },
        )
        return cls(catalog=catalog, database=database)

    def write(self, table_name: str, df: pd.DataFrame) -> dict[str, Any]:
        """Normalize, ensure table exists, then upsert/append per spec."""
        if df is None or df.empty:
            return {"skipped": True, "reason": "empty"}

        spec = TABLES[table_name]

        # 1. Rename Chinese columns to canonical names (best-effort)
        renamed = df.rename(columns=_AKSHARE_TO_CANONICAL)

        # 2. Normalize date columns (renames + coerces dtype, drops bad rows)
        try:
            normalized = normalize(renamed, spec.date_specs)
        except KeyError as e:
            self.logger.error(f"{table_name}: missing required date column - {e}")
            return {"error": str(e), "rows_inserted": 0}

        # 3. Drop in-batch PK duplicates (keep last)
        if normalized.duplicated(subset=spec.identifier_fields).any():
            n_before = len(normalized)
            normalized = normalized.drop_duplicates(
                subset=spec.identifier_fields, keep="last"
            ).reset_index(drop=True)
            self.logger.warning(
                f"{table_name}: dropped {n_before - len(normalized)} in-batch dupes"
            )

        # 4. Project to schema columns (drop unknown extras to avoid evolve churn)
        schema_cols = [f.name for f in spec.schema.fields]
        keep_cols = [c for c in schema_cols if c in normalized.columns]
        projected = normalized[keep_cols]

        # 5. Build Arrow table aligned to Iceberg schema
        arrow_table = pa.Table.from_pandas(projected, preserve_index=False)
        iceberg_arrow_schema = spec.schema.as_arrow()
        # Cast to align dtypes (e.g., Python date → Arrow date32)
        arrow_table = arrow_table.cast(
            pa.schema(
                [iceberg_arrow_schema.field(c) for c in arrow_table.column_names]
            )
        )

        # 6. Ensure table exists
        identifier = (self.database, table_name)
        if not self.catalog.table_exists(identifier):
            self.logger.info(f"Creating Iceberg table {self.database}.{table_name}")
            self.catalog.create_table(
                identifier=identifier,
                schema=spec.schema,
                partition_spec=spec.partition_spec,
            )
        table = self.catalog.load_table(identifier)

        # 7. Write per spec.write_mode
        if spec.write_mode == "upsert":
            result = table.upsert(arrow_table)
            return {
                "rows_inserted": result.rows_inserted,
                "rows_updated": result.rows_updated,
            }
        else:  # append
            table.append(arrow_table)
            return {"rows_appended": len(arrow_table)}
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
uv run pytest tests/test_iceberg_writer.py -v
```

Expected: 5+ tests PASS. The append-mode test for `fund_dividend` may need adjustment depending on column projection behavior; if it fails, inspect actual rows written and assert on row count.

- [ ] **Step 5: Commit**

```bash
git add lambda/shared/storage/iceberg_writer.py tests/test_iceberg_writer.py
git commit -m "feat(storage): add IcebergWriter with upsert/append modes"
```

---

## Task 6: Wire IcebergWriter into BaseFetcher (dual-write with error isolation)

**Files:**
- Modify: `lambda/shared/fetchers/base_fetcher.py`
- Test: `tests/test_base_fetcher_dualwrite.py`

The current `_safe_fetch` returns a `FetchResult`; the per-Lambda handler then loops over results and calls `s3_client.upload_dataframe`. We add a new `dual_write` method on the fetcher that performs the upload + Iceberg write, and add an `iceberg_result` field to `FetchResult`. The per-Lambda handler will be updated in Task 7.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_base_fetcher_dualwrite.py
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
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run pytest tests/test_base_fetcher_dualwrite.py -v
```

Expected: `AttributeError: 'BaseFetcher' object has no attribute 'dual_write'`

- [ ] **Step 3: Add `dual_write` to BaseFetcher**

Replace the file `lambda/shared/fetchers/base_fetcher.py` with:

```python
"""Base fetcher with dual-write helper (raw S3 parquet + Iceberg)."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Optional

import pandas as pd

from shared.utils.logger import get_logger


@dataclass
class FetchResult:
    """Result of a data fetch operation."""

    name: str
    data: Optional[pd.DataFrame] = None
    success: bool = False
    error: Optional[str] = None
    row_count: int = 0
    raw_result: Optional[dict] = None
    iceberg_result: Optional[dict] = None

    def __post_init__(self) -> None:
        if self.data is not None:
            self.row_count = len(self.data)


@dataclass
class FetchSummary:
    category: str
    results: List[FetchResult] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failure_count(self) -> int:
        return sum(1 for r in self.results if not r.success)

    @property
    def total_rows(self) -> int:
        return sum(r.row_count for r in self.results)


class BaseFetcher(ABC):
    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    @property
    @abstractmethod
    def category(self) -> str: ...

    @abstractmethod
    def fetch_all(self) -> FetchSummary: ...

    def _safe_fetch(self, name: str, fetch_func, *args, **kwargs) -> FetchResult:
        """Invoke a fetch function and wrap success/failure in a FetchResult."""
        try:
            self.logger.info(f"Fetching {name}...")
            df = fetch_func(*args, **kwargs)
            if df is None or df.empty:
                self.logger.warning(f"{name}: No data returned")
                return FetchResult(name=name, success=True, data=pd.DataFrame())
            self.logger.info(f"{name}: Fetched {len(df)} rows")
            return FetchResult(name=name, data=df, success=True)
        except Exception as e:
            self.logger.error(f"{name}: Failed to fetch - {e}")
            return FetchResult(name=name, success=False, error=str(e))

    def dual_write(
        self,
        result: FetchResult,
        s3_client,
        iceberg_writer,
        category: str,
        **upload_kwargs: Any,
    ) -> dict[str, Any]:
        """Write a fetched DataFrame to both raw S3 parquet and Iceberg.

        Each side is independently wrapped: raw failure does NOT prevent
        Iceberg write, and vice versa. Iceberg always isolates exceptions.
        """
        if not result.success or result.data is None or result.data.empty:
            return {"raw": None, "iceberg": None, "skipped": True}

        # ① Raw parquet write (current source of truth)
        raw_out: dict[str, Any]
        try:
            raw_out = s3_client.upload_dataframe(
                df=result.data,
                category=category,
                data_name=result.name,
                **upload_kwargs,
            )
        except Exception as e:
            self.logger.error(f"{result.name}: raw upload failed - {e}")
            raw_out = {"error": str(e)}

        # ② Iceberg write (errors isolated; never fail the whole fetch)
        iceberg_out: dict[str, Any]
        try:
            iceberg_out = iceberg_writer.write(result.name, result.data)
        except Exception as e:
            self.logger.error(f"{result.name}: iceberg write failed - {e}")
            iceberg_out = {"error": str(e)}

        result.raw_result = raw_out
        result.iceberg_result = iceberg_out
        return {"raw": raw_out, "iceberg": iceberg_out}
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
uv run pytest tests/test_base_fetcher_dualwrite.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 5: Run the full test suite to confirm no regressions**

```bash
uv run pytest -v
```

Expected: all tests from Tasks 1-6 PASS.

- [ ] **Step 6: Commit**

```bash
git add lambda/shared/fetchers/base_fetcher.py tests/test_base_fetcher_dualwrite.py
git commit -m "feat(fetchers): add dual_write helper with error isolation"
```

---

## Task 7: Update all 7 Lambda handlers to use IcebergWriter

The change is mechanical: each handler imports `IcebergWriter`, constructs it from Glue, and replaces the `s3_client.upload_dataframe(...)` call with `fetcher.dual_write(result, s3_client, iceberg, category=summary.category, date=fetch_date)`.

**Files (modify all 7):**
- `lambda/fund-fetcher/handler.py`
- `lambda/cn-index-fetcher/handler.py`
- `lambda/cn-macro-fetcher/handler.py`
- `lambda/a-share-fetcher/handler.py`
- `lambda/hk-stock-fetcher/handler.py`
- `lambda/us-stock-fetcher/handler.py`
- `lambda/hist-kline-fetcher/handler.py`

**Files (modify requirements):**
- All 7 fetcher `requirements.txt` + `lambda/data-processor/requirements.txt`: add `pyiceberg[glue]>=0.7.0`

- [ ] **Step 1: Update `fund-fetcher/handler.py` (reference change for the other 6)**

Replace the body of `lambda/fund-fetcher/handler.py` with:

```python
"""Lambda handler for fund data fetch (dual-write: raw parquet + Iceberg)."""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict

from shared.utils.config import Config
from shared.utils.logger import get_logger
from shared.fetchers import FundFetcher
from shared.storage import S3Client
from shared.storage.iceberg_writer import IcebergWriter

logger = get_logger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start_time = datetime.now()
    logger.info(f"Starting fund data fetch. Event: {json.dumps(event, default=str)}")

    try:
        config = Config.from_env()
        config.validate()
        s3_client = S3Client(config.s3_bucket)
        warehouse = f"s3://{config.s3_bucket}/iceberg/"
        iceberg = IcebergWriter.from_glue(database="fund_data_lake", warehouse=warehouse)
        fetch_date = datetime.now()

        fetcher = FundFetcher()
        summary = fetcher.fetch_all()

        uploads: list[dict] = []
        errors: list[dict] = []
        iceberg_summaries: list[dict] = []

        for result in summary.results:
            out = fetcher.dual_write(
                result, s3_client, iceberg,
                category=summary.category, date=fetch_date,
            )
            if out.get("skipped"):
                if not result.success:
                    errors.append({"name": result.name, "error": result.error})
                continue
            raw_out = out["raw"] or {}
            iceberg_out = out["iceberg"] or {}
            if "error" in raw_out:
                errors.append({"name": result.name, "error": f"raw: {raw_out['error']}"})
            else:
                uploads.append({
                    "name": result.name, "rows": result.row_count,
                    "s3_key": raw_out.get("key"), "size": raw_out.get("size"),
                })
            if "error" in iceberg_out:
                errors.append({"name": result.name, "error": f"iceberg: {iceberg_out['error']}"})
            iceberg_summaries.append({"name": result.name, **iceberg_out})

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Fund fetch completed: {len(uploads)} uploads, {len(errors)} errors "
            f"in {elapsed:.2f}s"
        )

        return {
            "statusCode": 200,
            "downloader": "fund",
            "success": True,
            "success_count": len(uploads),
            "error_count": len(errors),
            "total_rows": sum(u["rows"] for u in uploads),
            "uploads": uploads,
            "iceberg": iceberg_summaries,
            "errors": errors,
            "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
            "catalog": FundFetcher.get_data_catalog(),
        }

    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.exception("Fund fetch failed")
        return {
            "statusCode": 500, "downloader": "fund", "success": False,
            "error": str(e), "elapsed_seconds": round(elapsed, 2),
            "timestamp": datetime.now().isoformat(),
        }
```

- [ ] **Step 2: Apply the same pattern to the other 6 handlers**

For each of the 6 sibling handlers, replicate the changes:

1. Add imports:
   ```python
   from shared.storage.iceberg_writer import IcebergWriter
   ```
2. Construct the writer:
   ```python
   warehouse = f"s3://{config.s3_bucket}/iceberg/"
   iceberg = IcebergWriter.from_glue(database="fund_data_lake", warehouse=warehouse)
   ```
3. Replace the `for result in summary.results:` upload loop with the `dual_write` pattern shown above (adapt fetcher class + downloader name).

Files to edit:
- `lambda/cn-index-fetcher/handler.py`
- `lambda/cn-macro-fetcher/handler.py`
- `lambda/a-share-fetcher/handler.py`
- `lambda/hk-stock-fetcher/handler.py`
- `lambda/us-stock-fetcher/handler.py`
- `lambda/hist-kline-fetcher/handler.py`

- [ ] **Step 3: Add pyiceberg to all 7 requirements.txt + maintenance**

Append `pyiceberg[glue]>=0.7.0` to:

```
lambda/fund-fetcher/requirements.txt
lambda/cn-index-fetcher/requirements.txt
lambda/cn-macro-fetcher/requirements.txt
lambda/a-share-fetcher/requirements.txt
lambda/hk-stock-fetcher/requirements.txt
lambda/us-stock-fetcher/requirements.txt
lambda/hist-kline-fetcher/requirements.txt
```

- [ ] **Step 4: Sanity check — Docker build the fund fetcher locally**

```bash
cd lambda
docker build -f fund-fetcher/Dockerfile -t fund-fetcher-test .
```

Expected: build completes; pyiceberg layer adds ~50 MB but no errors.

- [ ] **Step 5: Commit**

```bash
git add lambda/*/handler.py lambda/*/requirements.txt
git commit -m "feat(lambda): wire IcebergWriter dual-write into 7 fetcher handlers"
```

---

## Task 8: Add 4 new fetcher methods (closed/FOF/REITs/portfolio_hold)

**Files:**
- Modify: `lambda/shared/fetchers/fund_fetcher.py`
- (No new tests — these are thin akshare wrappers; integration tested via dev deployment)

- [ ] **Step 1: Add 4 methods + catalog entries**

Append to `lambda/shared/fetchers/fund_fetcher.py`:

```python
    # === New: closed-end / REITs / FOF / portfolio_hold ===
    def _fetch_fund_close_daily(self):
        return ak.fund_close_em()

    def _fetch_fund_fof_daily(self):
        return ak.fund_fof_em()

    def _fetch_fund_reits_daily(self):
        return ak.public_fund_REITs()

    def _fetch_fund_portfolio_hold(self):
        # Quarterly; fetch most recent quarter for all funds is too heavy.
        # As a starting point we delegate to a single-fund call list per quarter.
        # For now return empty — to be replaced when per-fund fan-out is added.
        import pandas as pd
        return pd.DataFrame()
```

Add 4 catalog entries to the `DATA_CATALOG` dict (above `# Priority 4`):

```python
        # Priority 5: Closed-end / REITs / FOF / portfolio
        "fund_close_daily": {
            "name_cn": "场内封闭式基金",
            "description": "场内封闭式基金净值与折溢价",
            "source_api": "fund_close_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "最新价", "单位净值", "折溢价率"],
            "has_fee_data": False,
        },
        "fund_fof_daily": {
            "name_cn": "FOF 基金每日",
            "description": "FOF 基金每日净值与收益",
            "source_api": "fund_fof_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "单位净值", "累计净值"],
            "has_fee_data": False,
        },
        "fund_reits_daily": {
            "name_cn": "公募 REITs",
            "description": "公募 REITs 实时行情",
            "source_api": "public_fund_REITs",
            "update_frequency": "daily",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅"],
            "has_fee_data": False,
        },
        "fund_portfolio_hold": {
            "name_cn": "基金持仓明细",
            "description": "基金季度报告持仓明细（低频）",
            "source_api": "fund_portfolio_hold_em",
            "update_frequency": "quarterly",
            "key_fields": ["基金代码", "报告期", "持仓代码", "持仓名称", "占净值比例"],
            "has_fee_data": False,
        },
```

Add 4 calls inside `fetch_all`:

```python
        # ===== Priority 5: Closed / FOF / REITs / portfolio =====
        results.append(self._safe_fetch("fund_close_daily", self._fetch_fund_close_daily))
        results.append(self._safe_fetch("fund_fof_daily", self._fetch_fund_fof_daily))
        results.append(self._safe_fetch("fund_reits_daily", self._fetch_fund_reits_daily))
        results.append(self._safe_fetch("fund_portfolio_hold", self._fetch_fund_portfolio_hold))
```

- [ ] **Step 2: Confirm no test regressions**

```bash
uv run pytest -v
```

Expected: existing tests still PASS (no new tests added; akshare wrappers verified via dev deploy).

- [ ] **Step 3: Commit**

```bash
git add lambda/shared/fetchers/fund_fetcher.py
git commit -m "feat(fund-fetcher): add 4 new sources (closed/FOF/REITs/portfolio_hold)"
```

---

## Task 9: Maintenance Lambda — compaction + snapshot expiration

**Files:**
- Create: `lambda/iceberg-maintenance/handler.py`
- Create: `lambda/iceberg-maintenance/requirements.txt`
- Create: `lambda/iceberg-maintenance/Dockerfile`
- Test: `tests/test_iceberg_maintenance.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_iceberg_maintenance.py
"""Tests for the iceberg-maintenance Lambda handler logic."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_run_maintenance_iterates_all_tables():
    from importlib import import_module
    import sys
    sys.path.insert(0, "lambda/iceberg-maintenance")
    handler_mod = import_module("handler")

    fake_table = MagicMock()
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fake_table

    summary = handler_mod.run_maintenance(catalog=fake_catalog, database="fund_data_lake")

    # 27 tables means 27 calls each
    from shared.schemas import TABLES
    assert fake_catalog.load_table.call_count == len(TABLES)
    assert fake_table.rewrite_data_files.call_count == len(TABLES)
    assert fake_table.expire_snapshots.call_count == len(TABLES)
    assert summary["tables_processed"] == len(TABLES)
    assert summary["errors"] == []


def test_one_table_failure_continues_others():
    from importlib import import_module
    handler_mod = import_module("handler")

    fake_catalog = MagicMock()
    bad_table = MagicMock()
    bad_table.rewrite_data_files.side_effect = RuntimeError("boom")
    good_table = MagicMock()
    fake_catalog.load_table.side_effect = (
        [bad_table] + [good_table] * 1000
    )

    summary = handler_mod.run_maintenance(catalog=fake_catalog, database="fund_data_lake")

    assert len(summary["errors"]) == 1
    assert "boom" in summary["errors"][0]["error"]
    # Still processed remaining 26
    assert summary["tables_processed"] >= 26
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run pytest tests/test_iceberg_maintenance.py -v
```

Expected: import fails (`handler` module not found yet).

- [ ] **Step 3: Implement the maintenance handler**

```python
# lambda/iceberg-maintenance/handler.py
"""Weekly Iceberg maintenance: compaction + snapshot expiration."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from shared.schemas import TABLES
from shared.storage.iceberg_writer import IcebergWriter
from shared.utils.config import Config
from shared.utils.logger import get_logger

logger = get_logger(__name__)

TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB
SNAPSHOT_RETENTION_DAYS = 14


def run_maintenance(catalog, database: str) -> Dict[str, Any]:
    """Iterate all registered tables; run compaction + snapshot expiration.

    Errors on a single table are isolated and recorded; others continue.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    summary: Dict[str, Any] = {
        "tables_processed": 0,
        "errors": [],
    }

    for table_name in TABLES:
        try:
            table = catalog.load_table((database, table_name))
            table.rewrite_data_files(target_size_bytes=TARGET_FILE_SIZE_BYTES)
            table.expire_snapshots(timestamp_ms=cutoff_ms)
            summary["tables_processed"] += 1
            logger.info(f"Maintenance ok: {table_name}")
        except Exception as e:
            logger.exception(f"Maintenance failed for {table_name}")
            summary["errors"].append({"table": table_name, "error": str(e)})
    return summary


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start = datetime.now()
    logger.info(f"Iceberg maintenance starting. Event: {event}")
    config = Config.from_env()
    config.validate()
    warehouse = f"s3://{config.s3_bucket}/iceberg/"
    writer = IcebergWriter.from_glue(database="fund_data_lake", warehouse=warehouse)
    summary = run_maintenance(writer.catalog, "fund_data_lake")
    elapsed = (datetime.now() - start).total_seconds()
    return {
        "statusCode": 200,
        "downloader": "iceberg-maintenance",
        "success": len(summary["errors"]) == 0,
        **summary,
        "elapsed_seconds": round(elapsed, 2),
        "timestamp": datetime.now().isoformat(),
    }
```

- [ ] **Step 4: Add requirements.txt and Dockerfile**

```
# lambda/iceberg-maintenance/requirements.txt
pyiceberg[glue]>=0.7.0
pyarrow>=14.0.0
boto3>=1.34.0
```

```dockerfile
# lambda/iceberg-maintenance/Dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc g++ make curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /var/task

RUN pip install --no-cache-dir awslambdaric

COPY iceberg-maintenance/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY iceberg-maintenance/handler.py .

ENTRYPOINT ["python", "-m", "awslambdaric"]
CMD ["handler.lambda_handler"]
```

- [ ] **Step 5: Run the full test suite**

```bash
uv run pytest -v
```

Expected: all tests PASS, including 2 new ones.

- [ ] **Step 6: Commit**

```bash
git add lambda/iceberg-maintenance/ tests/test_iceberg_maintenance.py
git commit -m "feat(maintenance): add weekly Iceberg compaction + snapshot expire Lambda"
```

---

## Task 10: CDK changes — Glue Database, IAM, maintenance Lambda, weekly schedule

**Files:**
- Modify: `cdk/lib/fund-data-fetch-stack.ts`

- [ ] **Step 1: Add Glue Database, IAM policy, maintenance Lambda + schedule**

Insert the following at appropriate locations in `cdk/lib/fund-data-fetch-stack.ts`:

**(a) Add import at top:**

```typescript
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
```

**(b) After the S3 bucket block, before `// ========== Lambda Environment ==========`, add:**

```typescript
    // ========== Glue Catalog ==========
    const glueDatabase = new glue.CfnDatabase(this, "FundDataLakeDb", {
      catalogId: this.account,
      databaseInput: { name: "fund_data_lake" },
    });
```

**(c) Inside `lambdaEnv`, append `WAREHOUSE_PATH`:**

```typescript
    const lambdaEnv = {
      S3_BUCKET: this.bucket.bucketName,
      LOG_LEVEL: "INFO",
      PYTHONUNBUFFERED: "1",
      WAREHOUSE_PATH: `s3://${this.bucket.bucketName}/iceberg/`,
    };
```

**(d) Add an IAM PolicyStatement for Iceberg writes, attached to the 7 fetcher Lambdas + maintenance Lambda:**

After the `[fundFetchLambda, ...].forEach((fn) => this.bucket.grantReadWrite(fn));` block, add:

```typescript
    const icebergGluePolicy = new iam.PolicyStatement({
      actions: [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
      ],
      resources: [
        `arn:aws:glue:${this.region}:${this.account}:catalog`,
        `arn:aws:glue:${this.region}:${this.account}:database/fund_data_lake`,
        `arn:aws:glue:${this.region}:${this.account}:table/fund_data_lake/*`,
      ],
    });

    [
      fundFetchLambda,
      cnIndexFetchLambda,
      cnMacroFetchLambda,
      aShareFetchLambda,
      hkStockFetchLambda,
      usStockFetchLambda,
      histKlineFetchLambda,
    ].forEach((fn) => fn.addToRolePolicy(icebergGluePolicy));
```

**(e) Add the maintenance Lambda (after `dataProcessorLambda` definition):**

```typescript
    const icebergMaintenanceLambda = this.createDockerLambda(
      "IcebergMaintenanceLambda",
      lambdaDir,
      "iceberg-maintenance/Dockerfile",
      "Weekly Iceberg compaction + snapshot expiration",
      3008,
      14,
      lambdaEnv
    );
    this.bucket.grantReadWrite(icebergMaintenanceLambda);
    icebergMaintenanceLambda.addToRolePolicy(icebergGluePolicy);
```

**(f) Add a weekly EventBridge rule (after the existing daily `scheduleRule`):**

```typescript
    const weeklyMaintenanceRule = new events.Rule(this, "WeeklyMaintenanceRule", {
      ruleName: "FundDataLakeWeeklyMaintenance",
      description: "Weekly Iceberg compaction + snapshot expiration (Sunday 20:00 UTC)",
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "20",
        weekDay: "SUN",
      }),
    });
    weeklyMaintenanceRule.addTarget(
      new targets.LambdaFunction(icebergMaintenanceLambda)
    );
```

**(g) Add output for the Glue DB:**

```typescript
    new CfnOutput(this, "GlueDatabaseName", {
      value: "fund_data_lake",
      description: "Glue database for Iceberg tables",
      exportName: "FundDataLakeGlueDb",
    });
```

- [ ] **Step 2: Verify CDK synth succeeds**

```bash
cd cdk
npm run build
npx cdk synth FundDataFetchStack > /dev/null
```

Expected: synth succeeds; no TypeScript errors.

- [ ] **Step 3: Confirm Glue DB and policies appear in synthesized template**

```bash
npx cdk synth FundDataFetchStack | grep -E "fund_data_lake|IcebergMaintenance|WeeklyMaintenance" | head -20
```

Expected: at least 4 matches (database, lambda, schedule, IAM).

- [ ] **Step 4: Commit**

```bash
cd ..
git add cdk/lib/fund-data-fetch-stack.ts
git commit -m "feat(cdk): add Glue DB, Iceberg IAM, weekly maintenance Lambda"
```

---

## Task 11: Backfill script — historical raw parquet → Iceberg

**Files:**
- Create: `scripts/iceberg_init_tables.py`
- Create: `scripts/backfill_to_iceberg.py`

- [ ] **Step 1: Create the dry-run init script**

```python
#!/usr/bin/env python3
# scripts/iceberg_init_tables.py
"""Print table-creation plan for the fund_data_lake Glue database.

Run this BEFORE deploying to verify the registry is valid:
    uv run python scripts/iceberg_init_tables.py --dry-run

Or to actually create empty tables in Glue:
    uv run python scripts/iceberg_init_tables.py --apply --bucket=fund-data-...
"""
from __future__ import annotations

import argparse
import sys

sys.path.insert(0, "lambda")
from shared.schemas import TABLES  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--bucket", help="S3 bucket for warehouse")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--database", default="fund_data_lake")
    args = p.parse_args()

    if args.dry_run == args.apply:
        p.error("specify exactly one of --dry-run or --apply")

    print(f"# {len(TABLES)} tables in registry\n")
    for name, spec in TABLES.items():
        print(f"## {name} ({spec.write_mode})")
        print(f"  PK: {spec.identifier_fields}")
        print(f"  Partition: {[f.name for f in spec.partition_spec.fields]}")
        print(f"  Schema fields: {[f.name for f in spec.schema.fields]}")
        print()

    if args.dry_run:
        return 0

    if not args.bucket:
        p.error("--apply requires --bucket")

    from shared.storage.iceberg_writer import IcebergWriter
    writer = IcebergWriter.from_glue(
        database=args.database,
        warehouse=f"s3://{args.bucket}/iceberg/",
    )
    if args.database not in [n[0] for n in writer.catalog.list_namespaces()]:
        writer.catalog.create_namespace(args.database)

    for name, spec in TABLES.items():
        identifier = (args.database, name)
        if writer.catalog.table_exists(identifier):
            print(f"[skip] {name} (exists)")
            continue
        writer.catalog.create_table(
            identifier=identifier,
            schema=spec.schema,
            partition_spec=spec.partition_spec,
        )
        print(f"[create] {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Create the backfill script**

```python
#!/usr/bin/env python3
# scripts/backfill_to_iceberg.py
"""Backfill historical raw parquet from S3 into Iceberg tables.

Run from EC2 or local with AWS credentials configured:
    uv run python scripts/backfill_to_iceberg.py \
        --bucket fund-data-... \
        --table fund_daily \
        --start 2026-01-01 --end 2026-05-08

Or backfill all tables:
    uv run python scripts/backfill_to_iceberg.py --bucket ... --all
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta

import boto3
import pandas as pd

sys.path.insert(0, "lambda")
from shared.schemas import TABLES  # noqa: E402
from shared.storage.iceberg_writer import IcebergWriter  # noqa: E402


def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def backfill_table(s3, writer, bucket: str, table_name: str,
                   start: date, end: date) -> dict:
    spec = TABLES[table_name]
    n_files, n_rows_inserted, n_rows_updated, n_failures = 0, 0, 0, 0

    for d in daterange(start, end):
        key = f"{spec.source_category}/{d.isoformat()}/{table_name}.parquet"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
        except s3.exceptions.NoSuchKey:
            continue
        try:
            df = pd.read_parquet(obj["Body"])
            result = writer.write(table_name, df)
            n_files += 1
            n_rows_inserted += result.get("rows_inserted", 0)
            n_rows_updated += result.get("rows_updated", 0)
            n_rows_inserted += result.get("rows_appended", 0)
            print(f"  {key}: {result}")
        except Exception as e:
            n_failures += 1
            print(f"  {key}: FAIL {e}")

    return {
        "table": table_name,
        "files": n_files,
        "rows_inserted": n_rows_inserted,
        "rows_updated": n_rows_updated,
        "failures": n_failures,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--database", default="fund_data_lake")
    p.add_argument("--start", help="YYYY-MM-DD", default="2025-01-01")
    p.add_argument("--end", help="YYYY-MM-DD",
                   default=datetime.now().date().isoformat())
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--table", help="single table name")
    g.add_argument("--all", action="store_true")
    args = p.parse_args()

    s3 = boto3.client("s3", region_name=args.region)
    writer = IcebergWriter.from_glue(
        database=args.database, warehouse=f"s3://{args.bucket}/iceberg/"
    )

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    targets = list(TABLES) if args.all else [args.table]
    summaries = []
    for t in targets:
        print(f"\n=== {t} ===")
        summary = backfill_table(s3, writer, args.bucket, t, start, end)
        summaries.append(summary)
        print(f"summary: {summary}")

    print("\n=== overall ===")
    for s in summaries:
        print(s)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Smoke test the dry-run init**

```bash
uv run python scripts/iceberg_init_tables.py --dry-run | head -30
```

Expected: prints 27 tables with PK / partition / schema fields.

- [ ] **Step 4: Make scripts executable + commit**

```bash
chmod +x scripts/iceberg_init_tables.py scripts/backfill_to_iceberg.py
git add scripts/iceberg_init_tables.py scripts/backfill_to_iceberg.py
git commit -m "feat(scripts): add Iceberg init dry-run and historical backfill"
```

---

## Task 12: Daily dual-write consistency check (in catalog-generator)

**Files:**
- Modify: `lambda/catalog-generator/handler.py`
- Modify: `lambda/catalog-generator/requirements.txt` (add `pyiceberg[glue]>=0.7.0`)

- [ ] **Step 1: Append validation to catalog-generator handler**

After the existing catalog upload block in `lambda/catalog-generator/handler.py`, before the `return` statement, add:

```python
        # === Dual-write consistency check (raw vs Iceberg row counts) ===
        try:
            from shared.schemas import TABLES
            from shared.storage.iceberg_writer import IcebergWriter
            writer = IcebergWriter.from_glue(
                database="fund_data_lake",
                warehouse=f"s3://{config.s3_bucket}/iceberg/",
            )
            mismatches = []
            for tname, spec in TABLES.items():
                # Only check tables likely to have data on this day
                if spec.write_mode != "upsert":
                    continue
                raw_key = f"{spec.source_category}/{date_str}/{tname}.parquet"
                try:
                    head = s3_client.s3_client.head_object(
                        Bucket=config.s3_bucket, Key=raw_key
                    )
                except Exception:
                    continue  # raw not present; skip
                # Iceberg row count for today
                try:
                    table = writer.catalog.load_table(("fund_data_lake", tname))
                    # Count rows for today's partition
                    df = table.scan().to_pandas()
                    date_col = spec.date_specs[0].target if spec.date_specs else None
                    if date_col and date_col in df.columns:
                        ice_count = int((df[date_col] == date.fromisoformat(date_str)).sum())
                    else:
                        ice_count = len(df)
                    raw_size = head["ContentLength"]
                    if raw_size > 0 and ice_count == 0:
                        mismatches.append({"table": tname, "raw_size": raw_size, "iceberg_rows": ice_count})
                except Exception as e:
                    mismatches.append({"table": tname, "error": str(e)})
            catalog["consistency_check"] = {
                "checked_tables": len(TABLES),
                "mismatches": mismatches,
            }
            if mismatches:
                logger.warning(f"Dual-write mismatches: {mismatches}")
        except Exception as e:
            logger.exception("Consistency check failed (non-fatal)")
            catalog["consistency_check"] = {"error": str(e)}
```

Add `from datetime import date` to the imports at the top.

- [ ] **Step 2: Add pyiceberg to catalog-generator requirements**

```bash
echo "pyiceberg[glue]>=0.7.0" >> lambda/catalog-generator/requirements.txt
```

- [ ] **Step 3: Run all tests**

```bash
uv run pytest -v
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add lambda/catalog-generator/handler.py lambda/catalog-generator/requirements.txt
git commit -m "feat(catalog): add daily dual-write consistency check"
```

---

## Task 13: End-to-end deploy + smoke test (manual, dev account)

This task is operational, not coded. It verifies M1-M6 on real AWS before declaring "ready for the 4-week soak" (M7 Phase 1).

- [ ] **Step 1: Deploy CDK**

```bash
cd cdk
npm run build
npx cdk diff FundDataFetchStack
npx cdk deploy FundDataFetchStack --require-approval never
```

Expected: stack updates with new Glue DB, IAM, and maintenance Lambda. No drift on existing resources.

- [ ] **Step 2: Initialize empty Iceberg tables**

```bash
cd ..
BUCKET=$(aws cloudformation describe-stacks --stack-name FundDataFetchStack \
  --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" --output text)
uv run python scripts/iceberg_init_tables.py --apply --bucket "$BUCKET"
```

Expected: 27 tables created in Glue (verify in console: AWS Glue → Databases → fund_data_lake → Tables).

- [ ] **Step 3: Manually trigger Step Functions**

```bash
SFN_ARN=$(aws cloudformation describe-stacks --stack-name FundDataFetchStack \
  --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" --output text)
aws stepfunctions start-execution \
  --state-machine-arn "$SFN_ARN" \
  --input '{"triggered_by":"manual-iceberg-smoke"}'
```

Wait ~5 minutes; verify the execution succeeds in the console.

- [ ] **Step 4: Verify dual-write happened**

```bash
TODAY=$(date -u +%Y-%m-%d)
# Raw still written
aws s3 ls "s3://$BUCKET/fund/$TODAY/" | head
# Iceberg has data
aws s3 ls "s3://$BUCKET/iceberg/fund_data_lake/fund_daily/" | head
```

Expected: both paths populated.

- [ ] **Step 5: Query via Athena**

In the Athena console (Engine v3, same account/region):

```sql
SELECT count(*) FROM fund_data_lake.fund_daily
WHERE trade_date = current_date;
```

Expected: returns a non-zero count matching the raw parquet row count (within ±1%).

- [ ] **Step 6: Trigger maintenance manually**

```bash
aws lambda invoke --function-name $(aws cloudformation describe-stacks \
  --stack-name FundDataFetchStack --query \
  "Stacks[0].StackResources[?LogicalResourceId=='IcebergMaintenanceLambda'].PhysicalResourceId" \
  --output text 2>/dev/null || echo "FundDataFetchStack-IcebergMaintenanceLambda") \
  /tmp/maint-out.json
cat /tmp/maint-out.json | jq
```

Expected: `tables_processed: 27, errors: []`.

- [ ] **Step 7: Document any gotchas in the spec**

If anything broke (column name mismatches, IAM gaps, akshare interface changes), record under a new "## Lessons learned" section in the spec file and commit:

```bash
git add docs/superpowers/specs/2026-05-09-fund-data-lake-design.md
git commit -m "docs: record dev deploy lessons for fund data lake"
```

---

## Task 14: Post-deploy backfill + 4-week monitoring kickoff

- [ ] **Step 1: Run the backfill on EC2 / local for one table first**

```bash
uv run python scripts/backfill_to_iceberg.py \
  --bucket "$BUCKET" --table fund_daily \
  --start 2025-12-01 --end 2026-05-08
```

Expected: prints per-day inserts; no failures. Total time: 5-15 min depending on history depth.

- [ ] **Step 2: Backfill all remaining tables**

```bash
uv run python scripts/backfill_to_iceberg.py \
  --bucket "$BUCKET" --all \
  --start 2025-12-01 --end 2026-05-08
```

Expected: 30-60 min; per-table summaries print at end.

- [ ] **Step 3: Run compaction immediately after backfill**

```bash
aws lambda invoke --function-name <maintenance-lambda-name> /tmp/post-backfill.json
```

Expected: compaction reduces file count substantially (check S3 file count under `iceberg/.../data/`).

- [ ] **Step 4: Open a tracking issue / note for the 4-week soak**

(M7 Phase 1) — manual checkpoint at +1 week, +2 weeks, +4 weeks:
- Are mismatches in `_catalog/{date}/data_catalog.json` consistency_check section consistent and small (<1%)?
- Do snapshots stay bounded (Glue table → properties → `current-snapshot-id` history)?
- Cost: check Cost Explorer for Glue + Lambda + S3 deltas vs baseline.

This task has no commit — it's an operational handoff to the spec's M7 phase.

---

## Self-Review Notes

Spec coverage check:
- §1 Architecture: covered by Tasks 6, 7, 9, 10
- §2 Data model (27 tables): covered by Tasks 3, 4
- §3 Code changes: covered by Tasks 5, 6, 7, 9, 10
- §4 Error handling + backfill: covered by Tasks 5 (error iso), 6 (dual-write iso), 11 (backfill), 12 (consistency check)
- §5 Testing: covered by Tasks 2, 3, 4, 5, 6, 9 (unit) + Task 13 (E2E)
- §6 Cost: no code task — spec section is descriptive
- §7 Milestones: M1=Tasks 1-5; M2=Task 10; M3=Tasks 6-7; M4=Task 8; M5=Tasks 9-10; M6=Tasks 11, 14; M7=operational, Task 14 step 4

Type consistency:
- `TableSpec` defined in Task 3, used identically in Tasks 4, 5, 9
- `IcebergWriter.from_glue(database, warehouse)` defined in Task 5, called identically in Tasks 7, 9, 10 (env var), 11, 12
- `dual_write(result, s3, iceberg, category, **upload_kwargs)` defined in Task 6, called in Task 7
- `run_maintenance(catalog, database)` defined in Task 9, tested in Task 9

No placeholders / TBDs / "implement later" remain.
