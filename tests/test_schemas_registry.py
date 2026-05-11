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
