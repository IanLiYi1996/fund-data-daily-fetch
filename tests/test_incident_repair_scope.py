"""Exercise the exact incident deletion against a real local Iceberg table."""
from __future__ import annotations

import importlib.util
import io
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import And, In

from shared.storage.iceberg_writer import IcebergWriter


def _repair():
    path = Path(__file__).resolve().parents[1] / "scripts/repair_fund_data_20261030.py"
    spec = importlib.util.spec_from_file_location("incident_repair_scope", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_delete_is_bounded_to_exact_dates_and_codes_and_preserves_old_snapshot(tmp_path):
    repair = _repair()
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog("repair-test", **{
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{warehouse}",
    })
    catalog.create_namespace("fund_data_lake")
    writer = IcebergWriter(catalog=catalog)
    rows = [(code, day) for code in repair.BAD_CODES for day in repair.BAD_DATES]
    # Valid rows on affected dates and an out-of-scope currency key must
    # survive; the production preflight separately rejects extra pollution.
    keep = [("968008", date(2026, 10, 28)), ("968063", date(2026, 10, 26)),
            ("人民币", date(2026, 10, 26))]
    all_rows = rows + keep
    frame = pd.DataFrame({
        "基金代码": [code for code, _ in all_rows],
        "基金简称": ["fixture"] * len(all_rows),
        "数据日期": [day for _, day in all_rows],
        "单位净值": [1.0] * len(all_rows),
    })
    writer.write("fund_hk_rank", frame)
    table = catalog.load_table(("fund_data_lake", "fund_hk_rank"))
    original = table.scan().to_arrow()
    snapshot = table.current_snapshot().snapshot_id
    backup = repair._parquet(original)
    assert pq.read_table(io.BytesIO(backup)).equals(original)
    table.delete(And(In("fund_code", repair.BAD_CODES),
                     In("snapshot_date", repair.BAD_DATES)))
    remaining = table.scan().to_arrow().to_pylist()
    assert {(r["fund_code"], r["snapshot_date"]) for r in remaining} == set(keep)
    assert table.scan(snapshot_id=snapshot).to_arrow().equals(original)
