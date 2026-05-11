"""Tests for the iceberg-maintenance Lambda handler logic."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock


# Add the iceberg-maintenance handler directory to sys.path so we can import
# its top-level `handler` module directly (it's not a package).
_MAINT_DIR = Path(__file__).resolve().parent.parent / "lambda" / "iceberg-maintenance"
if str(_MAINT_DIR) not in sys.path:
    sys.path.insert(0, str(_MAINT_DIR))


def test_run_maintenance_iterates_all_tables():
    import handler as handler_mod

    fake_table = MagicMock()
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fake_table

    summary = handler_mod.run_maintenance(catalog=fake_catalog, database="fund_data_lake")

    from shared.schemas import TABLES
    assert fake_catalog.load_table.call_count == len(TABLES)
    assert fake_table.rewrite_data_files.call_count == len(TABLES)
    assert fake_table.expire_snapshots.call_count == len(TABLES)
    assert summary["tables_processed"] == len(TABLES)
    assert summary["errors"] == []


def test_one_table_failure_continues_others():
    import handler as handler_mod

    fake_catalog = MagicMock()
    bad_table = MagicMock()
    bad_table.rewrite_data_files.side_effect = RuntimeError("boom")
    good_table = MagicMock()
    # First load_table returns bad_table, rest return good_table
    fake_catalog.load_table.side_effect = (
        [bad_table] + [good_table] * 1000
    )

    summary = handler_mod.run_maintenance(catalog=fake_catalog, database="fund_data_lake")

    assert len(summary["errors"]) == 1
    assert "boom" in summary["errors"][0]["error"]
    # Still processed remaining 26
    assert summary["tables_processed"] >= 26
