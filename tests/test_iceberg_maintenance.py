"""Tests for the iceberg-maintenance Lambda handler logic."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch


# Add the iceberg-maintenance handler directory to sys.path so we can import
# its top-level `handler` module directly (it's not a package).
_MAINT_DIR = Path(__file__).resolve().parent.parent / "lambda" / "iceberg-maintenance"
if str(_MAINT_DIR) not in sys.path:
    sys.path.insert(0, str(_MAINT_DIR))


def test_run_maintenance_iterates_all_tables():
    """Every registered table is visited and has its snapshots expired.

    Compaction itself is NOT asserted here: compact_table skips tables under
    COMPACT_FILE_COUNT_THRESHOLD, and a MagicMock table reports 0 files, so a
    mocked run legitimately compacts nothing. These assertions previously
    expected `rewrite_data_files` on every table, which was stale twice over —
    compaction moved to read-all + `overwrite()`, and the threshold was added.
    """
    import handler as handler_mod

    fake_table = MagicMock()
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fake_table

    summary = handler_mod.run_maintenance(catalog=fake_catalog, database="fund_data_lake")

    from shared.schemas import TABLES
    assert fake_catalog.load_table.call_count == len(TABLES)
    assert fake_table.expire_snapshots.call_count == len(TABLES)
    assert summary["tables_processed"] == len(TABLES)
    assert summary["errors"] == []
    # Below-threshold tables are recorded as skipped, not silently omitted.
    assert len(summary["compactions"]) == len(TABLES)
    assert all(c["skipped"] for c in summary["compactions"])


def test_compaction_runs_once_past_the_file_threshold():
    """A table with enough files is compacted via overwrite()."""
    import handler as handler_mod

    fake_table = MagicMock()
    result = handler_mod.compact_table(fake_table)
    assert result["skipped"] is True
    fake_table.overwrite.assert_not_called()

    busy = MagicMock()
    with patch.object(
        handler_mod, "_count_data_files",
        return_value=handler_mod.COMPACT_FILE_COUNT_THRESHOLD,
    ), patch.object(handler_mod, "_table_id_columns", return_value=[]):
        result = handler_mod.compact_table(busy)
    assert result["skipped"] is False
    busy.overwrite.assert_called_once()


def test_one_table_failure_continues_others():
    """One bad table must not abort maintenance for the rest.

    The failure is injected on expire_snapshots rather than
    rewrite_data_files: the latter is no longer called at all, so the old
    injection point meant this test proved nothing.
    """
    import handler as handler_mod
    from shared.schemas import TABLES

    fake_catalog = MagicMock()
    bad_table = MagicMock()
    bad_table.expire_snapshots.side_effect = RuntimeError("boom")
    good_table = MagicMock()
    fake_catalog.load_table.side_effect = (
        [bad_table] + [good_table] * 1000
    )

    summary = handler_mod.run_maintenance(catalog=fake_catalog, database="fund_data_lake")

    assert len(summary["errors"]) == 1
    assert "boom" in summary["errors"][0]["error"]
    assert summary["tables_processed"] == len(TABLES) - 1
