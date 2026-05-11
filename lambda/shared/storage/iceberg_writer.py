"""Write pandas DataFrames to Iceberg tables via pyiceberg.

Production writes spawn a subprocess per table so that pyiceberg-core
(Rust) segfaults on one table do not take down the whole Lambda invocation.
See ``write()`` for the subprocess entry.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from typing import Any, Optional

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
    "姓名": "manager_name",
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
    """Write DataFrames to Iceberg tables registered in a catalog.

    In-process mode (used by tests and the backfill script): pass a
    pre-constructed ``catalog`` to ``__init__``. ``write()`` then runs
    ``_write_inline()`` directly in the current process.

    Subprocess mode (Lambda runtime, constructed via ``from_glue()``): each
    call to ``write()`` spawns a Python subprocess that loads Glue, imports
    ``_write_inline``, and returns a JSON result. If pyiceberg-core segfaults
    on one table, only that subprocess dies; the caller continues.
    """

    def __init__(
        self,
        catalog: Optional[Catalog] = None,
        database: str = "fund_data_lake",
        warehouse: Optional[str] = None,
        subprocess_mode: bool = False,
    ) -> None:
        self.catalog = catalog
        self.database = database
        self.warehouse = warehouse
        self.subprocess_mode = subprocess_mode
        self.logger = get_logger(self.__class__.__name__)

    @classmethod
    def from_glue(cls, database: str, warehouse: str) -> "IcebergWriter":
        """Construct for the Lambda runtime (subprocess mode).

        We do NOT eagerly load the Glue catalog here because that would defeat
        the whole point of subprocess isolation — a segfault in pyiceberg-core
        during catalog init would crash the parent. Catalog is loaded inside
        each subprocess.
        """
        return cls(
            catalog=None,
            database=database,
            warehouse=warehouse,
            subprocess_mode=True,
        )

    def _load_catalog(self) -> Catalog:
        """Lazily load the Glue catalog (used inside subprocess + tests)."""
        if self.catalog is not None:
            return self.catalog
        self.catalog = load_catalog(
            "glue",
            **{
                "type": "glue",
                "glue.region": os.environ.get("AWS_REGION", "us-east-1"),
                "warehouse": self.warehouse or "",
            },
        )
        return self.catalog

    def write(
        self,
        table_name: str,
        df: pd.DataFrame,
        fetch_date=None,
    ) -> dict[str, Any]:
        """Write ``df`` to the Iceberg table ``table_name``.

        In subprocess_mode this dispatches the real work to a child Python
        process (isolation against pyiceberg-core segfaults). Otherwise it
        runs inline.
        """
        if df is None or df.empty:
            return {"skipped": True, "reason": "empty"}

        if self.subprocess_mode:
            return self._write_via_subprocess(table_name, df, fetch_date)
        return self._write_inline(table_name, df, fetch_date)

    def _write_via_subprocess(
        self,
        table_name: str,
        df: pd.DataFrame,
        fetch_date,
    ) -> dict[str, Any]:
        """Dump df to a tempfile, spawn a Python subprocess to do the write."""
        from datetime import date, datetime
        fd_str = None
        if fetch_date is not None:
            if isinstance(fetch_date, datetime):
                fetch_date = fetch_date.date()
            if isinstance(fetch_date, date):
                fd_str = fetch_date.isoformat()

        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".parquet", dir="/tmp", delete=False
        ) as f:
            tmp_path = f.name
        try:
            df.to_parquet(tmp_path, index=False, engine="pyarrow")
            # Subprocess entry: ``python -m shared.storage.iceberg_writer``
            cmd = [
                sys.executable, "-m", "shared.storage.iceberg_writer",
                "--table", table_name,
                "--database", self.database,
                "--warehouse", self.warehouse or "",
                "--parquet", tmp_path,
            ]
            if fd_str:
                cmd.extend(["--fetch-date", fd_str])
            try:
                proc = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=300,
                )
            except subprocess.TimeoutExpired:
                return {"error": "subprocess timeout (>300s)", "rows_inserted": 0}

            if proc.returncode != 0:
                reason = f"subprocess exitcode={proc.returncode}"
                stderr_tail = (proc.stderr or "").strip().splitlines()[-5:]
                self.logger.error(
                    f"{table_name}: iceberg subprocess failed ({reason}); "
                    f"stderr tail: {stderr_tail}"
                )
                return {"error": reason, "rows_inserted": 0,
                        "stderr": "\n".join(stderr_tail)}
            # Parse JSON from last non-empty stdout line
            out = (proc.stdout or "").strip().splitlines()
            if not out:
                return {"error": "subprocess produced no output", "rows_inserted": 0}
            try:
                return json.loads(out[-1])
            except json.JSONDecodeError as e:
                return {"error": f"subprocess json decode failed: {e}",
                        "stdout_tail": out[-5:], "rows_inserted": 0}
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def _write_inline(
        self,
        table_name: str,
        df: pd.DataFrame,
        fetch_date=None,
    ) -> dict[str, Any]:
        """Normalize, ensure table exists, then upsert/append per spec.

        ``fetch_date`` (a ``date`` or ``datetime``) is used as the fallback
        for partition date columns when the upstream DataFrame does not
        include the date (common for akshare snapshot endpoints like
        ``fund_open_fund_daily_em`` that return "today's values" with no
        date column).
        """
        if df is None or df.empty:
            return {"skipped": True, "reason": "empty"}

        if table_name not in TABLES:
            self.logger.debug(
                f"{table_name}: not registered in Iceberg TABLES; skipping"
            )
            return {"skipped": True, "reason": "not_registered"}

        spec = TABLES[table_name]

        # 1. Rename Chinese columns to canonical names (best-effort)
        renamed = df.rename(columns=_AKSHARE_TO_CANONICAL)

        # 2. Normalize date columns (renames + coerces dtype, drops bad rows)
        from datetime import datetime as _dt, date as _date
        fallback = fetch_date
        if isinstance(fallback, _dt):
            fallback = fallback.date()
        try:
            normalized = normalize(renamed, spec.date_specs, fallback_date=fallback)
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
        projected = normalized[keep_cols].copy()

        # 4b. Coerce numeric columns: akshare often emits '' or '---' as
        # placeholders for missing values, which break pyarrow.cast to double.
        # Use pd.to_numeric(errors='coerce') so placeholders become NaN.
        from pyiceberg.types import DoubleType, FloatType, IntegerType, LongType
        numeric_iceberg_types = (DoubleType, FloatType, IntegerType, LongType)
        for field in spec.schema.fields:
            if field.name in projected.columns and isinstance(
                field.field_type, numeric_iceberg_types
            ):
                projected[field.name] = pd.to_numeric(
                    projected[field.name], errors="coerce"
                )

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
        catalog = self._load_catalog()
        identifier = (self.database, table_name)
        if not catalog.table_exists(identifier):
            self.logger.info(f"Creating Iceberg table {self.database}.{table_name}")
            catalog.create_table(
                identifier=identifier,
                schema=spec.schema,
                partition_spec=spec.partition_spec,
            )
        table = catalog.load_table(identifier)

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


def _subprocess_main() -> int:
    """Entry point for the per-table subprocess.

    Reads parquet from --parquet path, writes into Iceberg via _write_inline,
    prints the result dict as JSON on the last stdout line.
    """
    import argparse
    from datetime import date as _date

    p = argparse.ArgumentParser()
    p.add_argument("--table", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--warehouse", required=True)
    p.add_argument("--parquet", required=True, help="Path to input parquet")
    p.add_argument("--fetch-date", help="YYYY-MM-DD", default=None)
    args = p.parse_args()

    df = pd.read_parquet(args.parquet, engine="pyarrow")
    fd = _date.fromisoformat(args.fetch_date) if args.fetch_date else None

    writer = IcebergWriter(
        catalog=None, database=args.database,
        warehouse=args.warehouse, subprocess_mode=False,
    )
    result = writer._write_inline(args.table, df, fetch_date=fd)
    # JSON-serialise numeric types (rows_inserted etc are already ints)
    print(json.dumps(result, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(_subprocess_main())
