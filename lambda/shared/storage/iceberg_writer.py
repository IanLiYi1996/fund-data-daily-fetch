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
    "简称": "fund_name",
    "单位净值": "unit_nav",
    "累计净值": "accum_nav",
    # daily return has at least 3 akshare aliases in different endpoints
    "日增长率": "daily_return_pct",
    "日涨幅": "daily_return_pct",
    "增长率": "daily_return_pct",
    "申购状态": "subscription_status",
    "赎回状态": "redemption_status",
    "手续费": "fee",
    "最新价": "latest_price",
    "涨跌幅": "change_pct",
    "成交量": "volume",
    "成交额": "turnover",
    "市价": "market_price",
    "折溢价率": "premium_pct",
    "折价率": "premium_pct",
    "万份收益": "ten_thousand_yield",
    "7日年化": "annual_yield_7d",
    "7日年化%": "annual_yield_7d",
    "年化收益率7日": "annual_yield_7d",
    "估算净值": "estimated_nav",
    "估算涨跌幅": "estimated_change_pct",
    "近1周": "weekly_return",
    "近1月": "monthly_return",
    "近1年": "yearly_return",
    # fund_dividend → akshare returns 分红 / 权益登记日 / 除息日期 / 分红发放日
    "分红金额": "dividend_amount",
    "分红": "dividend_amount",
    "除息日": "event_date",
    "除息日期": "event_date",
    "发放日": "payment_date",
    "分红发放日": "payment_date",
    # fund_split → akshare returns 拆分折算 / 拆分折算日
    "拆分比例": "split_ratio",
    "拆分折算": "split_ratio",
    "拆分日期": "event_date",
    "拆分折算日": "event_date",
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
    "现任基金资产总规模": "aum",
    "任职回报": "tenure_return",
    "现任基金最佳回报": "tenure_return",
    "评级机构": "rating_agency",
    "评级": "rating",
    "基金类型": "fund_type",
    # K-line aliases
    "日期": "trade_date",
    "开盘": "open", "最高": "high", "最低": "low", "收盘": "close",
}


# Regex: matches akshare "YYYY-MM-DD-<metric>" or "YYYY-MM-DD--<metric>"
# column names (e.g. "2026-05-08-单位净值", "2026-05-07--累计净值").
_DATE_PREFIX_COL_RE = __import__("re").compile(
    r"^(\d{4}-\d{2}-\d{2})-+(?P<metric>.+)$"
)


_PLACEHOLDER_VALUES: frozenset = frozenset({"", "---", "--", "-", "nan", "NaN", "None"})


def _col_has_real_values(series) -> bool:
    """True if the series has any non-placeholder value."""
    if series is None or len(series) == 0:
        return False
    # Fast path: count non-placeholder rows
    s = series.astype(str)
    mask = ~s.isin(_PLACEHOLDER_VALUES)
    return bool(mask.any())


def _strip_date_prefix_keep_latest(df: "pd.DataFrame") -> "pd.DataFrame":
    """Collapse akshare's "YYYY-MM-DD-单位净值" columns to bare "单位净值".

    Several akshare endpoints (fund_open_fund_daily_em, fund_etf_fund_daily_em,
    fund_money_fund_daily_em, fund_graded_fund_daily_em) return today's and
    yesterday's snapshots as PARALLEL columns:
        基金代码 | 基金简称 | 2026-05-11-单位净值 | 2026-05-08-单位净值
    The LATEST date often holds placeholders ('---', empty) until market
    close; earlier dates are populated. So for each metric we pick the
    column with the most-recent date that ALSO has real (non-placeholder)
    values, falling back to newest if all are empty.
    """
    if df is None or df.empty:
        return df
    # Group all prefixed columns by metric → list[(date, col)] sorted desc by date
    by_metric: dict[str, list[tuple[str, str]]] = {}
    for col in df.columns:
        m = _DATE_PREFIX_COL_RE.match(str(col))
        if m is None:
            continue
        by_metric.setdefault(m.group("metric"), []).append((m.group(1), col))
    if not by_metric:
        return df

    keep: dict[str, str] = {}  # metric → original col to keep
    for metric, dated_cols in by_metric.items():
        dated_cols.sort(key=lambda x: x[0], reverse=True)  # newest first
        chosen = dated_cols[0][1]  # default: newest date
        for _, col in dated_cols:
            if _col_has_real_values(df[col]):
                chosen = col
                break
        keep[metric] = chosen

    rename = {orig: metric for metric, orig in keep.items()}
    drop_cols = [
        c for c in df.columns
        if _DATE_PREFIX_COL_RE.match(str(c)) and c not in rename
    ]
    return df.drop(columns=drop_cols).rename(columns=rename)


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

    # Tables that have been observed to SIGSEGV in pyiceberg-core during
    # upsert. Only these go through the subprocess isolation path in prod;
    # everything else runs in-process for speed (subprocess fork+init is
    # ~5s/table which pushes total fund-fetcher runtime past the Lambda
    # 15-min hard limit).
    _KNOWN_SIGSEGV_TABLES: frozenset = frozenset({
        "fund_performance",
        "fund_name",
        "fund_daily",
        "fund_value_estimation",
    })

    def write(
        self,
        table_name: str,
        df: pd.DataFrame,
        fetch_date=None,
    ) -> dict[str, Any]:
        """Write ``df`` to the Iceberg table ``table_name``.

        Subprocess path is ONLY used for tables empirically known to
        SIGSEGV in pyiceberg-core (see ``_KNOWN_SIGSEGV_TABLES``).
        All others run in-process to stay within Lambda's 15-min timeout.
        """
        if df is None or df.empty:
            return {"skipped": True, "reason": "empty"}

        if self.subprocess_mode and table_name in self._KNOWN_SIGSEGV_TABLES:
            return self._write_via_subprocess(table_name, df, fetch_date)
        return self._write_inline(table_name, df, fetch_date)

    def _write_via_subprocess(
        self,
        table_name: str,
        df: pd.DataFrame,
        fetch_date,
    ) -> dict[str, Any]:
        """Dump df to a tempfile, spawn a Python subprocess to do the write.

        On SIGSEGV (exitcode -11) — a known pyiceberg-core Rust bug on upsert
        for certain tables — retry once in force-append mode so at least the
        data lands. Downstream deduplication handles the resulting duplicates.
        """
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
            result = self._run_subprocess(table_name, tmp_path, fd_str)
            # pyiceberg-core SIGSEGV (-11) on upsert: retry in append mode
            if result.get("error", "").endswith("exitcode=-11"):
                self.logger.warning(
                    f"{table_name}: upsert SIGSEGV, retrying as append"
                )
                result = self._run_subprocess(
                    table_name, tmp_path, fd_str, force_append=True,
                )
                if "error" not in result:
                    result["fallback"] = "append_after_sigsegv"
            return result
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def _run_subprocess(
        self,
        table_name: str,
        tmp_path: str,
        fd_str: Optional[str],
        force_append: bool = False,
    ) -> dict[str, Any]:
        cmd = [
            sys.executable, "-m", "shared.storage.iceberg_writer",
            "--table", table_name,
            "--database", self.database,
            "--warehouse", self.warehouse or "",
            "--parquet", tmp_path,
        ]
        if fd_str:
            cmd.extend(["--fetch-date", fd_str])
        if force_append:
            cmd.append("--force-append")
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=900,
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
        out = (proc.stdout or "").strip().splitlines()
        if not out:
            return {"error": "subprocess produced no output", "rows_inserted": 0}
        try:
            return json.loads(out[-1])
        except json.JSONDecodeError as e:
            return {"error": f"subprocess json decode failed: {e}",
                    "stdout_tail": out[-5:], "rows_inserted": 0}

    def _write_inline(
        self,
        table_name: str,
        df: pd.DataFrame,
        fetch_date=None,
        force_append: bool = False,
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

        # 1a. Collapse akshare's "YYYY-MM-DD-metric" columns to bare "metric"
        # (keeping only the latest date per metric). Affects daily-NAV
        # endpoints that return today + yesterday side by side.
        pre = _strip_date_prefix_keep_latest(df)

        # 1b. Rename Chinese columns to canonical names (best-effort)
        renamed = pre.rename(columns=_AKSHARE_TO_CANONICAL)

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
        # placeholders for missing values, or tacks on '%' for percentages
        # (e.g. '0.01%', '-0.02%'). Strip '%' and use pd.to_numeric with
        # errors='coerce' so placeholders become NaN.
        from pyiceberg.types import DoubleType, FloatType, IntegerType, LongType
        numeric_iceberg_types = (DoubleType, FloatType, IntegerType, LongType)
        for field in spec.schema.fields:
            if field.name in projected.columns and isinstance(
                field.field_type, numeric_iceberg_types
            ):
                col = projected[field.name]
                if pd.api.types.is_string_dtype(col) or col.dtype == object:
                    # Strip '%' suffix on string values before numeric coerce
                    col = col.astype(str).str.rstrip("%")
                projected[field.name] = pd.to_numeric(col, errors="coerce")

        # 4c. Pad missing (nullable) schema columns with None so the Arrow
        # table always has EXACTLY the target schema's field set in the same
        # order. pyiceberg.upsert() calls source_table.cast(target_schema)
        # which requires strict field-name match; partial projections fail.
        for field in spec.schema.fields:
            if field.name not in projected.columns:
                projected[field.name] = None
        # Reorder to match schema field order exactly
        projected = projected[schema_cols]

        # 5. Build Arrow table aligned to Iceberg schema
        arrow_table = pa.Table.from_pandas(projected, preserve_index=False)
        iceberg_arrow_schema = spec.schema.as_arrow()
        # Cast to align dtypes (e.g., Python date → Arrow date32)
        arrow_table = arrow_table.cast(iceberg_arrow_schema)

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

        # 7. Write per spec.write_mode (or force-append override)
        if spec.write_mode == "upsert" and not force_append:
            result = table.upsert(arrow_table)
            return {
                "rows_inserted": result.rows_inserted,
                "rows_updated": result.rows_updated,
            }
        else:
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
    p.add_argument("--force-append", action="store_true",
                   help="Force append mode (fallback after pyiceberg upsert SIGSEGV)")
    args = p.parse_args()

    df = pd.read_parquet(args.parquet, engine="pyarrow")
    fd = _date.fromisoformat(args.fetch_date) if args.fetch_date else None

    writer = IcebergWriter(
        catalog=None, database=args.database,
        warehouse=args.warehouse, subprocess_mode=False,
    )
    result = writer._write_inline(
        args.table, df, fetch_date=fd, force_append=args.force_append,
    )
    # JSON-serialise numeric types (rows_inserted etc are already ints)
    print(json.dumps(result, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(_subprocess_main())
