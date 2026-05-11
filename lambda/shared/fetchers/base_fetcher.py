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

        # 1. Raw parquet write (current source of truth)
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

        # 2. Iceberg write (errors isolated; never fail the whole fetch)
        iceberg_out: dict[str, Any]
        try:
            iceberg_out = iceberg_writer.write(result.name, result.data)
        except Exception as e:
            self.logger.error(f"{result.name}: iceberg write failed - {e}")
            iceberg_out = {"error": str(e)}

        result.raw_result = raw_out
        result.iceberg_result = iceberg_out
        return {"raw": raw_out, "iceberg": iceberg_out}
