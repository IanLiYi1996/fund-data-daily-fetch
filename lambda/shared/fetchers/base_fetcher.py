"""Base fetcher with dual-write helper (raw S3 parquet + Iceberg)."""
from __future__ import annotations

import http.client
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Optional

import pandas as pd
import requests
import urllib3


from shared.utils.logger import get_logger

# Upstreams (eastmoney especially) sometimes cut a connection mid-response.
# fund_money_fund_daily_em died ~1s in with "Response ended prematurely" on
# 2026-10-17 and 2026-10-20 and the table missed 10-20; a manual re-run
# minutes later fetched all 544 rows. These fast connection drops are worth
# retrying. Timeouts and HTTP status errors are not: a timeout already costs
# its full wait, and 4xx/5xx from a dead endpoint (push2delay 502s, Sep-Oct
# 2026) do not clear in seconds.
TRANSIENT_MAX_ATTEMPTS = 3
TRANSIENT_BACKOFF_SECONDS = (2.0, 5.0)
# Only retry when the failed attempt was quick. A drop after minutes of
# work could push a partition past the 15 min Lambda timeout on re-run.
TRANSIENT_RETRY_MAX_ELAPSED = 60.0

_TRANSIENT_TYPES = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    urllib3.exceptions.ProtocolError,
    http.client.RemoteDisconnected,
    http.client.IncompleteRead,
    ConnectionResetError,
)


def _is_transient(exc: BaseException) -> bool:
    """True if exc, or anything in its cause/context chain, is a connection drop."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        # ConnectTimeout subclasses ConnectionError; timeouts are excluded.
        if isinstance(exc, _TRANSIENT_TYPES) and not isinstance(
            exc, requests.exceptions.Timeout
        ):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


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
        """Invoke a fetch function and wrap success/failure in a FetchResult.

        Fast connection drops are retried up to TRANSIENT_MAX_ATTEMPTS times.
        """
        try:
            self.logger.info(f"Fetching {name}...")
            df = self._call_with_retry(name, fetch_func, *args, **kwargs)
            if df is None or df.empty:
                self.logger.warning(f"{name}: No data returned")
                return FetchResult(name=name, success=True, data=pd.DataFrame())
            self.logger.info(f"{name}: Fetched {len(df)} rows")
            return FetchResult(name=name, data=df, success=True)
        except Exception as e:
            self.logger.error(f"{name}: Failed to fetch - {e}")
            return FetchResult(name=name, success=False, error=str(e))

    def _call_with_retry(self, name: str, fetch_func, *args, **kwargs):
        for attempt in range(1, TRANSIENT_MAX_ATTEMPTS + 1):
            started = time.monotonic()
            try:
                return fetch_func(*args, **kwargs)
            except Exception as e:
                elapsed = time.monotonic() - started
                if (
                    attempt >= TRANSIENT_MAX_ATTEMPTS
                    or elapsed > TRANSIENT_RETRY_MAX_ELAPSED
                    or not _is_transient(e)
                ):
                    raise
                delay = TRANSIENT_BACKOFF_SECONDS[
                    min(attempt - 1, len(TRANSIENT_BACKOFF_SECONDS) - 1)
                ]
                self.logger.warning(
                    f"{name}: transient {type(e).__name__} after {elapsed:.1f}s "
                    f"(attempt {attempt}/{TRANSIENT_MAX_ATTEMPTS}): {e}; "
                    f"retrying in {delay:.0f}s"
                )
                time.sleep(delay)

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
            iceberg_out = iceberg_writer.write(
                result.name, result.data,
                fetch_date=upload_kwargs.get("date"),
            )
        except Exception as e:
            self.logger.error(f"{result.name}: iceberg write failed - {e}")
            iceberg_out = {"error": str(e)}

        result.raw_result = raw_out
        result.iceberg_result = iceberg_out
        return {"raw": raw_out, "iceberg": iceberg_out}
