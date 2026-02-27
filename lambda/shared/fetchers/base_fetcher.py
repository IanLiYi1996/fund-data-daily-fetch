from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
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

    def __post_init__(self) -> None:
        if self.data is not None:
            self.row_count = len(self.data)


@dataclass
class FetchSummary:
    """Summary of all fetch operations for a category."""

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
    """Abstract base class for data fetchers."""

    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    @property
    @abstractmethod
    def category(self) -> str:
        """Return the category name for this fetcher (e.g., 'fund', 'stock', 'macro')."""
        pass

    @abstractmethod
    def fetch_all(self) -> FetchSummary:
        """Fetch all data for this category.

        Returns:
            FetchSummary containing results for all data types
        """
        pass

    def _safe_fetch(
        self, name: str, fetch_func: callable, *args, **kwargs
    ) -> FetchResult:
        """Safely execute a fetch function and return a FetchResult.

        Args:
            name: Name of the data being fetched
            fetch_func: Function to call for fetching data
            *args, **kwargs: Arguments to pass to fetch_func

        Returns:
            FetchResult with success or error information
        """
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
