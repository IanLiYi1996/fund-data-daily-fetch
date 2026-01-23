import akshare as ak
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


class MacroFetcher(BaseFetcher):
    """Fetcher for macroeconomic data from akshare."""

    @property
    def category(self) -> str:
        return "macro"

    def fetch_all(self) -> FetchSummary:
        """Fetch all macroeconomic data.

        Returns:
            FetchSummary containing results for:
            - macro_lpr: LPR interest rate data
            - macro_cpi: CPI inflation data
            - macro_ppi: PPI index data
        """
        results = []

        # LPR interest rate (LPR 利率)
        results.append(
            self._safe_fetch("macro_lpr", self._fetch_lpr)
        )

        # CPI inflation (CPI 通胀)
        results.append(
            self._safe_fetch("macro_cpi", self._fetch_cpi)
        )

        # PPI index (PPI 指数)
        results.append(
            self._safe_fetch("macro_ppi", self._fetch_ppi)
        )

        return FetchSummary(category=self.category, results=results)

    def _fetch_lpr(self):
        """Fetch LPR interest rate data."""
        df = ak.macro_china_lpr()
        return df

    def _fetch_cpi(self):
        """Fetch CPI inflation data."""
        df = ak.macro_china_cpi()
        return df

    def _fetch_ppi(self):
        """Fetch PPI index data."""
        df = ak.macro_china_ppi()
        return df
