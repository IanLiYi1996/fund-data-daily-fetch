import akshare as ak
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


class StockFetcher(BaseFetcher):
    """Fetcher for stock index data from akshare."""

    @property
    def category(self) -> str:
        return "stock"

    def fetch_all(self) -> FetchSummary:
        """Fetch all stock index data.

        Returns:
            FetchSummary containing results for:
            - stock_index_sh: Shanghai stock indices
            - stock_index_sz: Shenzhen stock indices
            - stock_market_activity: Market activity data
        """
        results = []

        # Shanghai stock indices (上证系列指数)
        results.append(
            self._safe_fetch("stock_index_sh", self._fetch_stock_index_sh)
        )

        # Shenzhen stock indices (深证系列指数)
        results.append(
            self._safe_fetch("stock_index_sz", self._fetch_stock_index_sz)
        )

        # Market activity (市场活跃度)
        results.append(
            self._safe_fetch("stock_market_activity", self._fetch_market_activity)
        )

        return FetchSummary(category=self.category, results=results)

    def _fetch_stock_index_sh(self):
        """Fetch Shanghai stock indices."""
        df = ak.stock_zh_index_spot_em(symbol="上证系列指数")
        return df

    def _fetch_stock_index_sz(self):
        """Fetch Shenzhen stock indices."""
        df = ak.stock_zh_index_spot_em(symbol="深证系列指数")
        return df

    def _fetch_market_activity(self):
        """Fetch stock market activity data."""
        import pandas as pd
        df = ak.stock_market_activity_legu()
        # Clean percentage strings (e.g., '63.67%' -> 63.67) for all columns
        for col in df.columns:
            try:
                # Convert to string first to handle mixed types
                str_series = df[col].astype(str)
                # Check if any values contain '%'
                if str_series.str.contains('%', na=False).any():
                    # Remove '%' and convert to float
                    df[col] = pd.to_numeric(
                        str_series.str.replace('%', '', regex=False),
                        errors='coerce'
                    )
            except Exception:
                pass  # Keep original if conversion fails
        return df
