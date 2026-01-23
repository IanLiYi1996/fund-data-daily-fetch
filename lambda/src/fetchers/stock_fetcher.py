import akshare as ak
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


class StockFetcher(BaseFetcher):
    """Fetcher for stock index data from akshare."""

    DATA_CATALOG = {
        "stock_index_sh": {
            "name_cn": "上证系列指数",
            "description": "上海证券交易所系列指数实时行情，包含上证指数、上证50等",
            "source_api": "stock_zh_index_spot_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌额", "涨跌幅", "成交量", "成交额"],
        },
        "stock_index_sz": {
            "name_cn": "深证系列指数",
            "description": "深圳证券交易所系列指数实时行情，包含深证成指、创业板指等",
            "source_api": "stock_zh_index_spot_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌额", "涨跌幅", "成交量", "成交额"],
        },
        "stock_market_activity": {
            "name_cn": "市场活跃度",
            "description": "A股市场活跃度数据，包含涨跌家数、涨停跌停统计等",
            "source_api": "stock_market_activity_legu",
            "update_frequency": "daily",
            "key_fields": ["上涨家数", "下跌家数", "涨停家数", "跌停家数", "换手率"],
        },
    }

    @classmethod
    def get_data_catalog(cls) -> dict:
        """Get the data catalog with descriptions for all data sources."""
        return {
            "category": "stock",
            "category_cn": "股票指数数据",
            "description": "A股市场指数行情和市场活跃度数据",
            "data_sources": cls.DATA_CATALOG,
            "total_sources": len(cls.DATA_CATALOG),
        }

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
