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
        """Fetch all stock index data."""
        results = []
        results.append(self._safe_fetch("stock_index_sh", self._fetch_stock_index_sh))
        results.append(self._safe_fetch("stock_index_sz", self._fetch_stock_index_sz))
        results.append(self._safe_fetch("stock_market_activity", self._fetch_market_activity))
        return FetchSummary(category=self.category, results=results)

    def _fetch_stock_index_sh(self):
        df = ak.stock_zh_index_spot_em(symbol="上证系列指数")
        return df

    def _fetch_stock_index_sz(self):
        df = ak.stock_zh_index_spot_em(symbol="深证系列指数")
        return df

    def _fetch_market_activity(self):
        import pandas as pd
        df = ak.stock_market_activity_legu()
        for col in df.columns:
            try:
                str_series = df[col].astype(str)
                if str_series.str.contains('%', na=False).any():
                    df[col] = pd.to_numeric(
                        str_series.str.replace('%', '', regex=False),
                        errors='coerce'
                    )
            except Exception:
                pass
        return df
