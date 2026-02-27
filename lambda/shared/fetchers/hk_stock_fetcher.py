import akshare as ak
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


class HKStockFetcher(BaseFetcher):
    """Fetcher for Hong Kong stock market data from akshare.

    Ported from reference/investment-advisory hk_stocks module.
    """

    DATA_CATALOG = {
        "hk_spot": {
            "name_cn": "港股实时行情",
            "description": "全部港股实时行情数据，包含价格、涨跌幅、成交量等",
            "source_api": "stock_hk_spot_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅", "换手率"],
        },
        "hk_index": {
            "name_cn": "港股指数行情",
            "description": "香港主要股票指数实时行情，包含恒生指数、国企指数等",
            "source_api": "stock_hk_index_spot_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌额", "涨跌幅", "昨收", "今开"],
        },
        "hk_hot_rank": {
            "name_cn": "港股人气排名",
            "description": "港股人气排名数据，反映市场关注热度",
            "source_api": "stock_hk_hot_rank_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅", "排名"],
        },
        "hk_ggt_components": {
            "name_cn": "港股通成分股",
            "description": "沪港通/深港通标的港股成分股列表",
            "source_api": "stock_hk_ggt_components_em",
            "update_frequency": "daily",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅"],
        },
        "hk_main_board": {
            "name_cn": "港股主板实时行情",
            "description": "港股主板全部股票实时行情数据",
            "source_api": "stock_hk_main_board_spot_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅", "成交量", "成交额"],
        },
    }

    @classmethod
    def get_data_catalog(cls) -> dict:
        return {
            "category": "hk_stock",
            "category_cn": "港股市场数据",
            "description": "香港股票市场行情、指数、港股通、人气排名等数据",
            "data_sources": cls.DATA_CATALOG,
            "total_sources": len(cls.DATA_CATALOG),
        }

    @property
    def category(self) -> str:
        return "hk_stock"

    def fetch_all(self) -> FetchSummary:
        results = []
        results.append(self._safe_fetch("hk_spot", self._fetch_hk_spot))
        results.append(self._safe_fetch("hk_index", self._fetch_hk_index))
        results.append(self._safe_fetch("hk_hot_rank", self._fetch_hk_hot_rank))
        results.append(self._safe_fetch("hk_ggt_components", self._fetch_hk_ggt_components))
        results.append(self._safe_fetch("hk_main_board", self._fetch_hk_main_board))
        return FetchSummary(category=self.category, results=results)

    def _fetch_hk_spot(self):
        """Fetch all HK stock spot data. (reference: akshare_client.py get_hk_quote)"""
        df = ak.stock_hk_spot_em()
        return df

    def _fetch_hk_index(self):
        """Fetch HK stock index data."""
        df = ak.stock_hk_index_spot_em()
        return df

    def _fetch_hk_hot_rank(self):
        """Fetch HK stock hot rank data."""
        df = ak.stock_hk_hot_rank_em()
        return df

    def _fetch_hk_ggt_components(self):
        """Fetch HK-SH/SZ Connect components."""
        df = ak.stock_hk_ggt_components_em()
        return df

    def _fetch_hk_main_board(self):
        """Fetch HK main board spot data."""
        df = ak.stock_hk_main_board_spot_em()
        return df
