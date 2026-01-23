import akshare as ak
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


class MacroFetcher(BaseFetcher):
    """Fetcher for macroeconomic data from akshare."""

    DATA_CATALOG = {
        "macro_lpr": {
            "name_cn": "LPR利率",
            "description": "贷款市场报价利率(LPR)历史数据，包含1年期和5年期LPR",
            "source_api": "macro_china_lpr",
            "update_frequency": "monthly",
            "key_fields": ["日期", "LPR1年", "LPR5年"],
        },
        "macro_cpi": {
            "name_cn": "CPI通胀数据",
            "description": "居民消费价格指数(CPI)历史数据，反映通货膨胀水平",
            "source_api": "macro_china_cpi",
            "update_frequency": "monthly",
            "key_fields": ["日期", "当月同比", "当月环比", "累计同比"],
        },
        "macro_ppi": {
            "name_cn": "PPI指数",
            "description": "工业生产者出厂价格指数(PPI)历史数据",
            "source_api": "macro_china_ppi",
            "update_frequency": "monthly",
            "key_fields": ["日期", "当月同比", "当月环比", "累计同比"],
        },
    }

    @classmethod
    def get_data_catalog(cls) -> dict:
        """Get the data catalog with descriptions for all data sources."""
        return {
            "category": "macro",
            "category_cn": "宏观经济数据",
            "description": "中国宏观经济指标数据，包括利率、通胀等",
            "data_sources": cls.DATA_CATALOG,
            "total_sources": len(cls.DATA_CATALOG),
        }

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
