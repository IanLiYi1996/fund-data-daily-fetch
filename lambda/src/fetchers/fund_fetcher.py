import akshare as ak
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


class FundFetcher(BaseFetcher):
    """Fetcher for fund-related data from akshare."""

    @property
    def category(self) -> str:
        return "fund"

    def fetch_all(self) -> FetchSummary:
        """Fetch all fund data.

        Returns:
            FetchSummary containing results for:
            - fund_nav: Open fund NAV data
            - fund_performance: Fund performance ranking
            - fund_etf: ETF spot data
            - fund_name: Fund basic information
            - fund_manager: Fund manager information
        """
        results = []

        # Fund NAV data (开放式基金净值)
        results.append(
            self._safe_fetch("fund_nav", self._fetch_fund_nav)
        )

        # Fund performance ranking (基金业绩排名)
        results.append(
            self._safe_fetch("fund_performance", self._fetch_fund_performance)
        )

        # ETF spot data (ETF 实时数据)
        results.append(
            self._safe_fetch("fund_etf", self._fetch_fund_etf)
        )

        # Fund basic info (基金基本信息)
        results.append(
            self._safe_fetch("fund_name", self._fetch_fund_name)
        )

        # Fund manager info (基金经理信息)
        results.append(
            self._safe_fetch("fund_manager", self._fetch_fund_manager)
        )

        return FetchSummary(category=self.category, results=results)

    def _fetch_fund_nav(self):
        """Fetch open fund NAV data."""
        # 开放式基金实时净值
        df = ak.fund_open_fund_info_em(symbol="000001", indicator="单位净值走势")
        # For demo, fetch basic info. In production, may need to iterate funds
        return df

    def _fetch_fund_performance(self):
        """Fetch fund performance ranking data."""
        # 开放式基金排行
        df = ak.fund_open_fund_rank_em(symbol="全部")
        return df

    def _fetch_fund_etf(self):
        """Fetch ETF spot data."""
        # ETF 基金实时行情
        df = ak.fund_etf_spot_em()
        return df

    def _fetch_fund_name(self):
        """Fetch fund basic information."""
        # 基金名称列表
        df = ak.fund_name_em()
        return df

    def _fetch_fund_manager(self):
        """Fetch fund manager information."""
        # 基金经理排行榜
        df = ak.fund_manager_em()
        return df
