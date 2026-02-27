import akshare as ak
from datetime import datetime
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


class AShareFetcher(BaseFetcher):
    """Fetcher for A-share (A股) market data from akshare."""

    DATA_CATALOG = {
        "a_share_spot": {
            "name_cn": "全部A股实时行情",
            "description": "全部A股个股实时行情数据，包含价格、涨跌幅、成交量等",
            "source_api": "stock_zh_a_spot_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅", "换手率", "市盈率"],
        },
        "board_industry": {
            "name_cn": "行业板块",
            "description": "东方财富行业板块行情数据，包含板块涨跌幅和资金流向",
            "source_api": "stock_board_industry_name_em",
            "update_frequency": "realtime",
            "key_fields": ["板块名称", "最新价", "涨跌幅", "总市值", "换手率", "上涨家数", "下跌家数"],
        },
        "board_concept": {
            "name_cn": "概念板块",
            "description": "东方财富概念板块行情数据，包含热门概念涨跌和成分股",
            "source_api": "stock_board_concept_name_em",
            "update_frequency": "realtime",
            "key_fields": ["板块名称", "最新价", "涨跌幅", "总市值", "换手率", "上涨家数", "下跌家数"],
        },
        "zt_pool": {
            "name_cn": "涨停池",
            "description": "当日涨停股票池数据，包含涨停时间、连板天数等",
            "source_api": "stock_zt_pool_em",
            "update_frequency": "daily",
            "key_fields": ["代码", "名称", "涨停价", "最新价", "涨停统计", "连板数", "首次封板时间", "最后封板时间"],
        },
        "dt_pool": {
            "name_cn": "跌停股概览",
            "description": "当日跌停和大面（涨停后跌）股票数据",
            "source_api": "stock_zt_pool_dtgc_em",
            "update_frequency": "daily",
            "key_fields": ["代码", "名称", "涨停价", "最新价", "涨跌幅", "成交额"],
        },
        "hot_rank": {
            "name_cn": "热门股票排名",
            "description": "东方财富个股人气排名数据",
            "source_api": "stock_hot_rank_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅", "排名"],
        },
        "fund_flow_market": {
            "name_cn": "大盘资金流向",
            "description": "沪深两市大盘资金流向数据，包含主力净流入",
            "source_api": "stock_market_fund_flow",
            "update_frequency": "daily",
            "key_fields": ["日期", "上证-Loss净流入", "深证-Loss净流入", "主力净流入", "主力净流入-净额"],
        },
        "fund_flow_sector": {
            "name_cn": "行业资金流向排名",
            "description": "行业板块资金流向排名数据",
            "source_api": "stock_sector_fund_flow_rank",
            "update_frequency": "daily",
            "key_fields": ["名称", "今日涨跌幅", "今日主力净流入-净额", "今日超大单净流入-净额"],
        },
        "margin_sh": {
            "name_cn": "上海融资融券",
            "description": "上海证券交易所融资融券汇总数据",
            "source_api": "stock_margin_sse",
            "update_frequency": "daily",
            "key_fields": ["信用交易日期", "融资余额", "融资买入额", "融券余量", "融券卖出量"],
        },
        "margin_sz": {
            "name_cn": "深圳融资融券",
            "description": "深圳证券交易所融资融券汇总数据",
            "source_api": "stock_margin_szse",
            "update_frequency": "daily",
            "key_fields": ["交易日期", "融资余额", "融资买入额", "融券余量", "融券卖出量"],
        },
        "lhb_detail": {
            "name_cn": "龙虎榜详情",
            "description": "龙虎榜详情数据，包含机构和游资买卖席位",
            "source_api": "stock_lhb_detail_em",
            "update_frequency": "daily",
            "key_fields": ["代码", "名称", "上榜原因", "买入额", "卖出额", "净买额"],
        },
    }

    @classmethod
    def get_data_catalog(cls) -> dict:
        """Get the data catalog with descriptions for all data sources."""
        return {
            "category": "a_share",
            "category_cn": "A股市场数据",
            "description": "A股市场行情、板块、资金流向、涨跌停、龙虎榜等数据",
            "data_sources": cls.DATA_CATALOG,
            "total_sources": len(cls.DATA_CATALOG),
        }

    @property
    def category(self) -> str:
        return "a_share"

    def fetch_all(self) -> FetchSummary:
        """Fetch all A-share market data."""
        results = []

        # A-share spot data (全部A股实时行情)
        results.append(self._safe_fetch("a_share_spot", self._fetch_a_spot))

        # Board data (板块数据)
        results.append(self._safe_fetch("board_industry", self._fetch_board_industry))
        results.append(self._safe_fetch("board_concept", self._fetch_board_concept))

        # Limit pool data (涨跌停池)
        results.append(self._safe_fetch("zt_pool", self._fetch_zt_pool))
        results.append(self._safe_fetch("dt_pool", self._fetch_dt_pool))

        # Hot rank (热门排名)
        results.append(self._safe_fetch("hot_rank", self._fetch_hot_rank))

        # Fund flow (资金流向)
        results.append(self._safe_fetch("fund_flow_market", self._fetch_fund_flow_market))
        results.append(self._safe_fetch("fund_flow_sector", self._fetch_fund_flow_sector))

        # Margin trading (融资融券)
        results.append(self._safe_fetch("margin_sh", self._fetch_margin_sh))
        results.append(self._safe_fetch("margin_sz", self._fetch_margin_sz))

        # Dragon-Tiger list (龙虎榜)
        results.append(self._safe_fetch("lhb_detail", self._fetch_lhb))

        return FetchSummary(category=self.category, results=results)

    def _fetch_a_spot(self):
        """Fetch all A-share spot data."""
        df = ak.stock_zh_a_spot_em()
        return df

    def _fetch_board_industry(self):
        """Fetch industry board data."""
        df = ak.stock_board_industry_name_em()
        return df

    def _fetch_board_concept(self):
        """Fetch concept board data."""
        df = ak.stock_board_concept_name_em()
        return df

    def _fetch_zt_pool(self):
        """Fetch limit-up (涨停) pool data."""
        date_str = datetime.now().strftime("%Y%m%d")
        df = ak.stock_zt_pool_em(date=date_str)
        return df

    def _fetch_dt_pool(self):
        """Fetch limit-down (跌停) pool data via 涨停强度概览."""
        date_str = datetime.now().strftime("%Y%m%d")
        df = ak.stock_zt_pool_dtgc_em(date=date_str)
        return df

    def _fetch_hot_rank(self):
        """Fetch hot stock rank data."""
        df = ak.stock_hot_rank_em()
        return df

    def _fetch_fund_flow_market(self):
        """Fetch market fund flow data."""
        df = ak.stock_market_fund_flow()
        return df

    def _fetch_fund_flow_sector(self):
        """Fetch sector fund flow ranking data."""
        df = ak.stock_sector_fund_flow_rank(indicator="今日")
        return df

    def _fetch_margin_sh(self):
        """Fetch Shanghai margin trading data (recent 30 days)."""
        from datetime import timedelta
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        df = ak.stock_margin_sse(start_date=start_date, end_date=end_date)
        return df

    def _fetch_margin_sz(self):
        """Fetch Shenzhen margin trading data (latest date)."""
        date_str = datetime.now().strftime("%Y%m%d")
        df = ak.stock_margin_szse(date=date_str)
        return df

    def _fetch_lhb(self):
        """Fetch Dragon-Tiger list detail data (recent 5 trading days)."""
        from datetime import timedelta
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
        df = ak.stock_lhb_detail_em(start_date=start_date, end_date=end_date)
        return df
