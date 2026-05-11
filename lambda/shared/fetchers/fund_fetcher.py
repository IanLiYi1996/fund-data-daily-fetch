import akshare as ak
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


class FundFetcher(BaseFetcher):
    """Fetcher for fund-related data from akshare."""

    # Data catalog with descriptions for each data source
    DATA_CATALOG = {
        # Existing interfaces
        "fund_performance": {
            "name_cn": "开放式基金排行",
            "description": "开放式基金业绩排名数据，包含净值、各时段收益率",
            "source_api": "fund_open_fund_rank_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "单位净值", "累计净值", "日增长率", "近1周", "近1月", "近1年", "手续费"],
            "has_fee_data": True,
        },
        "fund_etf": {
            "name_cn": "ETF实时行情",
            "description": "场内ETF基金实时交易数据，包含价格、成交量、涨跌幅",
            "source_api": "fund_etf_spot_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌额", "涨跌幅", "成交量", "成交额"],
            "has_fee_data": False,
        },
        "fund_name": {
            "name_cn": "基金基本信息列表",
            "description": "全量公募基金基本信息，包含基金代码、名称、类型",
            "source_api": "fund_name_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "基金类型"],
            "has_fee_data": False,
        },
        "fund_manager": {
            "name_cn": "基金经理排行",
            "description": "基金经理信息及业绩排行",
            "source_api": "fund_manager_em",
            "update_frequency": "daily",
            "key_fields": ["基金经理", "所属公司", "管理规模", "任职回报"],
            "has_fee_data": False,
        },
        # Priority 1: Daily real-time data
        "fund_daily": {
            "name_cn": "开放式基金实时净值",
            "description": "全量开放式基金当日净值数据，包含单位净值、累计净值、日增长率",
            "source_api": "fund_open_fund_daily_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "单位净值", "累计净值", "日增长率", "申购状态", "赎回状态", "手续费"],
            "has_fee_data": True,
        },
        "fund_money_daily": {
            "name_cn": "货币基金实时收益",
            "description": "货币基金每日万份收益和7日年化收益率",
            "source_api": "fund_money_fund_daily_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "万份收益", "7日年化"],
            "has_fee_data": False,
        },
        "fund_financial_daily": {
            "name_cn": "理财基金实时数据",
            "description": "理财型基金每日收益数据",
            "source_api": "fund_financial_fund_daily_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "万份收益", "7日年化"],
            "has_fee_data": False,
        },
        "fund_etf_daily": {
            "name_cn": "场内ETF实时数据",
            "description": "场内交易ETF基金净值和折溢价数据",
            "source_api": "fund_etf_fund_daily_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "单位净值", "累计净值", "市价", "折溢价率"],
            "has_fee_data": False,
        },
        "fund_lof": {
            "name_cn": "LOF实时行情",
            "description": "LOF基金实时交易价格和折溢价数据",
            "source_api": "fund_lof_spot_em",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅", "成交量", "净值", "折溢价率"],
            "has_fee_data": False,
        },
        "fund_value_estimation": {
            "name_cn": "净值估算数据",
            "description": "基金盘中净值实时估算数据，供投资参考",
            "source_api": "fund_value_estimation_em",
            "update_frequency": "realtime",
            "key_fields": ["基金代码", "基金名称", "估算净值", "估算涨跌幅", "估算时间"],
            "has_fee_data": False,
        },
        "fund_purchase": {
            "name_cn": "基金申购状态",
            "description": "基金申购/赎回状态、费率和限额信息（销售渠道费用数据）",
            "source_api": "fund_purchase_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "申购状态", "赎回状态", "购买起点", "日累计限定金额", "手续费"],
            "has_fee_data": True,
            "fee_description": "包含申购手续费率、购买起点金额、日限额",
        },
        # Priority 2: Ranking and rating data
        "fund_exchange_rank": {
            "name_cn": "场内基金排行",
            "description": "场内交易基金业绩排行数据",
            "source_api": "fund_exchange_rank_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "单位净值", "近1周", "近1月", "近1年"],
            "has_fee_data": False,
        },
        "fund_money_rank": {
            "name_cn": "货币基金排行",
            "description": "货币基金收益排行数据",
            "source_api": "fund_money_rank_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "万份收益", "7日年化", "近1月", "近1年", "手续费"],
            "has_fee_data": True,
        },
        "fund_hk_rank": {
            "name_cn": "香港基金排行",
            "description": "港股基金业绩排行数据",
            "source_api": "fund_hk_rank_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "币种", "单位净值", "近1月", "近1年"],
            "has_fee_data": False,
        },
        "fund_rating": {
            "name_cn": "基金评级总汇",
            "description": "多家评级机构的基金评级汇总数据",
            "source_api": "fund_rating_all",
            "update_frequency": "weekly",
            "key_fields": ["基金代码", "基金简称", "上海证券评级", "招商证券评级", "济安金信评级"],
            "has_fee_data": False,
        },
        "fund_dividend_rank": {
            "name_cn": "基金分红排行",
            "description": "基金累计分红排行数据",
            "source_api": "fund_fh_rank_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "累计分红", "分红次数"],
            "has_fee_data": False,
        },
        # Priority 3: Dividend and split data
        "fund_dividend": {
            "name_cn": "基金分红数据",
            "description": "基金分红记录详情，包含分红日期和金额",
            "source_api": "fund_fh_em",
            "update_frequency": "event-driven",
            "key_fields": ["基金代码", "基金简称", "分红金额", "除息日", "发放日"],
            "has_fee_data": False,
        },
        "fund_split": {
            "name_cn": "基金拆分数据",
            "description": "基金拆分记录详情，包含拆分比例和日期",
            "source_api": "fund_cf_em",
            "update_frequency": "event-driven",
            "key_fields": ["基金代码", "基金简称", "拆分比例", "拆分日期"],
            "has_fee_data": False,
        },
        # Priority 4: Index fund specific
        "fund_index_info": {
            "name_cn": "指数型基金信息",
            "description": "指数基金详细数据，包含跟踪标的和跟踪误差",
            "source_api": "fund_info_index_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金名称", "单位净值", "手续费", "起购金额", "跟踪标的", "跟踪方式"],
            "has_fee_data": True,
            "fee_description": "包含申购手续费和起购金额",
        },
        "fund_graded_daily": {
            "name_cn": "分级基金实时数据",
            "description": "分级基金净值和折溢价数据",
            "source_api": "fund_graded_fund_daily_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "单位净值", "折溢价率"],
            "has_fee_data": False,
        },
        # Priority 5: Closed-end / REITs / FOF / portfolio
        "fund_close_daily": {
            "name_cn": "场内封闭式基金",
            "description": "场内封闭式基金净值与折溢价",
            "source_api": "fund_close_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "最新价", "单位净值", "折溢价率"],
            "has_fee_data": False,
        },
        "fund_fof_daily": {
            "name_cn": "FOF 基金每日",
            "description": "FOF 基金每日净值与收益",
            "source_api": "fund_fof_em",
            "update_frequency": "daily",
            "key_fields": ["基金代码", "基金简称", "单位净值", "累计净值"],
            "has_fee_data": False,
        },
        "fund_reits_daily": {
            "name_cn": "公募 REITs",
            "description": "公募 REITs 实时行情",
            "source_api": "public_fund_REITs",
            "update_frequency": "daily",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅"],
            "has_fee_data": False,
        },
        "fund_portfolio_hold": {
            "name_cn": "基金持仓明细",
            "description": "基金季度报告持仓明细（低频）",
            "source_api": "fund_portfolio_hold_em",
            "update_frequency": "quarterly",
            "key_fields": ["基金代码", "报告期", "持仓代码", "持仓名称", "占净值比例"],
            "has_fee_data": False,
        },
    }

    @classmethod
    def get_data_catalog(cls) -> dict:
        """Get the data catalog with descriptions for all data sources."""
        return {
            "category": "fund",
            "category_cn": "公募基金数据",
            "description": "公募基金相关数据，包括净值、排行、费率、分红等",
            "data_sources": cls.DATA_CATALOG,
            "fee_data_sources": [
                name for name, info in cls.DATA_CATALOG.items() if info.get("has_fee_data")
            ],
            "total_sources": len(cls.DATA_CATALOG),
        }

    @property
    def category(self) -> str:
        return "fund"

    def fetch_all(self) -> FetchSummary:
        """Fetch all fund data."""
        results = []

        # ===== Existing interfaces =====
        results.append(self._safe_fetch("fund_performance", self._fetch_fund_performance))
        results.append(self._safe_fetch("fund_etf", self._fetch_fund_etf))
        results.append(self._safe_fetch("fund_name", self._fetch_fund_name))
        results.append(self._safe_fetch("fund_manager", self._fetch_fund_manager))

        # ===== Priority 1: Daily real-time data =====
        results.append(self._safe_fetch("fund_daily", self._fetch_fund_daily))
        results.append(self._safe_fetch("fund_money_daily", self._fetch_fund_money_daily))
        results.append(self._safe_fetch("fund_financial_daily", self._fetch_fund_financial_daily))
        results.append(self._safe_fetch("fund_etf_daily", self._fetch_fund_etf_daily))
        results.append(self._safe_fetch("fund_lof", self._fetch_fund_lof))
        results.append(self._safe_fetch("fund_value_estimation", self._fetch_fund_value_estimation))
        results.append(self._safe_fetch("fund_purchase", self._fetch_fund_purchase))

        # ===== Priority 2: Ranking and rating data =====
        results.append(self._safe_fetch("fund_exchange_rank", self._fetch_fund_exchange_rank))
        results.append(self._safe_fetch("fund_money_rank", self._fetch_fund_money_rank))
        results.append(self._safe_fetch("fund_hk_rank", self._fetch_fund_hk_rank))
        results.append(self._safe_fetch("fund_rating", self._fetch_fund_rating))
        results.append(self._safe_fetch("fund_dividend_rank", self._fetch_fund_dividend_rank))

        # ===== Priority 3: Dividend and split data =====
        results.append(self._safe_fetch("fund_dividend", self._fetch_fund_dividend))
        results.append(self._safe_fetch("fund_split", self._fetch_fund_split))

        # ===== Priority 4: Index fund specific =====
        results.append(self._safe_fetch("fund_index_info", self._fetch_fund_index_info))
        results.append(self._safe_fetch("fund_graded_daily", self._fetch_fund_graded_daily))

        # ===== Priority 5: REITs (closed/FOF deferred — akshare has no direct API) =====
        # fund_close_daily and fund_fof_daily need per-code akshare APIs; skip until a
        # fan-out design lands. fund_portfolio_hold is similarly quarterly per-fund.
        results.append(self._safe_fetch("fund_reits_daily", self._fetch_fund_reits_daily))

        return FetchSummary(category=self.category, results=results)

    def _fetch_fund_performance(self):
        df = ak.fund_open_fund_rank_em(symbol="全部")
        return df

    def _fetch_fund_etf(self):
        df = ak.fund_etf_spot_em()
        return df

    def _fetch_fund_name(self):
        df = ak.fund_name_em()
        return df

    def _fetch_fund_manager(self):
        import hashlib
        df = ak.fund_manager_em()
        # akshare returns no stable manager id. Synthesize one as a 16-char
        # hash of (name + company) so PK (manager_id, snapshot_date) is stable
        # across days for the same manager.
        if "姓名" in df.columns and "所属公司" in df.columns:
            df["manager_id"] = (
                df["姓名"].astype(str) + "|" + df["所属公司"].astype(str)
            ).apply(lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()[:16])
        return df

    def _fetch_fund_daily(self):
        df = ak.fund_open_fund_daily_em()
        return df

    def _fetch_fund_money_daily(self):
        df = ak.fund_money_fund_daily_em()
        return df

    def _fetch_fund_financial_daily(self):
        df = ak.fund_financial_fund_daily_em()
        return df

    def _fetch_fund_etf_daily(self):
        df = ak.fund_etf_fund_daily_em()
        return df

    def _fetch_fund_lof(self):
        df = ak.fund_lof_spot_em()
        return df

    def _fetch_fund_value_estimation(self):
        df = ak.fund_value_estimation_em()
        return df

    def _fetch_fund_purchase(self):
        df = ak.fund_purchase_em()
        return df

    def _fetch_fund_exchange_rank(self):
        df = ak.fund_exchange_rank_em()
        return df

    def _fetch_fund_money_rank(self):
        df = ak.fund_money_rank_em()
        return df

    def _fetch_fund_hk_rank(self):
        df = ak.fund_hk_rank_em()
        return df

    def _fetch_fund_rating(self):
        import pandas as pd
        wide = ak.fund_rating_all()
        # akshare returns a wide frame (one row per fund with 4 rating-agency
        # columns). The Iceberg schema is long (fund_code, snapshot_date,
        # rating_agency) → rating. Melt here.
        rating_cols = [c for c in ("上海证券", "招商证券", "济安金信", "晨星评级")
                       if c in wide.columns]
        if not rating_cols:
            return wide
        # akshare fund_rating_all returns 代码 + 简称 (not 基金代码/基金简称)
        id_cols = [c for c in ("代码", "简称", "基金代码", "基金简称")
                   if c in wide.columns]
        long_df = wide.melt(
            id_vars=id_cols,
            value_vars=rating_cols,
            var_name="rating_agency",
            value_name="rating",
        )
        # Drop rows with missing rating and coerce to string (schema is String)
        long_df = long_df.dropna(subset=["rating"])
        long_df["rating"] = long_df["rating"].astype(str)
        return long_df

    def _fetch_fund_dividend_rank(self):
        df = ak.fund_fh_rank_em()
        return df

    def _fetch_fund_dividend(self):
        df = ak.fund_fh_em()
        return df

    def _fetch_fund_split(self):
        df = ak.fund_cf_em()
        return df

    def _fetch_fund_index_info(self):
        import pandas as pd

        dfs = []
        combinations = [
            ("沪深指数", "被动指数型"),
            ("沪深指数", "增强指数型"),
            ("行业主题", "被动指数型"),
            ("行业主题", "增强指数型"),
        ]

        for symbol, indicator in combinations:
            try:
                df = ak.fund_info_index_em(symbol=symbol, indicator=indicator)
                df["指数类型"] = symbol
                df["基金类型"] = indicator
                dfs.append(df)
            except Exception:
                pass

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def _fetch_fund_graded_daily(self):
        df = ak.fund_graded_fund_daily_em()
        return df

    # === New: REITs ===
    # fund_close_em / fund_fof_em / public_fund_REITs do not exist in current
    # akshare (1.18.x). Keeping _fetch_fund_close_daily / _fetch_fund_fof_daily
    # / _fetch_fund_portfolio_hold as placeholders (returning empty) so the
    # DATA_CATALOG catalog entries don't dangle if anyone re-enables them.
    def _fetch_fund_close_daily(self):
        import pandas as pd
        return pd.DataFrame()

    def _fetch_fund_fof_daily(self):
        import pandas as pd
        return pd.DataFrame()

    def _fetch_fund_reits_daily(self):
        # akshare 1.18: reits_realtime_em covers public REITs intraday prices.
        return ak.reits_realtime_em()

    def _fetch_fund_portfolio_hold(self):
        import pandas as pd
        return pd.DataFrame()
