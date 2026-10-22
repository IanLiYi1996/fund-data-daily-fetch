import math
import re

import akshare as ak
import pandas as pd
import requests

from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


def _normalize_sina_spot(df: pd.DataFrame) -> pd.DataFrame:
    """Shape Sina's ETF/LOF spot list like the eastmoney one it replaces.

    Sina prefixes codes with the exchange ("sz159998", "sh510300"); everywhere
    else in the lake fund_code is the bare 6-digit code, and fund_processor
    joins top_etf.json on it. The Chinese headers Sina uses for price, change,
    volume and turnover already match the eastmoney ones consumers key on, so
    only the code needs touching.
    """
    if df is None or df.empty or "代码" not in df.columns:
        return df
    out = df.copy()
    out["代码"] = out["代码"].astype(str).str.replace(
        r"^(sh|sz|bj)", "", regex=True,
    )
    return out


# REIT quotes come from Sina hq, not ak.reits_realtime_em. That calls
# 95.push2.eastmoney.com, the push2 quote API that already killed fund_etf /
# fund_lof: it failed 11 days since July (all 3 retries RemoteDisconnected on
# 2026-10-21), and on 10-22 every push2 host (95./82./push2/push2delay)
# answered empty or 502 to most probes. It also hard-codes pz=100 while the
# market lists 103 REITs, so successful runs silently dropped 3 a day.
_SINA_HQ_URL = "https://hq.sinajs.cn/list="
_SINA_HQ_BATCH = 200
_SINA_HQ_LINE = re.compile(r'var hq_str_(sh|sz)(\d{6})="([^"]*)"')


def _sina_symbol(code: str):
    """Exchange-prefixed Sina symbol for a listed REIT code."""
    if code.startswith("5"):
        return f"sh{code}"
    if code.startswith("1"):
        return f"sz{code}"
    return None


def _reit_codes(fund_name_df: pd.DataFrame) -> list:
    """Exchange-listed public REITs (excludes QDII-REITs, which are OTC funds)."""
    mask = fund_name_df["基金类型"].astype(str) == "Reits"
    return fund_name_df.loc[mask, "基金代码"].astype(str).tolist()


def _get_sina_hq(symbols: list) -> str:
    r = requests.get(
        _SINA_HQ_URL + ",".join(symbols),
        headers={"Referer": "https://finance.sina.com.cn",
                 "User-Agent": "Mozilla/5.0"},
        timeout=15,
    )
    r.raise_for_status()
    r.encoding = "gbk"
    return r.text


def _num(v: str) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return math.nan


def _parse_sina_hq(text: str) -> pd.DataFrame:
    """Sina hq lines -> the eastmoney reits_realtime_em column shape.

    Sina fields: 0 name, 1 open, 2 prev close, 3 price, 4 high, 5 low,
    8 volume (shares), 9 turnover (yuan), 30 date. 成交量 is converted to 手
    to match eastmoney's f5 and the existing series. A price of 0 means no
    trade yet / suspended; it becomes NaN as eastmoney's "-" did.
    """
    rows = []
    for _, code, body in _SINA_HQ_LINE.findall(text):
        f = body.split(",")
        if len(f) < 31:
            continue
        price, prev = _num(f[3]), _num(f[2])
        if not price:
            price = math.nan
        traded = not math.isnan(price) and bool(prev)
        rows.append({
            "代码": code,
            "名称": f[0],
            "最新价": price,
            "涨跌额": round(price - prev, 3) if traded else math.nan,
            "涨跌幅": round((price / prev - 1) * 100, 2) if traded else math.nan,
            "成交量": _num(f[8]) / 100,
            "成交额": _num(f[9]),
            "开盘价": _num(f[1]) or math.nan,
            "最高价": _num(f[4]) or math.nan,
            "最低价": _num(f[5]) or math.nan,
            "昨收": prev,
            "数据日期": f[30],
        })
    return pd.DataFrame(rows)


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
            "source_api": "fund_etf_category_sina(ETF基金)",
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
            "source_api": "fund_etf_category_sina(LOF基金)",
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
            "source_api": "fund_name_em(Reits) + Sina hq.sinajs.cn",
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

    @property
    def TABLE_METHODS(self):
        return {
            "fund_performance": self._fetch_fund_performance,
            "fund_etf": self._fetch_fund_etf,
            "fund_name": self._fetch_fund_name,
            "fund_manager": self._fetch_fund_manager,
            "fund_daily": self._fetch_fund_daily,
            "fund_money_daily": self._fetch_fund_money_daily,
            "fund_financial_daily": self._fetch_fund_financial_daily,
            "fund_etf_daily": self._fetch_fund_etf_daily,
            "fund_lof": self._fetch_fund_lof,
            "fund_value_estimation": self._fetch_fund_value_estimation,
            "fund_purchase": self._fetch_fund_purchase,
            "fund_exchange_rank": self._fetch_fund_exchange_rank,
            "fund_money_rank": self._fetch_fund_money_rank,
            "fund_hk_rank": self._fetch_fund_hk_rank,
            "fund_rating": self._fetch_fund_rating,
            "fund_dividend_rank": self._fetch_fund_dividend_rank,
            "fund_dividend": self._fetch_fund_dividend,
            "fund_split": self._fetch_fund_split,
            "fund_index_info": self._fetch_fund_index_info,
            "fund_graded_daily": self._fetch_fund_graded_daily,
            "fund_reits_daily": self._fetch_fund_reits_daily,
        }

    @classmethod
    def list_tables(cls) -> list[str]:
        # Class-level access without instantiation; mirrors TABLE_METHODS keys.
        return [
            "fund_performance", "fund_etf", "fund_name", "fund_manager",
            "fund_daily", "fund_money_daily", "fund_financial_daily",
            "fund_etf_daily", "fund_lof", "fund_value_estimation", "fund_purchase",
            "fund_exchange_rank", "fund_money_rank", "fund_hk_rank",
            "fund_rating", "fund_dividend_rank", "fund_dividend", "fund_split",
            "fund_index_info", "fund_graded_daily", "fund_reits_daily",
        ]

    def fetch_one(self, table: str) -> FetchResult:
        method = self.TABLE_METHODS.get(table)
        if method is None:
            return FetchResult(name=table, success=False, error=f"unknown table: {table}")
        return self._safe_fetch(table, method)

    def fetch_all(self) -> FetchSummary:
        results = [self.fetch_one(name) for name in self.list_tables()]
        return FetchSummary(category=self.category, results=results)

    def _fetch_fund_performance(self):
        df = ak.fund_open_fund_rank_em(symbol="全部")
        return df

    # ETF and LOF spot quotes come from Sina, not eastmoney's push2 quote API.
    # push2delay.eastmoney.com answered 502 (ETF) or dropped the connection
    # (LOF, 88.push2) on every daily run from 2026-09-18 / 2026-08-26 through
    # 2026-10-08, from Lambda and from an EC2 host alike, on akshare 1.18.60
    # and 1.19.1 — the endpoint rejects this traffic, the client is not at
    # fault. The Map state swallowed the per-table failure, so the two tables
    # went stale for weeks while the workflow reported SUCCEEDED. Sina's list
    # (fund_etf_category_sina) returned 1,694 ETFs / 360 LOFs in ~3s.
    def _fetch_fund_etf(self):
        df = ak.fund_etf_category_sina(symbol="ETF基金")
        return _normalize_sina_spot(df)

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
        df = ak.fund_etf_category_sina(symbol="LOF基金")
        return _normalize_sina_spot(df)

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
        fn = ak.fund_name_em()
        codes = _reit_codes(fn)
        symbols = [s for s in map(_sina_symbol, codes) if s]
        text = "".join(
            _get_sina_hq(symbols[i:i + _SINA_HQ_BATCH])
            for i in range(0, len(symbols), _SINA_HQ_BATCH)
        )
        df = _parse_sina_hq(text)
        if symbols and df.empty:
            raise ValueError(
                f"Sina hq returned 0 of {len(symbols)} REIT quotes"
            )
        # Registered-but-not-yet-trading REITs (14 on 2026-10-22) get no
        # Sina quote at all. eastmoney listed them with a null price, so keep
        # an unpriced row to hold the per-day REIT universe steady.
        missing = [c for c in codes if c not in set(df["代码"])]
        if missing:
            names = dict(zip(fn["基金代码"].astype(str), fn["基金简称"]))
            quote_date = df["数据日期"].mode().iloc[0]
            df = pd.concat([df, pd.DataFrame({
                "代码": missing,
                "名称": [names.get(c) for c in missing],
                "数据日期": quote_date,
            })], ignore_index=True)
        return df

    def _fetch_fund_portfolio_hold(self):
        import pandas as pd
        return pd.DataFrame()
