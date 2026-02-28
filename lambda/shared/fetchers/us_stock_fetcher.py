import pandas as pd
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


# US famous stock lists by category (matching original 类别 values)
_US_FAMOUS_STOCKS = {
    "科技类": [
        "AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "INTC",
        "AMD", "CRM", "ORCL", "ADBE", "CSCO", "AVGO", "QCOM", "NFLX",
        "UBER", "ABNB",
    ],
    "金融类": [
        "JPM", "BAC", "GS", "MS", "WFC", "C", "BLK", "AXP", "V", "MA",
    ],
    "医药食品类": [
        "JNJ", "PFE", "UNH", "ABBV", "MRK", "LLY", "TMO", "ABT",
        "KO", "PEP", "MCD",
    ],
    "媒体类": [
        "DIS", "CMCSA", "NWSA", "RBLX", "EA", "TTWO", "SPOT",
    ],
}

# Flat list of all famous tickers (used for us_stock_spot)
_ALL_US_TICKERS = sorted(
    {sym for syms in _US_FAMOUS_STOCKS.values() for sym in syms}
)


class USStockFetcher(BaseFetcher):
    """Fetcher for US stock and US macroeconomic data.

    US stock spot data via yfinance (replaces blocked _em APIs).
    US macro 16 indicators via akshare (still working fine).
    """

    US_FAMOUS_CATEGORIES = list(_US_FAMOUS_STOCKS.keys())

    DATA_CATALOG = {
        # US Stock Market (yfinance source)
        "us_stock_spot": {
            "name_cn": "美股实时行情",
            "description": "美股大盘股实时行情数据（yfinance源），包含价格、涨跌幅、市值等",
            "source_api": "yfinance",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "总市值"],
        },
        "us_famous_spot": {
            "name_cn": "知名美股行情",
            "description": "知名美股分类行情（科技、金融、医药、媒体），yfinance源",
            "source_api": "yfinance",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅", "成交量", "总市值", "类别"],
        },
        # US Macroeconomic Data (16 indicators — akshare, still works)
        "us_macro_nonfarm_payroll": {
            "name_cn": "美国非农就业",
            "description": "美国非农就业人数变化，月度劳动力市场核心指标",
            "source_api": "macro_usa_non_farm",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_unemployment": {
            "name_cn": "美国失业率",
            "description": "美国失业率数据",
            "source_api": "macro_usa_unemployment_rate",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_jobless_claims": {
            "name_cn": "美国初请失业金",
            "description": "美国每周初请失业金人数",
            "source_api": "macro_usa_initial_jobless",
            "update_frequency": "weekly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_cpi": {
            "name_cn": "美国CPI",
            "description": "美国消费者价格指数(CPI)月率",
            "source_api": "macro_usa_cpi_monthly",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_core_pce": {
            "name_cn": "美国核心PCE",
            "description": "美国核心个人消费支出价格指数(PCE)，美联储首选通胀指标",
            "source_api": "macro_usa_core_pce_price",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_ppi": {
            "name_cn": "美国PPI",
            "description": "美国生产者价格指数(PPI)月率",
            "source_api": "macro_usa_ppi",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_ism_pmi": {
            "name_cn": "美国ISM制造业PMI",
            "description": "美国供应管理协会(ISM)制造业采购经理人指数",
            "source_api": "macro_usa_ism_pmi",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_industrial_production": {
            "name_cn": "美国工业产出",
            "description": "美国工业生产指数月率",
            "source_api": "macro_usa_industrial_production",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_gdp": {
            "name_cn": "美国GDP",
            "description": "美国国内生产总值(GDP)月度数据",
            "source_api": "macro_usa_gdp_monthly",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_consumer_confidence": {
            "name_cn": "美国消费者信心",
            "description": "美国咨商会消费者信心指数",
            "source_api": "macro_usa_cb_consumer_confidence",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_michigan_sentiment": {
            "name_cn": "美国密歇根消费者信心",
            "description": "密歇根大学消费者信心指数",
            "source_api": "macro_usa_michigan_consumer_sentiment",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_housing_starts": {
            "name_cn": "美国新屋开工",
            "description": "美国新屋开工数据",
            "source_api": "macro_usa_house_starts",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_building_permits": {
            "name_cn": "美国建筑许可",
            "description": "美国建筑许可数据",
            "source_api": "macro_usa_building_permits",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_trade_balance": {
            "name_cn": "美国贸易差额",
            "description": "美国贸易帐数据",
            "source_api": "macro_usa_trade_balance",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_retail_sales": {
            "name_cn": "美国零售销售",
            "description": "美国零售销售月率",
            "source_api": "macro_usa_retail_sales",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
        "us_macro_durable_goods": {
            "name_cn": "美国耐用品订单",
            "description": "美国耐用品订单月率",
            "source_api": "macro_usa_durable_goods_orders",
            "update_frequency": "monthly",
            "key_fields": ["日期", "今值", "预测值", "前值"],
        },
    }

    @classmethod
    def get_data_catalog(cls) -> dict:
        return {
            "category": "us_stock",
            "category_cn": "美股市场与美国宏观数据",
            "description": "美股实时行情（yfinance源）及美国16项核心宏观经济指标（非农、CPI、GDP等）",
            "data_sources": cls.DATA_CATALOG,
            "total_sources": len(cls.DATA_CATALOG),
        }

    @property
    def category(self) -> str:
        return "us_stock"

    def fetch_all(self) -> FetchSummary:
        results = []

        # US Stock spot (yfinance)
        results.append(self._safe_fetch("us_stock_spot", self._fetch_us_spot))
        results.append(self._safe_fetch("us_famous_spot", self._fetch_us_famous_spot))

        # US Macro (16 indicators — akshare, unchanged)
        macro_apis = [
            ("us_macro_nonfarm_payroll", ak.macro_usa_non_farm),
            ("us_macro_unemployment", ak.macro_usa_unemployment_rate),
            ("us_macro_jobless_claims", ak.macro_usa_initial_jobless),
            ("us_macro_cpi", ak.macro_usa_cpi_monthly),
            ("us_macro_core_pce", ak.macro_usa_core_pce_price),
            ("us_macro_ppi", ak.macro_usa_ppi),
            ("us_macro_ism_pmi", ak.macro_usa_ism_pmi),
            ("us_macro_industrial_production", ak.macro_usa_industrial_production),
            ("us_macro_gdp", ak.macro_usa_gdp_monthly),
            ("us_macro_consumer_confidence", ak.macro_usa_cb_consumer_confidence),
            ("us_macro_michigan_sentiment", ak.macro_usa_michigan_consumer_sentiment),
            ("us_macro_housing_starts", ak.macro_usa_house_starts),
            ("us_macro_building_permits", ak.macro_usa_building_permits),
            ("us_macro_trade_balance", ak.macro_usa_trade_balance),
            ("us_macro_retail_sales", ak.macro_usa_retail_sales),
            ("us_macro_durable_goods", ak.macro_usa_durable_goods_orders),
        ]
        for name, api_func in macro_apis:
            results.append(self._safe_fetch(name, api_func))

        return FetchSummary(category=self.category, results=results)

    # ------------------------------------------------------------------
    # yfinance helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _yf_to_row(sym, info, code=None):
        """Convert a yfinance info dict into a row with Chinese column names."""
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        prev = info.get("previousClose") or info.get("regularMarketPreviousClose")
        change_pct = info.get("regularMarketChangePercent")
        if change_pct is None and price and prev and prev != 0:
            change_pct = round((price - prev) / prev * 100, 2)
        return {
            "代码": code or sym,
            "名称": info.get("shortName", ""),
            "最新价": price,
            "涨跌幅": change_pct,
            "涨跌额": round(price - prev, 2) if price and prev else None,
            "成交量": info.get("volume"),
            "成交额": (info.get("volume") or 0) * (price or 0),
            "总市值": info.get("marketCap"),
            "最高": info.get("dayHigh"),
            "最低": info.get("dayLow"),
            "今开": info.get("open") or info.get("regularMarketOpen"),
            "昨收": prev,
        }

    def _fetch_yf_batch(self, symbols, code_map=None):
        """Fetch a batch of tickers via yfinance in parallel threads."""
        import yfinance as yf

        rows = []

        def _fetch_one(sym):
            try:
                t = yf.Ticker(sym)
                info = t.info
                if not info or not info.get("regularMarketPrice"):
                    return None
                code = code_map.get(sym, sym) if code_map else sym
                return self._yf_to_row(sym, info, code=code)
            except Exception as e:
                self.logger.warning(f"yfinance fetch failed for {sym}: {e}")
                return None

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_fetch_one, sym): sym for sym in symbols}
            for future in as_completed(futures):
                row = future.result()
                if row:
                    rows.append(row)

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # ------------------------------------------------------------------
    # Fetch methods
    # ------------------------------------------------------------------

    def _fetch_us_spot(self):
        """Fetch US large-cap spot data via yfinance."""
        return self._fetch_yf_batch(_ALL_US_TICKERS)

    def _fetch_us_famous_spot(self):
        """Fetch famous US stocks across categories via yfinance.

        Merges 科技类/金融类/医药食品类/媒体类 into one DataFrame with 类别 column.
        """
        all_rows = []
        for category, symbols in _US_FAMOUS_STOCKS.items():
            df = self._fetch_yf_batch(symbols)
            if not df.empty:
                df["类别"] = category
                all_rows.append(df)
        if all_rows:
            return pd.concat(all_rows, ignore_index=True)
        return pd.DataFrame()
