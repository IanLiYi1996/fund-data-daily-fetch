import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


# ~50 HK blue-chip tickers (HSI / HSCEI constituents)
_HK_BLUE_CHIPS = [
    "0700", "9988", "1810", "0005", "2318", "0941", "0388", "1299",
    "0001", "0016", "0027", "0003", "0011", "0006", "0002", "0883",
    "0066", "1038", "0012", "0017", "0267", "1109", "0823", "0175",
    "1997", "9618", "9999", "3690", "0241", "2020", "2269", "9961",
    "1211", "2382", "0669", "0288", "1928", "2628", "1398", "0939",
    "3988", "3968", "0386", "1088", "0857", "2899", "1113", "0101",
    "0688",
]

_HK_INDICES = {
    "^HSI":   {"代码": "HSI",   "名称": "恒生指数"},
    "^HSCE":  {"代码": "HSCE",  "名称": "国企指数"},
    "^HSCCI": {"代码": "HSCCI", "名称": "红筹指数"},
}


class HKStockFetcher(BaseFetcher):
    """Fetcher for Hong Kong stock market data via yfinance.

    Replaces blocked 东方财富 _em APIs with yfinance for HK blue-chip
    stocks and major indices.
    """

    DATA_CATALOG = {
        "hk_spot": {
            "name_cn": "港股实时行情",
            "description": "港股蓝筹股实时行情数据（yfinance源），包含价格、涨跌幅、成交量等",
            "source_api": "yfinance",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额"],
        },
        "hk_index": {
            "name_cn": "港股指数行情",
            "description": "香港主要股票指数实时行情（恒生指数、国企指数、红筹指数）",
            "source_api": "yfinance",
            "update_frequency": "realtime",
            "key_fields": ["代码", "名称", "最新价", "涨跌额", "涨跌幅", "昨收", "今开"],
        },
    }

    @classmethod
    def get_data_catalog(cls) -> dict:
        return {
            "category": "hk_stock",
            "category_cn": "港股市场数据",
            "description": "港股蓝筹行情及主要指数数据（yfinance源）",
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
            # These are unavailable from yfinance; set None for processor compat
            "振幅": None,
            "换手率": None,
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

    def _fetch_hk_spot(self):
        """Fetch HK blue-chip spot data via yfinance."""
        yf_symbols = [f"{code}.HK" for code in _HK_BLUE_CHIPS]
        code_map = {f"{code}.HK": code.zfill(5) for code in _HK_BLUE_CHIPS}
        return self._fetch_yf_batch(yf_symbols, code_map=code_map)

    def _fetch_hk_index(self):
        """Fetch HK index data via yfinance."""
        import yfinance as yf

        rows = []
        for yf_sym, meta in _HK_INDICES.items():
            try:
                t = yf.Ticker(yf_sym)
                info = t.info
                price = info.get("regularMarketPrice")
                prev = info.get("previousClose") or info.get("regularMarketPreviousClose")
                change_pct = info.get("regularMarketChangePercent")
                if change_pct is None and price and prev and prev != 0:
                    change_pct = round((price - prev) / prev * 100, 2)
                rows.append({
                    "代码": meta["代码"],
                    "名称": meta["名称"],
                    "最新价": price,
                    "涨跌幅": change_pct,
                    "涨跌额": round(price - prev, 2) if price and prev else None,
                    "昨收": prev,
                    "今开": info.get("open") or info.get("regularMarketOpen"),
                })
            except Exception as e:
                self.logger.warning(f"Failed to fetch HK index {yf_sym}: {e}")
        return pd.DataFrame(rows) if rows else pd.DataFrame()
