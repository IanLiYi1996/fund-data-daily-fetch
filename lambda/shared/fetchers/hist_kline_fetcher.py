"""Historical K-line fetcher for A-shares, HK stocks, and US stocks.

Uses yfinance batch download for all three markets.
- A-shares: CSI 300 + CSI 500 (~800 stocks) via akshare csindex
- HK: csindex HK indices + Wikipedia HSI (~300 stocks)
- US: S&P 500 (~503 stocks) via Wikipedia
"""

import pandas as pd
from .base_fetcher import BaseFetcher, FetchResult, FetchSummary


_INTERVALS = [
    ("daily", "1d"),
    ("weekly", "1wk"),
    ("monthly", "1mo"),
]

_CHUNK_SIZE = 100

# Column mapping from yfinance English names to Chinese output schema
_COL_MAP = {
    "Open": "开盘",
    "Close": "收盘",
    "High": "最高",
    "Low": "最低",
    "Volume": "成交量",
}


class HistKlineFetcher(BaseFetcher):
    """Fetcher for 1-year historical K-line data (daily/weekly/monthly).

    Markets: A-shares (CSI 300 + CSI 500 ~800), HK (港股通+HSI ~300), US (S&P 500 ~503).
    Source: yfinance batch download.
    Output: per-stock parquet files across 3 markets x 3 frequencies.
    """

    DATA_CATALOG = {
        "a_share_daily": {
            "name_cn": "A股日K线",
            "description": "沪深300+中证500成分股近1年日K线数据",
            "source_api": "yfinance",
            "update_frequency": "daily",
            "key_fields": ["代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"],
        },
        "a_share_weekly": {
            "name_cn": "A股周K线",
            "description": "沪深300+中证500成分股近1年周K线数据",
            "source_api": "yfinance",
            "update_frequency": "weekly",
            "key_fields": ["代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"],
        },
        "a_share_monthly": {
            "name_cn": "A股月K线",
            "description": "沪深300+中证500成分股近1年月K线数据",
            "source_api": "yfinance",
            "update_frequency": "monthly",
            "key_fields": ["代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"],
        },
        "hk_daily": {
            "name_cn": "港股日K线",
            "description": "港股通+恒生指数成分股近1年日K线数据(~300只)",
            "source_api": "yfinance",
            "update_frequency": "daily",
            "key_fields": ["代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"],
        },
        "hk_weekly": {
            "name_cn": "港股周K线",
            "description": "港股通+恒生指数成分股近1年周K线数据(~300只)",
            "source_api": "yfinance",
            "update_frequency": "weekly",
            "key_fields": ["代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"],
        },
        "hk_monthly": {
            "name_cn": "港股月K线",
            "description": "港股通+恒生指数成分股近1年月K线数据(~300只)",
            "source_api": "yfinance",
            "update_frequency": "monthly",
            "key_fields": ["代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"],
        },
        "us_daily": {
            "name_cn": "美股日K线",
            "description": "S&P 500成分股近1年日K线数据(~503只)",
            "source_api": "yfinance",
            "update_frequency": "daily",
            "key_fields": ["代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"],
        },
        "us_weekly": {
            "name_cn": "美股周K线",
            "description": "S&P 500成分股近1年周K线数据(~503只)",
            "source_api": "yfinance",
            "update_frequency": "weekly",
            "key_fields": ["代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"],
        },
        "us_monthly": {
            "name_cn": "美股月K线",
            "description": "S&P 500成分股近1年月K线数据(~503只)",
            "source_api": "yfinance",
            "update_frequency": "monthly",
            "key_fields": ["代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅"],
        },
    }

    @classmethod
    def get_data_catalog(cls) -> dict:
        return {
            "category": "hist_kline",
            "category_cn": "历史K线数据",
            "description": "A股/港股/美股近1年历史K线（日K/周K/月K），yfinance源",
            "data_sources": cls.DATA_CATALOG,
            "total_sources": len(cls.DATA_CATALOG),
        }

    @property
    def category(self) -> str:
        return "hist_kline"

    # ------------------------------------------------------------------
    # Ticker lists
    # ------------------------------------------------------------------

    def _get_a_share_tickers(self) -> list[str]:
        """Get CSI 300 + CSI 500 constituents and convert to yfinance format.

        Uses akshare's index_stock_cons_csindex which queries the CSIndex
        official site (NOT 东方财富), so it works on AWS.
        """
        import akshare as ak

        codes = set()
        for index_code in ("000300", "000905"):
            try:
                df = ak.index_stock_cons_csindex(symbol=index_code)
                # Prefer '成分券代码' (constituent code), not '指数代码' (index code)
                code_col = None
                for col in df.columns:
                    if col == "成分券代码":
                        code_col = col
                        break
                if code_col is None:
                    for col in df.columns:
                        if "成分" in col and "代码" in col:
                            code_col = col
                            break
                if code_col is None:
                    code_col = df.columns[0]
                    self.logger.warning(f"Could not find constituent code column, using {code_col}")
                codes.update(df[code_col].astype(str).str.zfill(6).tolist())
            except Exception as e:
                self.logger.warning(f"Failed to fetch CSI {index_code} constituents: {e}")

        self.logger.info(f"Got {len(codes)} unique A-share tickers from CSI 300 + CSI 500")

        # Convert to yfinance format: 6xxxxx.SS (Shanghai) or others .SZ (Shenzhen)
        yf_tickers = []
        for code in sorted(codes):
            if code.startswith("6"):
                yf_tickers.append(f"{code}.SS")
            else:
                # 000xxx, 001xxx, 002xxx, 003xxx, 300xxx, 301xxx → Shenzhen
                yf_tickers.append(f"{code}.SZ")
        return yf_tickers

    def _get_hk_tickers(self) -> list[str]:
        """Get HK tickers from multiple csindex HK indices + Wikipedia HSI.

        Combines csindex港股通/恒生 indices with Wikipedia HSI constituents
        for broad coverage (~300 unique HK stocks).
        """
        import akshare as ak
        import io
        import requests

        hk_codes: set[str] = set()

        # Source 1: csindex HK indices (港股通 + 恒生 series)
        csindex_hk_indices = [
            ("930914", "港股通50"),
            ("930931", "港股通"),
            ("931643", "港股通综合"),
            ("H11146", "恒生港股通"),
            ("H30374", "恒生港股通高股息"),
            ("931351", "沪港深500"),
            ("H30533", "恒生综合"),
        ]
        for idx_code, idx_name in csindex_hk_indices:
            try:
                df = ak.index_stock_cons_csindex(symbol=idx_code)
                col = "成分券代码" if "成分券代码" in df.columns else df.columns[4]
                # Only keep .HK tickers (some indices mix A-shares)
                hk_only = df[df[col].astype(str).str.endswith(".HK")][col].unique()
                codes = {c.replace(".HK", "").zfill(4) for c in hk_only}
                self.logger.info(f"csindex {idx_name}({idx_code}): {len(codes)} HK tickers")
                hk_codes.update(codes)
            except Exception as e:
                self.logger.warning(f"csindex {idx_name}({idx_code}) failed: {e}")

        # Source 2: Wikipedia HSI constituents (~85 stocks)
        try:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; FundDataBot/1.0)"}
            resp = requests.get(
                "https://en.wikipedia.org/wiki/Hang_Seng_Index",
                headers=headers, timeout=15,
            )
            import pandas as _pd
            tables = _pd.read_html(io.StringIO(resp.text))
            for t in tables:
                if "Ticker" in t.columns and len(t) >= 50:
                    wiki_codes = t["Ticker"].str.extract(r"(\d+)")[0].dropna()
                    codes = {c.zfill(4) for c in wiki_codes}
                    self.logger.info(f"Wikipedia HSI: {len(codes)} tickers")
                    hk_codes.update(codes)
                    break
        except Exception as e:
            self.logger.warning(f"Wikipedia HSI fetch failed: {e}")

        self.logger.info(f"Total unique HK tickers: {len(hk_codes)}")
        return sorted(f"{code}.HK" for code in hk_codes)

    def _get_us_tickers(self) -> list[str]:
        """Get S&P 500 constituents from Wikipedia (~503 stocks).

        Falls back to the hardcoded 46-stock list if Wikipedia fetch fails.
        """
        import io
        import requests

        try:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; FundDataBot/1.0)"}
            resp = requests.get(
                "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                headers=headers, timeout=15,
            )
            import pandas as _pd
            tables = _pd.read_html(io.StringIO(resp.text))
            sp500 = tables[0]
            # Wikipedia uses BRK.B format; yfinance expects BRK-B
            tickers = sorted(
                sp500["Symbol"].str.replace(".", "-", regex=False).tolist()
            )
            self.logger.info(f"S&P 500 from Wikipedia: {len(tickers)} tickers")
            return tickers
        except Exception as e:
            self.logger.warning(f"S&P 500 Wikipedia fetch failed: {e}, using fallback list")
            # Fallback to the hardcoded list from us_stock_fetcher
            from .us_stock_fetcher import _ALL_US_TICKERS
            return list(_ALL_US_TICKERS)

    # ------------------------------------------------------------------
    # Core download logic
    # ------------------------------------------------------------------

    def _fetch_market_kline(
        self, tickers: list[str], interval: str, market: str
    ) -> pd.DataFrame:
        """Batch-download historical K-line data via yf.download().

        Downloads in chunks of _CHUNK_SIZE to avoid timeouts, then
        flattens the MultiIndex DataFrame into the standard output schema.
        """
        import yfinance as yf

        all_frames = []

        for i in range(0, len(tickers), _CHUNK_SIZE):
            chunk = tickers[i : i + _CHUNK_SIZE]
            self.logger.info(
                f"[{market}] Downloading chunk {i // _CHUNK_SIZE + 1} "
                f"({len(chunk)} tickers, interval={interval})"
            )
            try:
                df = yf.download(
                    tickers=chunk,
                    period="1y",
                    interval=interval,
                    group_by="ticker",
                    auto_adjust=True,
                    threads=True,
                )
                if df is None or df.empty:
                    self.logger.warning(f"[{market}] Chunk {i // _CHUNK_SIZE + 1} returned empty")
                    continue

                # yf.download with multiple tickers returns MultiIndex columns:
                # level 0 = ticker, level 1 = OHLCV field
                # With a single ticker it returns flat columns.
                if len(chunk) == 1:
                    ticker = chunk[0]
                    flat = df.copy()
                    flat["_ticker"] = ticker
                    all_frames.append(flat)
                elif isinstance(df.columns, pd.MultiIndex):
                    for ticker in chunk:
                        if ticker in df.columns.get_level_values(0):
                            sub = df[ticker].copy()
                            sub["_ticker"] = ticker
                            all_frames.append(sub)
                else:
                    # Fallback: single ticker returned flat columns
                    flat = df.copy()
                    flat["_ticker"] = chunk[0] if len(chunk) == 1 else "UNKNOWN"
                    all_frames.append(flat)

            except Exception as e:
                self.logger.warning(
                    f"[{market}] Chunk {i // _CHUNK_SIZE + 1} failed: {e}"
                )

        if not all_frames:
            return pd.DataFrame()

        combined = pd.concat(all_frames, ignore_index=False)
        return self._normalize_kline(combined, market)

    def _normalize_kline(self, df: pd.DataFrame, market: str) -> pd.DataFrame:
        """Normalize raw yfinance output to the standard Chinese column schema."""
        # Reset index to get Date as a column
        df = df.reset_index()

        # Identify the date column (yfinance uses 'Date' or 'Datetime')
        date_col = None
        for col in ("Date", "Datetime", "date"):
            if col in df.columns:
                date_col = col
                break
        if date_col is None:
            # Fallback: first datetime-like column
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    date_col = col
                    break

        # Build output DataFrame
        out = pd.DataFrame()
        out["代码"] = df["_ticker"].apply(lambda t: self._strip_suffix(t, market))
        if date_col:
            out["日期"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
        else:
            out["日期"] = None

        for en, cn in _COL_MAP.items():
            out[cn] = df[en] if en in df.columns else None

        # 成交额 = Close * Volume (approximate, yfinance doesn't provide turnover)
        if "Close" in df.columns and "Volume" in df.columns:
            out["成交额"] = (df["Close"] * df["Volume"]).round(2)
        else:
            out["成交额"] = None

        # 涨跌幅 = pct change based on Close
        if "Close" in df.columns:
            out["涨跌幅"] = df.groupby("_ticker")["Close"].pct_change().mul(100).round(2)
        else:
            out["涨跌幅"] = None

        # Drop rows where Close is NaN (non-trading days / missing data)
        out = out.dropna(subset=["收盘"])
        out = out.reset_index(drop=True)
        return out

    @staticmethod
    def _strip_suffix(ticker: str, market: str) -> str:
        """Remove yfinance suffix to get the raw stock code."""
        if market == "a_share":
            return ticker.replace(".SS", "").replace(".SZ", "")
        elif market == "hk":
            return ticker.replace(".HK", "")
        else:
            return ticker

    # ------------------------------------------------------------------
    # fetch_all
    # ------------------------------------------------------------------

    def fetch_all(self) -> FetchSummary:
        """Fetch all markets & frequencies, split into per-stock FetchResults.

        Each result is named ``{market}/{frequency}/{ticker_code}`` so the
        handler produces one parquet file per stock, e.g.
        ``hist_kline/2026-02-28/a_share/daily/600519.parquet``.
        """
        results: list = []

        # Resolve ticker lists
        a_share_tickers = self._get_a_share_tickers()
        hk_tickers = self._get_hk_tickers()
        us_tickers = self._get_us_tickers()

        markets = [
            ("a_share", a_share_tickers),
            ("hk", hk_tickers),
            ("us", us_tickers),
        ]

        for market, tickers in markets:
            if not tickers:
                self.logger.warning(f"No tickers for market {market}, skipping")
                continue
            for freq_name, yf_interval in _INTERVALS:
                try:
                    self.logger.info(f"Fetching {market}/{freq_name}...")
                    combined = self._fetch_market_kline(tickers, yf_interval, market)
                    if combined is None or combined.empty:
                        self.logger.warning(f"{market}/{freq_name}: No data returned")
                        continue

                    # Split combined DataFrame by stock code
                    for code, group_df in combined.groupby("代码", sort=True):
                        stock_df = group_df.reset_index(drop=True)
                        data_name = f"{market}/{freq_name}/{code}"
                        results.append(
                            FetchResult(
                                name=data_name,
                                data=stock_df,
                                success=True,
                            )
                        )

                    self.logger.info(
                        f"{market}/{freq_name}: Split into "
                        f"{combined['代码'].nunique()} stock files, "
                        f"{len(combined)} total rows"
                    )
                except Exception as e:
                    self.logger.error(f"{market}/{freq_name}: Failed - {e}")
                    results.append(
                        FetchResult(
                            name=f"{market}/{freq_name}",
                            success=False,
                            error=str(e),
                        )
                    )

        return FetchSummary(category=self.category, results=results)
