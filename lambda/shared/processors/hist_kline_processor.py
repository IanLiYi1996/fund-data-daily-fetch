"""Processor: enrich per-stock daily K-line parquet with technical indicators."""

import io
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from shared.indicators import IndicatorConfig, TechnicalIndicators
from .base_processor import BaseProcessor

# Chinese → English column mapping for indicator calculations
_COL_MAP = {
    "收盘": "close",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
}

# Markets and their S3 sub-prefixes under hist_kline/{date}/
_MARKETS = ["a_share", "hk", "us"]

_MAX_WORKERS = 20


class HistKlineProcessor(BaseProcessor):
    """Read daily K-line parquets, calculate indicators, write enriched parquets.

    Input:  hist_kline/{date}/{market}/daily/{code}.parquet  (243 rows × 9 cols)
    Output: hist_kline_indicators/{date}/{market}/{code}.parquet (243 rows × 31 cols)
    """

    def __init__(self, s3_client, bucket: str, date_str: str) -> None:
        super().__init__(s3_client, bucket, date_str)
        self.config = IndicatorConfig()
        self.ti = TechnicalIndicators()

    # ── Public entry point ───────────────────────────────────────────────

    def process(self) -> dict:
        summary = {
            "processor": "hist_kline_indicators",
            "files_written": 0,
            "errors": [],
            "skipped": 0,
        }

        for market in _MARKETS:
            try:
                stats = self._process_market(market)
                summary["files_written"] += stats["written"]
                summary["skipped"] += stats["skipped"]
                summary["errors"].extend(stats["errors"])
            except Exception as e:
                msg = f"{market}: {e}"
                summary["errors"].append(msg)
                self.logger.error(f"Market {msg}", exc_info=True)

        self.logger.info(
            f"HistKline done: {summary['files_written']} written, "
            f"{summary['skipped']} skipped, {len(summary['errors'])} errors"
        )
        return summary

    # ── Per-market parallel orchestration ────────────────────────────────

    def _process_market(self, market: str) -> dict:
        stats = {"written": 0, "skipped": 0, "errors": []}

        # List all daily parquet files for this market
        prefix = f"hist_kline/{self.date_str}/{market}/daily/"
        stock_keys = self._list_parquet_keys(prefix)

        if not stock_keys:
            self.logger.info(f"No daily kline files for {market}")
            return stats

        self.logger.info(f"Processing {len(stock_keys)} stocks for {market}")

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {
                pool.submit(self._process_single_stock, market, key): key
                for key in stock_keys
            }
            for future in as_completed(futures):
                key = futures[future]
                try:
                    result = future.result()
                    if result == "written":
                        stats["written"] += 1
                    elif result == "skipped":
                        stats["skipped"] += 1
                except Exception as e:
                    code = key.rsplit("/", 1)[-1].replace(".parquet", "")
                    msg = f"{market}/{code}: {e}"
                    stats["errors"].append(msg)
                    self.logger.error(msg)

        return stats

    # ── Single stock: read → compute → write ────────────────────────────

    def _process_single_stock(self, market: str, s3_key: str) -> str:
        """Returns 'written' or 'skipped'."""
        code = s3_key.rsplit("/", 1)[-1].replace(".parquet", "")

        # Read parquet via BaseProcessor helper
        # s3_key = hist_kline/{date}/{market}/daily/{code}.parquet
        # read_parquet expects (category, name) → category/date/name.parquet
        df = self.read_parquet("hist_kline", f"{market}/daily/{code}")
        if df is None or len(df) < self.config.min_data_points:
            return "skipped"

        # Map Chinese columns to English for calculations
        col_en = {}
        for cn, en in _COL_MAP.items():
            if cn in df.columns:
                col_en[en] = df[cn]
        if not all(k in col_en for k in ("close", "high", "low", "volume")):
            self.logger.warning(f"Missing required columns for {market}/{code}")
            return "skipped"

        close = col_en["close"]
        high = col_en["high"]
        low = col_en["low"]
        volume = col_en["volume"]

        # ── Calculate all indicators ─────────────────────────────────
        # SMA
        for p in self.config.sma_periods:
            df[f"MA{p}"] = self.ti.sma(close, p)

        # EMA
        for p in self.config.ema_periods:
            df[f"EMA{p}"] = self.ti.ema(close, p)

        # RSI
        df["RSI_14"] = self.ti.rsi(close, self.config.rsi_period)

        # MACD
        macd = self.ti.macd(close, self.config.macd_fast, self.config.macd_slow, self.config.macd_signal)
        df["MACD"] = macd["macd_line"]
        df["MACD_Signal"] = macd["signal_line"]
        df["MACD_Hist"] = macd["histogram"]

        # Bollinger Bands
        bb = self.ti.bollinger_bands(close, self.config.bb_period, self.config.bb_std_dev, self.config.bb_ma_type)
        df["BOLL_Upper"] = bb["upper"]
        df["BOLL_Mid"] = bb["middle"]
        df["BOLL_Lower"] = bb["lower"]

        # KDJ
        kdj = self.ti.kdj(high, low, close, self.config.kdj_k_period, self.config.kdj_d_period)
        df["KDJ_K"] = kdj["k"]
        df["KDJ_D"] = kdj["d"]
        df["KDJ_J"] = kdj["j"]

        # ATR / ADX
        df["ATR_14"] = self.ti.atr(high, low, close, self.config.atr_period)
        df["ADX_14"] = self.ti.adx(high, low, close, self.config.adx_period)

        # OBV
        df["OBV"] = self.ti.obv(close, volume)

        # Volume MAs
        for p in self.config.volume_ma_periods:
            df[f"VOL_MA{p}"] = self.ti.sma(volume, p)

        # ── Write enriched parquet to S3 ─────────────────────────────
        output_key = f"hist_kline_indicators/{self.date_str}/{market}/{code}.parquet"
        self._write_parquet(df, output_key)
        return "written"

    # ── S3 helpers ───────────────────────────────────────────────────────

    def _list_parquet_keys(self, prefix: str) -> list[str]:
        """List all .parquet keys under an S3 prefix."""
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])
        return keys

    def _write_parquet(self, df: pd.DataFrame, key: str) -> None:
        """Write a DataFrame as parquet to S3."""
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
