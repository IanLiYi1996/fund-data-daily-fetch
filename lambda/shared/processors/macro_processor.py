"""Macro data processor: CN + US macro latest values."""

from .base_processor import BaseProcessor


class MacroProcessor(BaseProcessor):
    """Process macro parquet into MCP-ready JSON.

    Reads: macro_lpr, macro_cpi, macro_ppi (CN)
           us_macro_* (16 US indicators from us_stock category)
    Writes: macro/cn_latest.json, macro/us_latest.json
    """

    # US macro dataset names (stored under us_stock category)
    US_MACRO_DATASETS = [
        ("us_macro_nonfarm_payroll", "nonfarm_payroll"),
        ("us_macro_unemployment", "unemployment"),
        ("us_macro_jobless_claims", "jobless_claims"),
        ("us_macro_cpi", "cpi"),
        ("us_macro_core_pce", "core_pce"),
        ("us_macro_ppi", "ppi"),
        ("us_macro_ism_pmi", "ism_pmi"),
        ("us_macro_industrial_production", "industrial_production"),
        ("us_macro_gdp", "gdp"),
        ("us_macro_consumer_confidence", "consumer_confidence"),
        ("us_macro_michigan_sentiment", "michigan_sentiment"),
        ("us_macro_housing_starts", "housing_starts"),
        ("us_macro_building_permits", "building_permits"),
        ("us_macro_trade_balance", "trade_balance"),
        ("us_macro_retail_sales", "retail_sales"),
        ("us_macro_durable_goods", "durable_goods"),
    ]

    def process(self) -> dict:
        summary = {"processor": "macro", "files_written": 0, "errors": []}

        # ── CN macro ────────────────────────────────────────────────────
        try:
            cn_data = self._build_cn_latest()
            if cn_data:
                self.write_json(cn_data, "macro/cn_latest.json")
        except Exception as e:
            summary["errors"].append(f"cn_latest: {e}")
            self.logger.error(f"CN macro failed: {e}")

        # ── US macro ────────────────────────────────────────────────────
        try:
            us_data = self._build_us_latest()
            if us_data:
                self.write_json(us_data, "macro/us_latest.json")
        except Exception as e:
            summary["errors"].append(f"us_latest: {e}")
            self.logger.error(f"US macro failed: {e}")

        summary["files_written"] = self._write_count
        return summary

    def _build_cn_latest(self) -> dict:
        """Build CN macro latest values from LPR, CPI, PPI."""
        result = {}

        # LPR
        df_lpr = self.read_parquet("macro", "macro_lpr")
        if df_lpr is not None and not df_lpr.empty:
            latest = df_lpr.iloc[-1]
            result["lpr_1y"] = self.safe_float(latest.get("LPR1Y", latest.get("LPR1年")))
            result["lpr_5y"] = self.safe_float(latest.get("LPR5Y", latest.get("LPR5年")))
            result["lpr_date"] = str(latest.get("TRADE_DATE", latest.get("日期", "")))

        # CPI
        df_cpi = self.read_parquet("macro", "macro_cpi")
        if df_cpi is not None and not df_cpi.empty:
            latest = df_cpi.iloc[-1]
            result["cpi_yoy"] = self.safe_float(latest.get("当月同比"))
            result["cpi_mom"] = self.safe_float(latest.get("当月环比"))
            result["cpi_date"] = str(latest.get("日期", ""))

        # PPI
        df_ppi = self.read_parquet("macro", "macro_ppi")
        if df_ppi is not None and not df_ppi.empty:
            latest = df_ppi.iloc[-1]
            result["ppi_yoy"] = self.safe_float(latest.get("当月同比"))
            result["ppi_mom"] = self.safe_float(latest.get("当月环比"))
            result["ppi_date"] = str(latest.get("日期", ""))

        return result

    def _build_us_latest(self) -> dict:
        """Build US macro latest values from 16 indicator datasets."""
        result = {}

        for dataset_name, key in self.US_MACRO_DATASETS:
            df = self.read_parquet("us_stock", dataset_name)
            if df is None or df.empty:
                continue
            try:
                latest = df.iloc[-1]
                entry = {
                    "value": self.safe_float(latest.get("今值")),
                    "forecast": self.safe_float(latest.get("预测值")),
                    "previous": self.safe_float(latest.get("前值")),
                    "date": str(latest.get("日期", "")),
                }
                result[key] = entry
            except Exception as e:
                self.logger.warning(f"US macro {key} extract failed: {e}")

        return result
