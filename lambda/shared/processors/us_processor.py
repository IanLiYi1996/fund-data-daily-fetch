"""US stock data processor: search index, top-N, famous stocks."""

from .base_processor import BaseProcessor


class USProcessor(BaseProcessor):
    """Process US stock parquet into MCP-ready JSON.

    Reads: us_stock_spot, us_famous_spot
    Writes: us_stock/search_index, us_stock/top/, us_stock/famous_stocks
    """

    def process(self) -> dict:
        summary = {"processor": "us_stock", "files_written": 0, "errors": []}

        df_spot = self.read_parquet("us_stock", "us_stock_spot")
        df_famous = self.read_parquet("us_stock", "us_famous_spot")

        # ── search_index.json ───────────────────────────────────────────
        try:
            records = self._build_search_index(df_spot)
            if records:
                self.write_json(records, "us_stock/search_index.json")
                summary["search_index_count"] = len(records)
        except Exception as e:
            summary["errors"].append(f"search_index: {e}")
            self.logger.error(f"US search_index failed: {e}")

        # ── top/ per-code (top 200 by market_cap) ──────────────────────
        try:
            top_records = self._build_top_records(df_spot)
            if top_records:
                n = self.write_per_code_json(
                    top_records, "us_stock", "code", top_n=200,
                    sort_field="market_cap", sort_ascending=False,
                )
                summary["top_files"] = n
        except Exception as e:
            summary["errors"].append(f"top: {e}")
            self.logger.error(f"US top failed: {e}")

        # ── famous_stocks.json ──────────────────────────────────────────
        try:
            self._write_famous_stocks(df_famous)
        except Exception as e:
            summary["errors"].append(f"famous_stocks: {e}")
            self.logger.error(f"US famous_stocks failed: {e}")

        summary["files_written"] = self._write_count
        return summary

    # ── Private builders ────────────────────────────────────────────────

    def _build_search_index(self, df_spot):
        if df_spot is None or df_spot.empty:
            return []
        records = []
        for _, row in df_spot.iterrows():
            code = str(row.get("代码", "")).strip()
            if not code:
                continue
            records.append({
                "code": code,
                "name": str(row.get("名称", "")),
                "price": self.safe_float(row.get("最新价")),
                "change_pct": self.safe_float(row.get("涨跌幅")),
                "volume": self.safe_float(row.get("成交量")),
                "amount": self.safe_float(row.get("成交额")),
                "market_cap": self.safe_float(row.get("总市值")),
            })
        return records

    def _build_top_records(self, df_spot):
        if df_spot is None or df_spot.empty:
            return []
        records = []
        for _, row in df_spot.iterrows():
            code = str(row.get("代码", "")).strip()
            if not code:
                continue
            records.append({
                "code": code,
                "name": str(row.get("名称", "")),
                "price": self.safe_float(row.get("最新价")),
                "change_pct": self.safe_float(row.get("涨跌幅")),
                "change_amt": self.safe_float(row.get("涨跌额")),
                "volume": self.safe_float(row.get("成交量")),
                "amount": self.safe_float(row.get("成交额")),
                "market_cap": self.safe_float(row.get("总市值")),
                "high": self.safe_float(row.get("最高")),
                "low": self.safe_float(row.get("最低")),
                "open": self.safe_float(row.get("今开")),
                "prev_close": self.safe_float(row.get("昨收")),
            })
        return records

    def _write_famous_stocks(self, df_famous):
        if df_famous is None or df_famous.empty:
            return
        grouped = {}
        for _, row in df_famous.iterrows():
            category = str(row.get("类别", "其他"))
            rec = {
                "code": str(row.get("代码", "")),
                "name": str(row.get("名称", "")),
                "price": self.safe_float(row.get("最新价")),
                "change_pct": self.safe_float(row.get("涨跌幅")),
                "volume": self.safe_float(row.get("成交量")),
                "market_cap": self.safe_float(row.get("总市值")),
            }
            grouped.setdefault(category, []).append(rec)

        # Sort each category by market cap desc
        for cat in grouped:
            grouped[cat].sort(key=lambda r: r.get("market_cap") or 0, reverse=True)

        self.write_json(grouped, "us_stock/famous_stocks.json")
