"""HK stock data processor: search index, top-N, index summary, hot stocks."""

from .base_processor import BaseProcessor


class HKProcessor(BaseProcessor):
    """Process HK stock parquet into MCP-ready JSON.

    Reads: hk_spot, hk_index, hk_hot_rank, hk_ggt_components, hk_main_board
    Writes: hk_stock/search_index, hk_stock/top/, hk_stock/index_summary,
            hk_stock/hot_stocks
    """

    def process(self) -> dict:
        summary = {"processor": "hk_stock", "files_written": 0, "errors": []}

        df_spot = self.read_parquet("hk_stock", "hk_spot")
        df_index = self.read_parquet("hk_stock", "hk_index")
        df_hot = self.read_parquet("hk_stock", "hk_hot_rank")

        # ── search_index.json ───────────────────────────────────────────
        try:
            records = self._build_search_index(df_spot)
            if records:
                self.write_json(records, "hk_stock/search_index.json")
                summary["search_index_count"] = len(records)
        except Exception as e:
            summary["errors"].append(f"search_index: {e}")
            self.logger.error(f"HK search_index failed: {e}")

        # ── top/ per-code (top 200 by 成交额) ──────────────────────────
        try:
            top_records = self._build_top_records(df_spot)
            if top_records:
                n = self.write_per_code_json(
                    top_records, "hk_stock", "code", top_n=200,
                    sort_field="amount", sort_ascending=False,
                )
                summary["top_files"] = n
        except Exception as e:
            summary["errors"].append(f"top: {e}")
            self.logger.error(f"HK top failed: {e}")

        # ── index_summary.json ──────────────────────────────────────────
        try:
            self._write_index_summary(df_index)
        except Exception as e:
            summary["errors"].append(f"index_summary: {e}")
            self.logger.error(f"HK index_summary failed: {e}")

        # ── hot_stocks.json ─────────────────────────────────────────────
        try:
            self._write_hot_stocks(df_hot)
        except Exception as e:
            summary["errors"].append(f"hot_stocks: {e}")
            self.logger.error(f"HK hot_stocks failed: {e}")

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
                "amplitude": self.safe_float(row.get("振幅")),
                "turnover": self.safe_float(row.get("换手率")),
                "high": self.safe_float(row.get("最高")),
                "low": self.safe_float(row.get("最低")),
                "open": self.safe_float(row.get("今开")),
                "prev_close": self.safe_float(row.get("昨收")),
            })
        return records

    def _write_index_summary(self, df_index):
        if df_index is None or df_index.empty:
            return
        recs = []
        for _, row in df_index.iterrows():
            recs.append({
                "code": str(row.get("代码", "")),
                "name": str(row.get("名称", "")),
                "value": self.safe_float(row.get("最新价")),
                "change_pct": self.safe_float(row.get("涨跌幅")),
                "change_amt": self.safe_float(row.get("涨跌额")),
                "prev_close": self.safe_float(row.get("昨收")),
                "open": self.safe_float(row.get("今开")),
            })
        self.write_json(recs, "hk_stock/index_summary.json")

    def _write_hot_stocks(self, df_hot):
        if df_hot is None or df_hot.empty:
            return
        recs = []
        for _, row in df_hot.iterrows():
            code = str(row.get("代码", "")).strip()
            if not code:
                continue
            recs.append({
                "code": code,
                "name": str(row.get("名称", "")),
                "price": self.safe_float(row.get("最新价")),
                "change_pct": self.safe_float(row.get("涨跌幅")),
                "rank": self.safe_float(row.get("排名")),
            })
        recs.sort(key=lambda r: r.get("rank") or 9999)
        self.write_json(recs, "hk_stock/hot_stocks.json")
