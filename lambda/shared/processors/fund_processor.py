"""Fund data processor: search index, top-N per-code, rankings, fees."""

from .base_processor import BaseProcessor


class FundProcessor(BaseProcessor):
    """Process fund parquet into MCP-ready JSON.

    Reads: fund_daily, fund_performance, fund_name, fund_etf,
           fund_money_rank, fund_purchase
    Writes: fund/search_index, fund/top/, fund/rankings/, fund/fee_summary
    """

    def process(self) -> dict:
        summary = {"processor": "fund", "files_written": 0, "errors": []}

        # ── Read raw data ───────────────────────────────────────────────
        df_daily = self.read_parquet("fund", "fund_daily")
        df_name = self.read_parquet("fund", "fund_name")
        df_etf = self.read_parquet("fund", "fund_etf")
        df_money_rank = self.read_parquet("fund", "fund_money_rank")
        df_purchase = self.read_parquet("fund", "fund_purchase")
        df_performance = self.read_parquet("fund", "fund_performance")

        # ── search_index.json ───────────────────────────────────────────
        try:
            index_records = self._build_search_index(df_daily, df_name)
            if index_records:
                self.write_json(index_records, "fund/search_index.json")
                summary["search_index_count"] = len(index_records)
        except Exception as e:
            summary["errors"].append(f"search_index: {e}")
            self.logger.error(f"Fund search_index failed: {e}")

        # ── top/ per-code JSON (top 500 by |daily_return|) ──────────────
        try:
            top_records = self._build_top_records(df_daily)
            if top_records:
                n = self.write_per_code_json(
                    top_records, "fund", "code", top_n=500,
                    sort_field="abs_change", sort_ascending=False,
                )
                summary["top_files"] = n
        except Exception as e:
            summary["errors"].append(f"top: {e}")
            self.logger.error(f"Fund top failed: {e}")

        # ── rankings ────────────────────────────────────────────────────
        try:
            self._write_rankings(df_daily, df_etf, df_money_rank)
        except Exception as e:
            summary["errors"].append(f"rankings: {e}")
            self.logger.error(f"Fund rankings failed: {e}")

        # ── fee_summary.json ────────────────────────────────────────────
        try:
            self._write_fee_summary(df_purchase)
        except Exception as e:
            summary["errors"].append(f"fee_summary: {e}")
            self.logger.error(f"Fund fee_summary failed: {e}")

        summary["files_written"] = self._write_count
        return summary

    # ── Private builders ────────────────────────────────────────────────

    def _build_search_index(self, df_daily, df_name):
        """Build [{code, name, type, nav, change_pct}] from fund_daily + fund_name."""
        if df_daily is None or df_daily.empty:
            return []

        records = []
        # Build name/type lookup from fund_name
        type_map = {}
        if df_name is not None and not df_name.empty:
            for _, row in df_name.iterrows():
                code = str(row.get("基金代码", "")).strip()
                if code:
                    type_map[code] = str(row.get("基金类型", ""))

        for _, row in df_daily.iterrows():
            code = str(row.get("基金代码", "")).strip()
            if not code:
                continue
            records.append({
                "code": code,
                "name": str(row.get("基金简称", "")),
                "type": type_map.get(code, ""),
                "nav": self.safe_float(row.get("单位净值")),
                "acc_nav": self.safe_float(row.get("累计净值")),
                "change_pct": self.safe_float(row.get("日增长率")),
            })
        return records

    def _build_top_records(self, df_daily):
        """Build per-code detail records sorted by abs(daily return)."""
        if df_daily is None or df_daily.empty:
            return []

        records = []
        for _, row in df_daily.iterrows():
            code = str(row.get("基金代码", "")).strip()
            if not code:
                continue
            change = self.safe_float(row.get("日增长率"))
            rec = {
                "code": code,
                "name": str(row.get("基金简称", "")),
                "nav": self.safe_float(row.get("单位净值")),
                "acc_nav": self.safe_float(row.get("累计净值")),
                "change_pct": change,
                "abs_change": abs(change) if change is not None else 0,
                "buy_status": str(row.get("申购状态", "")),
                "sell_status": str(row.get("赎回状态", "")),
                "fee": str(row.get("手续费", "")),
            }
            records.append(rec)
        return records

    def _write_rankings(self, df_daily, df_etf, df_money_rank):
        """Write ranking JSON files."""
        # by_daily_return.json: top 100 gainers + top 100 losers
        if df_daily is not None and not df_daily.empty:
            recs = self._build_top_records(df_daily)
            valid = [r for r in recs if r.get("change_pct") is not None]
            sorted_by_change = sorted(valid, key=lambda r: r["change_pct"], reverse=True)
            gainers = sorted_by_change[:100]
            losers = sorted_by_change[-100:][::-1]  # worst 100, reversed
            self.write_json(
                {"gainers": gainers, "losers": losers},
                "fund/rankings/by_daily_return.json",
            )

        # top_etf.json
        if df_etf is not None and not df_etf.empty:
            etf_recs = []
            for _, row in df_etf.iterrows():
                etf_recs.append({
                    "code": str(row.get("代码", "")),
                    "name": str(row.get("名称", "")),
                    "price": self.safe_float(row.get("最新价")),
                    "change_pct": self.safe_float(row.get("涨跌幅")),
                    "volume": self.safe_float(row.get("成交量")),
                    "amount": self.safe_float(row.get("成交额")),
                })
            etf_recs.sort(key=lambda r: r.get("change_pct") or 0, reverse=True)
            self.write_json(etf_recs[:200], "fund/rankings/top_etf.json")

        # money_fund.json sorted by 7日年化
        if df_money_rank is not None and not df_money_rank.empty:
            money_recs = []
            for _, row in df_money_rank.iterrows():
                money_recs.append({
                    "code": str(row.get("基金代码", "")),
                    "name": str(row.get("基金简称", "")),
                    "yield_7d": self.safe_float(row.get("7日年化")),
                    "yield_per_10k": self.safe_float(row.get("万份收益")),
                    "fee": str(row.get("手续费", "")),
                })
            money_recs.sort(
                key=lambda r: r.get("yield_7d") or 0, reverse=True
            )
            self.write_json(money_recs[:200], "fund/rankings/money_fund.json")

    def _write_fee_summary(self, df_purchase):
        """Write fee_summary.json from fund_purchase data."""
        if df_purchase is None or df_purchase.empty:
            return
        recs = []
        for _, row in df_purchase.iterrows():
            code = str(row.get("基金代码", "")).strip()
            if not code:
                continue
            recs.append({
                "code": code,
                "name": str(row.get("基金简称", "")),
                "buy_status": str(row.get("申购状态", "")),
                "sell_status": str(row.get("赎回状态", "")),
                "min_purchase": str(row.get("购买起点", "")),
                "daily_limit": str(row.get("日累计限定金额", "")),
                "fee": str(row.get("手续费", "")),
            })
        self.write_json(recs, "fund/fee_summary.json")
