"""Cross-market daily snapshot processor."""

from .base_processor import BaseProcessor


class MarketOverviewProcessor(BaseProcessor):
    """Generate market_overview.json combining CN/HK/US + macro highlights.

    Reads from raw parquet rather than other processors' output to avoid
    ordering dependencies.
    """

    def process(self) -> dict:
        summary = {"processor": "market_overview", "files_written": 0, "errors": []}

        overview = {}

        # ── A-share market ──────────────────────────────────────────────
        try:
            overview["a_share"] = self._a_share_snapshot()
        except Exception as e:
            summary["errors"].append(f"a_share: {e}")
            self.logger.error(f"Market overview A-share failed: {e}")

        # ── HK market ──────────────────────────────────────────────────
        try:
            overview["hk_stock"] = self._hk_snapshot()
        except Exception as e:
            summary["errors"].append(f"hk_stock: {e}")
            self.logger.error(f"Market overview HK failed: {e}")

        # ── US market ──────────────────────────────────────────────────
        try:
            overview["us_stock"] = self._us_snapshot()
        except Exception as e:
            summary["errors"].append(f"us_stock: {e}")
            self.logger.error(f"Market overview US failed: {e}")

        # ── CN indices ──────────────────────────────────────────────────
        try:
            overview["cn_index"] = self._cn_index_snapshot()
        except Exception as e:
            summary["errors"].append(f"cn_index: {e}")
            self.logger.error(f"Market overview CN index failed: {e}")

        # ── Macro highlights ────────────────────────────────────────────
        try:
            overview["macro"] = self._macro_highlights()
        except Exception as e:
            summary["errors"].append(f"macro: {e}")
            self.logger.error(f"Market overview macro failed: {e}")

        self.write_json(overview, "market_overview.json")
        summary["files_written"] = self._write_count
        return summary

    # ── Snapshots ───────────────────────────────────────────────────────

    def _a_share_snapshot(self) -> dict:
        df = self.read_parquet("a_share", "a_share_spot")
        if df is None or df.empty:
            return {"status": "no_data"}
        total = len(df)
        up = len(df[df["涨跌幅"] > 0]) if "涨跌幅" in df.columns else 0
        down = len(df[df["涨跌幅"] < 0]) if "涨跌幅" in df.columns else 0
        flat = total - up - down
        avg_change = self.safe_float(df["涨跌幅"].mean()) if "涨跌幅" in df.columns else None
        total_volume = self.safe_float(df["成交额"].sum()) if "成交额" in df.columns else None

        top5 = []
        if "涨跌幅" in df.columns:
            top_df = df.nlargest(5, "涨跌幅")
            for _, row in top_df.iterrows():
                top5.append({
                    "code": str(row.get("代码", "")),
                    "name": str(row.get("名称", "")),
                    "change_pct": self.safe_float(row.get("涨跌幅")),
                })

        return {
            "total_stocks": total,
            "up": up,
            "down": down,
            "flat": flat,
            "avg_change_pct": avg_change,
            "total_amount": total_volume,
            "top_gainers": top5,
        }

    def _hk_snapshot(self) -> dict:
        # Use HK index for overview
        df_idx = self.read_parquet("hk_stock", "hk_index")
        if df_idx is None or df_idx.empty:
            return {"status": "no_data"}

        indices = []
        for _, row in df_idx.iterrows():
            indices.append({
                "name": str(row.get("名称", "")),
                "value": self.safe_float(row.get("最新价")),
                "change_pct": self.safe_float(row.get("涨跌幅")),
            })
        return {"indices": indices}

    def _us_snapshot(self) -> dict:
        df = self.read_parquet("us_stock", "us_famous_spot")
        if df is None or df.empty:
            return {"status": "no_data"}

        # Group by category and show top 3 each
        result = {}
        if "类别" in df.columns:
            for cat, grp in df.groupby("类别"):
                top3 = []
                sorted_grp = grp.sort_values("总市值", ascending=False) if "总市值" in grp.columns else grp
                for _, row in sorted_grp.head(3).iterrows():
                    top3.append({
                        "code": str(row.get("代码", "")),
                        "name": str(row.get("名称", "")),
                        "price": self.safe_float(row.get("最新价")),
                        "change_pct": self.safe_float(row.get("涨跌幅")),
                    })
                result[str(cat)] = top3
        return result

    def _cn_index_snapshot(self) -> dict:
        # Try stock_index_sh and stock_index_sz
        indices = []
        for name in ("stock_index_sh", "stock_index_sz"):
            df = self.read_parquet("stock", name)
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    indices.append({
                        "code": str(row.get("代码", row.get("序号", ""))),
                        "name": str(row.get("名称", "")),
                        "value": self.safe_float(row.get("最新价")),
                        "change_pct": self.safe_float(row.get("涨跌幅")),
                    })
        return {"indices": indices} if indices else {"status": "no_data"}

    def _macro_highlights(self) -> dict:
        highlights = {}

        # LPR
        df = self.read_parquet("macro", "macro_lpr")
        if df is not None and not df.empty:
            latest = df.iloc[-1]
            highlights["lpr_1y"] = self.safe_float(latest.get("LPR1Y", latest.get("LPR1年")))
            highlights["lpr_5y"] = self.safe_float(latest.get("LPR5Y", latest.get("LPR5年")))

        # CPI
        df = self.read_parquet("macro", "macro_cpi")
        if df is not None and not df.empty:
            latest = df.iloc[-1]
            highlights["cpi_yoy"] = self.safe_float(latest.get("当月同比"))

        return highlights
