"""A-share data processor: search index, top-N, rankings, boards, fund flow."""

from .base_processor import BaseProcessor


class AShareProcessor(BaseProcessor):
    """Process A-share parquet into MCP-ready JSON.

    Reads: a_share_spot, board_industry, board_concept, zt_pool,
           hot_rank, lhb_detail, fund_flow_market, fund_flow_sector
    Writes: a_share/search_index, a_share/top/, a_share/rankings/,
            a_share/boards/, a_share/fund_flow/
    """

    def process(self) -> dict:
        summary = {"processor": "a_share", "files_written": 0, "errors": []}

        # ── Read raw data ───────────────────────────────────────────────
        df_spot = self.read_parquet("a_share", "a_share_spot")
        df_industry = self.read_parquet("a_share", "board_industry")
        df_concept = self.read_parquet("a_share", "board_concept")
        df_zt = self.read_parquet("a_share", "zt_pool")
        df_hot = self.read_parquet("a_share", "hot_rank")
        df_lhb = self.read_parquet("a_share", "lhb_detail")
        df_flow_mkt = self.read_parquet("a_share", "fund_flow_market")
        df_flow_sec = self.read_parquet("a_share", "fund_flow_sector")

        # ── search_index.json ───────────────────────────────────────────
        try:
            records = self._build_search_index(df_spot)
            if records:
                self.write_json(records, "a_share/search_index.json")
                summary["search_index_count"] = len(records)
        except Exception as e:
            summary["errors"].append(f"search_index: {e}")
            self.logger.error(f"A-share search_index failed: {e}")

        # ── top/ per-code (top 300 by 成交额) ──────────────────────────
        try:
            top_records = self._build_top_records(df_spot)
            if top_records:
                n = self.write_per_code_json(
                    top_records, "a_share", "code", top_n=300,
                    sort_field="amount", sort_ascending=False,
                )
                summary["top_files"] = n
        except Exception as e:
            summary["errors"].append(f"top: {e}")
            self.logger.error(f"A-share top failed: {e}")

        # ── rankings ────────────────────────────────────────────────────
        try:
            self._write_rankings(top_records if 'top_records' in dir() else [], df_zt, df_hot, df_lhb)
        except Exception as e:
            summary["errors"].append(f"rankings: {e}")
            self.logger.error(f"A-share rankings failed: {e}")

        # ── boards ──────────────────────────────────────────────────────
        try:
            self._write_boards(df_industry, df_concept)
        except Exception as e:
            summary["errors"].append(f"boards: {e}")
            self.logger.error(f"A-share boards failed: {e}")

        # ── fund_flow ───────────────────────────────────────────────────
        try:
            self._write_fund_flow(df_flow_mkt, df_flow_sec)
        except Exception as e:
            summary["errors"].append(f"fund_flow: {e}")
            self.logger.error(f"A-share fund_flow failed: {e}")

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
                "pe": self.safe_float(row.get("市盈率-动态")),
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
                "pe": self.safe_float(row.get("市盈率-动态")),
                "pb": self.safe_float(row.get("市净率")),
                "market_cap": self.safe_float(row.get("总市值")),
                "float_cap": self.safe_float(row.get("流通市值")),
                "high": self.safe_float(row.get("最高")),
                "low": self.safe_float(row.get("最低")),
                "open": self.safe_float(row.get("今开")),
                "prev_close": self.safe_float(row.get("昨收")),
            })
        return records

    def _write_rankings(self, top_records, df_zt, df_hot, df_lhb):
        valid = [r for r in top_records if r.get("change_pct") is not None]

        # top_gainers.json: top 100 by 涨跌幅
        sorted_gain = sorted(valid, key=lambda r: r["change_pct"], reverse=True)
        self.write_json(sorted_gain[:100], "a_share/rankings/top_gainers.json")

        # top_losers.json: bottom 100
        self.write_json(sorted_gain[-100:][::-1], "a_share/rankings/top_losers.json")

        # top_volume.json: top 100 by 成交额
        sorted_vol = sorted(valid, key=lambda r: r.get("amount") or 0, reverse=True)
        self.write_json(sorted_vol[:100], "a_share/rankings/top_volume.json")

        # hot_stocks.json: merge hot_rank + zt_pool + lhb (deduplicated)
        hot_map = {}  # code → record

        if df_hot is not None and not df_hot.empty:
            for _, row in df_hot.iterrows():
                code = str(row.get("代码", "")).strip()
                if code:
                    hot_map[code] = {
                        "code": code,
                        "name": str(row.get("名称", "")),
                        "price": self.safe_float(row.get("最新价")),
                        "change_pct": self.safe_float(row.get("涨跌幅")),
                        "hot_rank": self.safe_float(row.get("排名")),
                        "sources": ["hot_rank"],
                    }

        if df_zt is not None and not df_zt.empty:
            for _, row in df_zt.iterrows():
                code = str(row.get("代码", "")).strip()
                if not code:
                    continue
                if code in hot_map:
                    hot_map[code]["sources"].append("zt_pool")
                    hot_map[code]["zt_count"] = str(row.get("连板数", ""))
                else:
                    hot_map[code] = {
                        "code": code,
                        "name": str(row.get("名称", "")),
                        "price": self.safe_float(row.get("最新价")),
                        "change_pct": self.safe_float(row.get("涨跌幅")),
                        "zt_count": str(row.get("连板数", "")),
                        "sources": ["zt_pool"],
                    }

        if df_lhb is not None and not df_lhb.empty:
            for _, row in df_lhb.iterrows():
                code = str(row.get("代码", "")).strip()
                if not code:
                    continue
                if code in hot_map:
                    if "lhb" not in hot_map[code]["sources"]:
                        hot_map[code]["sources"].append("lhb")
                        hot_map[code]["lhb_reason"] = str(row.get("上榜原因", ""))
                else:
                    hot_map[code] = {
                        "code": code,
                        "name": str(row.get("名称", "")),
                        "lhb_reason": str(row.get("上榜原因", "")),
                        "lhb_buy": self.safe_float(row.get("买入额")),
                        "lhb_sell": self.safe_float(row.get("卖出额")),
                        "sources": ["lhb"],
                    }

        hot_list = sorted(hot_map.values(), key=lambda r: len(r.get("sources", [])), reverse=True)
        self.write_json(hot_list, "a_share/rankings/hot_stocks.json")

    def _write_boards(self, df_industry, df_concept):
        if df_industry is not None and not df_industry.empty:
            recs = []
            for _, row in df_industry.iterrows():
                recs.append({
                    "name": str(row.get("板块名称", "")),
                    "price": self.safe_float(row.get("最新价")),
                    "change_pct": self.safe_float(row.get("涨跌幅")),
                    "market_cap": self.safe_float(row.get("总市值")),
                    "turnover": self.safe_float(row.get("换手率")),
                    "up_count": self.safe_float(row.get("上涨家数")),
                    "down_count": self.safe_float(row.get("下跌家数")),
                })
            recs.sort(key=lambda r: r.get("change_pct") or 0, reverse=True)
            self.write_json(recs, "a_share/boards/industry.json")

        if df_concept is not None and not df_concept.empty:
            recs = []
            for _, row in df_concept.iterrows():
                recs.append({
                    "name": str(row.get("板块名称", "")),
                    "price": self.safe_float(row.get("最新价")),
                    "change_pct": self.safe_float(row.get("涨跌幅")),
                    "market_cap": self.safe_float(row.get("总市值")),
                    "turnover": self.safe_float(row.get("换手率")),
                    "up_count": self.safe_float(row.get("上涨家数")),
                    "down_count": self.safe_float(row.get("下跌家数")),
                })
            recs.sort(key=lambda r: r.get("change_pct") or 0, reverse=True)
            self.write_json(recs, "a_share/boards/concept.json")

    def _write_fund_flow(self, df_flow_mkt, df_flow_sec):
        if df_flow_mkt is not None and not df_flow_mkt.empty:
            recs = self.df_to_records(df_flow_mkt)
            self.write_json(recs, "a_share/fund_flow/market.json")

        if df_flow_sec is not None and not df_flow_sec.empty:
            recs = self.df_to_records(df_flow_sec)
            self.write_json(recs, "a_share/fund_flow/sector.json")
