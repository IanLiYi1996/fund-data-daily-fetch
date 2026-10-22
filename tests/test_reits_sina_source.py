"""Pin the Sina REIT quote source against the eastmoney shape it replaces.

ak.reits_realtime_em hits 95.push2.eastmoney.com, the same push2 quote API
that killed fund_etf / fund_lof: on 2026-10-21 all three retries got
RemoteDisconnected, and every push2 host (95./82./push2/push2delay) answered
empty bodies or 502 to most probes on 10-22. It also hard-codes pz=100 while
the market has 103 REITs, so even "successful" runs dropped 3 funds a day.

Replacement: REIT codes from fund_name_em (基金类型 == "Reits", 103 on
10-22), quotes from Sina hq.sinajs.cn. Contract with the Iceberg mapping and
the existing series:
- 代码 is the bare 6-digit code; 名称/最新价/涨跌幅/成交量/成交额 present
- 成交量 in 手 (lots of 100) like eastmoney's f5; Sina reports shares
- 数据日期 is Sina's quote date, so a weekend run upserts Friday's row
"""
from __future__ import annotations

import math

import pandas as pd
import pytest

from shared.fetchers import fund_fetcher
from shared.fetchers.fund_fetcher import (
    _parse_sina_hq,
    _reit_codes,
    _sina_symbol,
)

# Captured from hq.sinajs.cn 2026-10-22 10:05 CST (bid/ask tail trimmed)
SINA_TEXT = (
    'var hq_str_sh508000="张江REIT,1.855,1.855,1.860,1.860,1.843,1.859,1.860,'
    '77614,143806.000,400,1.859,200,1.858,120200,1.857,100,1.856,348700,1.854,'
    '1500,1.860,120000,1.865,450200,1.867,20000,1.868,35800,1.870,'
    '2026-10-22,10:05:44,00,";\n'
    'var hq_str_sz180101="蛇口产园,1.465,1.471,1.461,1.470,1.460,1.460,1.463,'
    '166400,243753.700,1000,1.460,1600,1.459,398900,1.458,77300,1.457,500,1.456,'
    '600,1.463,19400,1.464,310720,1.465,160200,1.466,75000,1.468,'
    '2026-10-22,10:05:45,00";\n'
    # Suspended / not yet traded: Sina reports price 0
    'var hq_str_sh508099="停牌REIT,0.000,2.100,0.000,0.000,0.000,0.000,0.000,'
    '0,0.000,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-10-22,10:05:45,03";\n'
    # Unknown code: Sina returns an empty string
    'var hq_str_sh508998="";\n'
)


def test_sina_symbol_prefix():
    assert _sina_symbol("508000") == "sh508000"
    assert _sina_symbol("180101") == "sz180101"
    assert _sina_symbol("006283") is None


def test_reit_codes_only_exchange_reits():
    fn = pd.DataFrame({
        "基金代码": ["508000", "180101", "006283", "000001"],
        "基金类型": ["Reits", "Reits", "QDII-REITs", "混合型-偏股"],
    })
    assert _reit_codes(fn) == ["508000", "180101"]


def test_parse_shape_and_units():
    df = _parse_sina_hq(SINA_TEXT)

    assert list(df["代码"]) == ["508000", "180101", "508099"]
    r = df.set_index("代码").loc["508000"]
    assert r["名称"] == "张江REIT"
    assert r["最新价"] == pytest.approx(1.860)
    assert r["昨收"] == pytest.approx(1.855)
    assert r["涨跌额"] == pytest.approx(0.005)
    assert r["涨跌幅"] == pytest.approx(0.27, abs=0.005)
    assert r["成交量"] == pytest.approx(776.14)      # shares -> 手
    assert r["成交额"] == pytest.approx(143806.0)
    assert r["数据日期"] == "2026-10-22"
    for col in ("代码", "名称", "最新价", "涨跌幅", "成交量", "成交额"):
        assert col in df.columns


def test_untraded_quote_has_no_price():
    r = _parse_sina_hq(SINA_TEXT).set_index("代码").loc["508099"]
    assert math.isnan(r["最新价"])
    assert math.isnan(r["涨跌幅"])
    assert r["昨收"] == pytest.approx(2.100)


def test_fetch_batches_and_fails_loudly_on_empty(monkeypatch):
    fn = pd.DataFrame({"基金代码": [f"508{i:03d}" for i in range(250)],
                       "基金简称": ["x"] * 250, "基金类型": ["Reits"] * 250})
    monkeypatch.setattr(fund_fetcher.ak, "fund_name_em", lambda: fn)
    calls = []

    def fake_get(symbols):
        calls.append(symbols)
        return ""

    monkeypatch.setattr(fund_fetcher, "_get_sina_hq", fake_get)

    with pytest.raises(ValueError, match="0 of 250"):
        fund_fetcher.FundFetcher()._fetch_fund_reits_daily()
    assert [len(c) for c in calls] == [200, 50]


def test_fetch_keeps_unquoted_reits_as_unpriced_rows(monkeypatch):
    """Registered-but-not-trading REITs (e.g. 508605) get no Sina quote;
    eastmoney listed them with a null price, so the row set stays whole."""
    fn = pd.DataFrame({
        "基金代码": ["508000", "180101", "508605"],
        "基金简称": ["张江REIT", "蛇口产园", "华夏保利商业REIT"],
        "基金类型": ["Reits"] * 3,
    })
    monkeypatch.setattr(fund_fetcher.ak, "fund_name_em", lambda: fn)
    monkeypatch.setattr(fund_fetcher, "_get_sina_hq", lambda s: SINA_TEXT)

    df = fund_fetcher.FundFetcher()._fetch_fund_reits_daily().set_index("代码")

    assert {"508000", "180101", "508605"} <= set(df.index)
    r = df.loc["508605"]
    assert r["名称"] == "华夏保利商业REIT"
    assert math.isnan(r["最新价"])
    assert r["数据日期"] == "2026-10-22"
    assert df["数据日期"].notna().all()


def test_writer_uses_quote_date_not_run_date(tmp_path):
    """A Saturday run must upsert Friday's row, not create a Saturday one."""
    from datetime import date

    from pyiceberg.catalog.sql import SqlCatalog

    from shared.storage.iceberg_writer import IcebergWriter

    (tmp_path / "wh").mkdir()
    cat = SqlCatalog("t", uri=f"sqlite:///{tmp_path}/c.db",
                     warehouse=f"file://{tmp_path}/wh")
    cat.create_namespace("fund_data_lake")
    w = IcebergWriter(catalog=cat, database="fund_data_lake")

    out = w.write("fund_reits_daily", _parse_sina_hq(SINA_TEXT),
                  fetch_date=date(2026, 10, 24))

    assert out.get("rows_inserted") == 3, out
    got = cat.load_table("fund_data_lake.fund_reits_daily").scan().to_pandas()
    assert set(got["trade_date"].astype(str)) == {"2026-10-22"}
    r = got.set_index("fund_code").loc["508000"]
    assert r["latest_price"] == pytest.approx(1.860)
    assert r["volume"] == pytest.approx(776.14)
