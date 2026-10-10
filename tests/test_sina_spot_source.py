"""Pin the Sina ETF/LOF spot source's shape against what consumers expect.

eastmoney's push2 quote API (akshare fund_etf_spot_em / fund_lof_spot_em)
has answered 502 or dropped the connection on every daily run since
2026-09-18 (ETF) and 2026-08-26 (LOF); the newest akshare (1.19.1) fails the
same way from AWS, so this is the endpoint, not the client. The Sina list
(akshare fund_etf_category_sina) answers in ~3s with the same per-fund
fields, but prefixes codes with the exchange ("sz159998"). The downstream
contract is:

- fund_processor builds top_etf.json from 代码/名称/最新价/涨跌幅/成交量/成交额
- the Iceberg mapping keys on the same Chinese headers
- fund_code is the bare 6-digit code everywhere else in the lake
"""
from __future__ import annotations

import pandas as pd

from shared.fetchers.fund_fetcher import _normalize_sina_spot


def _sina_frame():
    return pd.DataFrame({
        "代码": ["sz159998", "sh510300", "sz169201"],
        "名称": ["计算机ETF天弘", "沪深300ETF", "浙商鼎盈LOF"],
        "最新价": [0.890, 4.012, 1.635],
        "涨跌额": [0.011, -0.020, -0.039],
        "涨跌幅": [1.251, -0.496, -2.330],
        "买入": [0.890, 4.012, 1.625],
        "卖出": [0.891, 4.013, 1.635],
        "昨收": [0.879, 4.032, 1.674],
        "今开": [0.873, 4.030, 1.657],
        "最高": [0.892, 4.040, 1.657],
        "最低": [0.852, 4.000, 1.607],
        "成交量": [77964100, 1000, 12258],
        "成交额": [67527209, 4012, 19892],
    })


def test_exchange_prefix_is_stripped_from_code():
    out = _normalize_sina_spot(_sina_frame())
    assert out["代码"].tolist() == ["159998", "510300", "169201"]


def test_consumer_columns_survive():
    out = _normalize_sina_spot(_sina_frame())
    for col in ("代码", "名称", "最新价", "涨跌幅", "成交量", "成交额"):
        assert col in out.columns, col


def test_bare_codes_are_left_alone():
    df = _sina_frame()
    df["代码"] = ["159998", "510300", "169201"]
    out = _normalize_sina_spot(df)
    assert out["代码"].tolist() == ["159998", "510300", "169201"]


def test_empty_frame_is_returned_unchanged():
    out = _normalize_sina_spot(pd.DataFrame())
    assert out.empty
