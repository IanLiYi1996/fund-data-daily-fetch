from datetime import date
from unittest.mock import Mock

import pytest


def _response():
    return """var apidata={content:"<div class='boxitem'><h4 class='t'>
    测试基金 2026年3季度股票投资明细 截止至：2026-09-30</h4><table>
    <thead><tr><th>股票名称</th><th>持仓市值（万元人民币）</th><th>股票代码</th>
    <th>持股数（万股）</th><th>占净值<br/>比例</th></tr></thead>
    <tbody><tr><td>示例股份</td><td>1,234.56</td><td>00700</td>
    <td>12.34</td><td>2.10%</td></tr></tbody></table></div>",
    arryear:[2026],curyear:2026};""".replace("\n", "") + "\r\n"


def test_parser_matches_columns_by_name_and_preserves_hk_codes():
    from shared.quality.portfolio_hold import parse_holdings
    frame = parse_holdings(_response(), "000001", 2026, as_of=date(2026, 11, 16))
    row = frame.iloc[0]
    assert row.fund_code == "000001"
    assert row.holding_code == "00700"
    assert row.report_date == date(2026, 9, 30)
    assert row.weight_pct == 2.1
    assert row.shares == 123400
    assert row.market_value == 12345600


def test_fetch_checks_status_and_supplies_referer():
    from shared.quality.portfolio_hold import fetch_holdings
    response = Mock(text=_response())
    get = Mock(return_value=response)
    frame = fetch_holdings("000001", 2026, get=get)
    assert not frame.empty
    response.raise_for_status.assert_called_once()
    assert get.call_args.kwargs["headers"]["Referer"].endswith("ccmx_000001.html")
    assert get.call_args.kwargs["timeout"] == (20, 45)


@pytest.mark.parametrize("body", ["<html>404</html>", "var apidata={content:'bad'};"])
def test_error_page_and_unexpected_payload_are_not_no_data(body):
    from shared.quality.portfolio_hold import parse_holdings
    with pytest.raises(ValueError):
        parse_holdings(body, "000001", 2026)


def test_explicit_empty_disclosure_is_valid_no_data():
    from shared.quality.portfolio_hold import parse_holdings
    frame = parse_holdings("var apidata={content:'',arryear:[],curyear:2026};", "000001", 2026)
    assert frame.empty


def test_future_quarter_cannot_be_written():
    from shared.quality.portfolio_hold import parse_holdings
    future = _response().replace("3季度", "4季度").replace("2026-09-30", "2026-12-31")
    assert parse_holdings(future, "000001", 2026, as_of=date(2026, 11, 16)).empty


def test_missing_required_column_is_an_error():
    from shared.quality.portfolio_hold import parse_holdings
    with pytest.raises(ValueError, match="column"):
        parse_holdings(_response().replace("股票代码", "未知字段"), "000001", 2026)


def test_within_quarter_cutoff_preserves_report_period_and_observation_date():
    from shared.quality.portfolio_hold import parse_holdings
    frame = parse_holdings(
        _response().replace("2026-09-30", "2026-09-29"), "001884", 2026,
        as_of=date(2026, 11, 16), target_quarter=date(2026, 9, 30),
    )
    assert frame.iloc[0].report_date == date(2026, 9, 30)
    assert frame.attrs["observation_cutoffs"] == {"2026-09-30": "2026-09-29"}


@pytest.mark.parametrize("cutoff", ["2026-06-30", "2026-10-01", "2026-09-31"])
def test_cross_quarter_or_invalid_cutoff_is_not_relaxed(cutoff):
    from shared.quality.portfolio_hold import parse_holdings
    with pytest.raises(ValueError):
        parse_holdings(_response().replace("2026-09-30", cutoff), "000001", 2026)


def test_target_only_skips_unrelated_broken_disclosure_before_parsing():
    from shared.quality.portfolio_hold import parse_holdings
    older = (
        "<div><h4 class='t'>2026年2季度 截止至：2026-05-09</h4>"
        "<table><thead><tr><th>other</th></tr></thead></table></div>"
    )
    body = _response().replace('</div>",', f'</div>{older}",')
    frame = parse_holdings(
        body, "159011", 2026, target_quarter=date(2026, 9, 30),
        as_of=date(2026, 11, 16),
    )
    assert len(frame) == 1
    assert set(frame.report_date) == {date(2026, 9, 30)}


def test_disclosed_triple_dash_is_null_not_zero():
    from shared.quality.portfolio_hold import parse_holdings
    frame = parse_holdings(
        _response().replace("1,234.56", "---"), "010992", 2026,
        target_quarter=date(2026, 9, 30),
    )
    assert frame.iloc[0].market_value is None
