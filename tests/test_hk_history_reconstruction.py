"""Historical valuation reconstruction is not an observed ranking snapshot."""
from datetime import date
from unittest.mock import Mock

import pytest

from shared.quality.hk_history import (
    build_valuations,
    fetch_history,
    history_start,
    normalize_history,
    normalize_members,
    replay_history,
    seed_frozen_anchor,
    validate_controls,
)


def member(code="968063", hk="1002075744", nav_date="2026-10-23", nav=22.12):
    return {
        "基金代码": code, "香港基金代码": hk, "基金简称": "test",
        "币种": "美元", "日期": nav_date, "单位净值": nav,
    }


def record(day, nav, code="968063", hk="1002075744", growth=""):
    return {
        "NAVCHGRT": growth, "NAV": nav, "PDATE": day, "HKFCODE": hk,
        "FCODE": code, "CURRENCY": "美元", "new_field": "ignored",
    }


def history(rows, code="968063", hk="1002075744"):
    return normalize_history(
        rows, code, hk, date(2025, 9, 1), date(2026, 10, 29),
    )


def test_members_use_historical_membership_and_reject_duplicates():
    out = normalize_members([member()])
    assert out[0]["fund_code"] == "968063"
    assert out[0]["nav_date"] == date(2026, 10, 23)
    with pytest.raises(ValueError, match="duplicate"):
        normalize_members([member(), member()])
    with pytest.raises(ValueError, match="fund code"):
        normalize_members([member(code="美元")])


def test_never_reported_member_is_kept_without_fabricated_nav():
    members = normalize_members([member(nav_date=None, nav=None)])
    rows = build_valuations(members, {"968063": []}, [date(2026, 10, 27)])
    assert len(rows) == 1
    assert rows[0]["unit_nav"] is None
    assert rows[0]["nav_date"] is None
    assert rows[0]["nav_status"] == "no_reported_nav"
    assert rows[0]["nav_change_7d_pct"] is None


def test_history_parser_is_keyed_and_preserves_actual_source_dates():
    row = dict(reversed(list(record("2026-10-27", "21.93", growth="0.3661").items())))
    out = history([row])
    assert out[0]["nav_date"] == date(2026, 10, 27)
    assert out[0]["unit_nav"] == 21.93
    assert out[0]["daily_return_pct"] == 0.3661


@pytest.mark.parametrize("field,value", [
    ("FCODE", "968999"), ("HKFCODE", "wrong"), ("PDATE", "2026-10-30"),
    ("PDATE", "not-a-date"), ("NAV", float("nan")),
    ("NAV", float("inf")), ("NAV", -1), ("NAV", 0),
])
def test_source_identity_dates_and_numeric_values_fail_closed(field, value):
    row = record("2026-10-27", 21.93)
    row[field] = value
    with pytest.raises(ValueError):
        history([row])


def test_missing_required_source_field_fails_closed():
    row = record("2026-10-27", 21.93)
    del row["PDATE"]
    with pytest.raises(ValueError, match="missing"):
        history([row])


def test_duplicate_source_dates_are_not_silently_deduplicated():
    with pytest.raises(ValueError, match="duplicate"):
        history([record("2026-10-27", 21.93), record("2026-10-27", 22.0)])


def test_blank_growth_is_null_not_zero():
    out = history([record("2026-10-27", 21.93)])
    assert out[0]["daily_return_pct"] is None


def test_as_of_selection_never_uses_the_next_days_nav():
    members = normalize_members([member()])
    source = history([
        record("2026-10-26", 21.85), record("2026-10-27", 21.93),
        record("2026-10-28", 22.51), record("2026-10-29", 22.08),
    ])
    rows = build_valuations(
        members, {"968063": source}, [date(2026, 10, 27), date(2026, 10, 28)],
    )
    assert [row["unit_nav"] for row in rows] == [21.93, 22.51]
    for row in rows:
        assert row["nav_date"] <= row["as_of_date"]
        assert row["data_origin"] == "reconstructed"
        assert row["as_of_basis"] == "nav_date_cutoff"
        assert row["observation_time_recovered"] is False
        assert "snapshot_date" not in row
        assert "weekly_return" not in row
        assert "monthly_return" not in row
        assert "yearly_return" not in row


def test_known_stale_nav_is_not_relabelled_as_a_fresh_observation():
    members = normalize_members([member(nav_date="2026-09-04", nav=10)])
    source = history([record("2026-09-04", 10)])
    row = build_valuations(members, {"968063": source}, [date(2026, 10, 27)])[0]
    assert row["nav_date"] == date(2026, 9, 4)
    assert row["nav_age_days"] == 53
    assert row["nav_status"] == "carried_forward"
    assert row["nav_change_7d_pct"] is None


def test_price_changes_have_explicit_baselines_and_are_not_total_returns():
    members = normalize_members([member()])
    source = history([
        record("2025-10-27", 10), record("2026-09-25", 20),
        record("2026-10-20", 21), record("2026-10-27", 22),
    ])
    row = build_valuations(members, {"968063": source}, [date(2026, 10, 27)])[0]
    assert row["nav_change_7d_pct"] == pytest.approx(4.7619)
    assert row["nav_change_1m_pct"] == 10
    assert row["nav_change_1y_pct"] == 120
    assert row["baseline_1m_date"] == date(2026, 9, 25)
    assert row["return_basis"] == "unit_nav_price_change_no_dividend_reinvestment"


def test_short_history_does_not_invent_a_month_or_year_return():
    members = normalize_members([member(nav_date="2026-10-23", nav=100)])
    source = history([record("2026-10-23", 100), record("2026-10-27", 101)])
    row = build_valuations(members, {"968063": source}, [date(2026, 10, 27)])[0]
    assert row["nav_change_7d_pct"] is None
    assert row["nav_change_1m_pct"] is None
    assert row["nav_change_1y_pct"] is None


def test_price_change_never_compares_navs_in_different_currencies():
    members = normalize_members([member()])
    source = history([record("2026-10-20", 21), record("2026-10-27", 22)])
    source[0]["currency"] = "人民币"
    row = build_valuations(members, {"968063": source}, [date(2026, 10, 27)])[0]
    assert row["nav_change_7d_pct"] is None
    assert row["baseline_7d_date"] is None


def test_controls_compare_to_actual_anchor_nav_date_not_collection_date():
    members = normalize_members([member()])
    source = history([record("2026-10-23", 22.12), record("2026-10-27", 21.93)])
    assert validate_controls(members, {"968063": source}) == 1
    with pytest.raises(ValueError, match="control"):
        validate_controls(members, {"968063": history([record("2026-10-23", 23)])})
    with pytest.raises(ValueError, match="control"):
        validate_controls(members, {"968063": []})


def test_every_member_requires_a_successful_fetch_even_when_empty():
    members = normalize_members([member(nav_date=None, nav=None)])
    with pytest.raises(ValueError, match="history"):
        build_valuations(members, {}, [date(2026, 10, 27)])


def test_duplicate_targets_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        build_valuations(normalize_members([member()]), {"968063": []},
                         [date(2026, 10, 27)] * 2)


def test_history_window_includes_frozen_funds_and_leap_years():
    m = normalize_members([member(nav_date="2017-11-09", nav=46.06)])[0]
    assert history_start(m, date(2026, 10, 27)) < date(2016, 11, 9)
    m = normalize_members([member(nav_date="2024-02-29", nav=10)])[0]
    assert history_start(m, date(2024, 3, 1)) < date(2023, 2, 28)


def test_only_long_frozen_empty_history_can_use_the_known_good_anchor():
    members = normalize_members([member(nav_date="2017-11-09", nav=46.06)])
    seeded, codes = seed_frozen_anchor(members, {"968063": []}, date(2026, 10, 26))
    assert codes == ["968063"]
    assert validate_controls(members, seeded) == 1
    row = build_valuations(members, seeded, [date(2026, 10, 27)])[0]
    assert row["nav_source"] == "known_good_raw_snapshot"
    assert row["nav_date"] == date(2017, 11, 9)
    assert row["unit_nav"] == 46.06
    assert row["nav_change_7d_pct"] is None
    assert seeded["968063"][0]["source_observation_date"] == date(2026, 10, 26)

    recent = normalize_members([member()])
    seeded, codes = seed_frozen_anchor(recent, {"968063": []}, date(2026, 10, 26))
    assert codes == []
    with pytest.raises(ValueError, match="control"):
        validate_controls(recent, seeded)


def test_frozen_anchor_never_overrides_a_conflicting_source_value():
    members = normalize_members([member(nav_date="2025-10-27", nav=10)])
    source = history([record("2025-10-27", 11)])
    seeded, codes = seed_frozen_anchor(members, {"968063": source}, date(2026, 10, 29))
    assert codes == []
    with pytest.raises(ValueError, match="control"):
        validate_controls(members, seeded)


def test_frozen_anchor_is_never_used_before_its_known_observation_date():
    members = normalize_members([member(nav_date="2017-11-09", nav=46.06)])
    seeded, _ = seed_frozen_anchor(members, {"968063": []}, date(2026, 10, 26))
    row = build_valuations(members, seeded, [date(2026, 10, 25)])[0]
    assert row["unit_nav"] is None
    assert row["nav_status"] == "no_reported_nav"


def response(data, total, code="1"):
    r = Mock()
    r.json.return_value = {"Code": code, "Data": data, "TotalCount": total}
    return r


def test_fetch_paginates_with_timeouts_and_checks_http_status():
    get = Mock(side_effect=[
        response([record("2026-10-27", 21.93)], 2),
        response([record("2026-10-28", 22.51)], 2),
    ])
    m = normalize_members([member()])[0]
    records, evidence = fetch_history(
        m, date(2025, 9, 1), date(2026, 10, 29), get=get,
    )
    assert len(records) == 2
    assert len(evidence) == 2
    assert get.call_count == 2
    assert [c.kwargs["params"]["pageindex"] for c in get.call_args_list] == ["0", "1"]
    assert all(c.kwargs["timeout"] <= 20 for c in get.call_args_list)


@pytest.mark.parametrize("pages", [
    [response([], 2)],
    [response([], 0, code="-1")],
    [response([record("2026-10-27", 21.93)], 2), response([], 2)],
    [response([record("2026-10-27", 21.93)], 2),
     response([record("2026-10-28", 22.51)], 3)],
])
def test_failed_or_incomplete_pagination_is_not_reported_as_missing_nav(pages):
    with pytest.raises(ValueError):
        fetch_history(normalize_members([member()])[0],
                      date(2025, 9, 1), date(2026, 10, 29),
                      get=Mock(side_effect=pages))


def test_successful_empty_history_is_distinct_from_upstream_failure():
    records, evidence = fetch_history(
        normalize_members([member(nav_date=None, nav=None)])[0],
        date(2025, 9, 1), date(2026, 10, 29),
        get=Mock(return_value=response([], 0)),
    )
    assert records == []
    assert len(evidence) == 1


def test_captured_sources_replay_through_the_real_parser_and_pagination():
    m = normalize_members([member()])[0]
    get = Mock(return_value=response([record("2026-10-27", 21.93)], 1))
    records, evidence = fetch_history(m, date(2025, 9, 1), date(2026, 10, 29), get=get)
    assert replay_history(m, date(2025, 9, 1), date(2026, 10, 29), evidence) == records
    evidence[0]["request"]["hkfcode"] = "wrong"
    with pytest.raises(ValueError, match="request mismatch"):
        replay_history(m, date(2025, 9, 1), date(2026, 10, 29), evidence)


def test_captured_sources_reject_extra_pages_and_identity_changes():
    m = normalize_members([member()])[0]
    _, evidence = fetch_history(
        m, date(2025, 9, 1), date(2026, 10, 29),
        get=Mock(return_value=response([record("2026-10-27", 21.93)], 1)),
    )
    with pytest.raises(ValueError, match="unconsumed"):
        replay_history(m, date(2025, 9, 1), date(2026, 10, 29), evidence * 2)
    evidence[0]["response"]["Data"][0]["FCODE"] = "968999"
    with pytest.raises(ValueError, match="identity"):
        replay_history(m, date(2025, 9, 1), date(2026, 10, 29), evidence)
