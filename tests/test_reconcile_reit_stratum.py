"""Public REITs get their own reconcile stratum.

They were ~100 codes inside "other" (thousands of codes), so the sampler
reached one about once a month — which is how long the phantom same-day
rows upstream publishes and withdraws for REITs went unnoticed.
"""
from shared.quality.reconcile import _stratum


def test_public_reits_have_their_own_stratum():
    assert _stratum("Reits") == "reit"


def test_qdii_reits_stay_in_qdii():
    # Open-end funds holding overseas REIT shares; daily NAV like any QDII.
    assert _stratum("QDII-REITs") == "qdii"


def test_other_types_unchanged():
    assert _stratum("货币型-普通货币") == "money_market"
    assert _stratum("指数型-股票") == "index_etf"
    assert _stratum("") == "other"
    assert _stratum(None) == "other"
