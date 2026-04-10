from __future__ import annotations

from coordwatch.utils.treasury import extract_cash_balance_assumption, extract_statement_signal_hints


def test_extract_cash_balance_assumption_single_month() -> None:
    text = (
        "Treasury estimates borrowing needs for the quarter and assumes an end-of-March 2010 "
        "cash balance of $200 billion."
    )
    assert extract_cash_balance_assumption(text, quarter="2010Q1") == 200.0


def test_extract_cash_balance_assumption_month_pair() -> None:
    text = (
        "Assumes end-of-March 2026 and end-of-June 2026 cash balances of $850 billion and "
        "$900 billion, respectively, versus end-of-December 2025 cash balance of $873 billion."
    )
    assert extract_cash_balance_assumption(text, quarter="2026Q1") == 850.0
    assert extract_cash_balance_assumption(text, quarter="2026Q2") == 900.0


def test_extract_cash_balance_assumption_generic_fallback() -> None:
    text = "Treasury assumes a cash balance of 350 billion at quarter-end."
    assert extract_cash_balance_assumption(text, quarter="2014Q2") == 350.0


def test_extract_statement_signal_hints_flags_multiple_channels() -> None:
    text = (
        "Treasury presented the quarterly refunding statement after consulting the Treasury Borrowing Advisory Committee. "
        "Officials emphasized regular and predictable issuance, market functioning, and cash management. "
        "Bill issuance will remain flexible and buyback operations will continue."
    )
    signals = extract_statement_signal_hints(text)
    assert signals["tbac_mention_flag"] == 1
    assert signals["market_function_mention_flag"] == 1
    assert signals["regular_predictable_mention_flag"] == 1
    assert signals["bill_flexibility_mention_flag"] == 1
    assert signals["cash_management_mention_flag"] == 1
    assert signals["buyback_mention_flag"] == 1
