from __future__ import annotations

import pandas as pd

from coordwatch.utils.soma import (
    estimate_modified_duration,
    estimate_runoff_duration_equivalent,
    holdings_frame_from_payload,
)


def test_holdings_frame_from_payload_parses_numeric_and_dates() -> None:
    payload = {
        "soma": {
            "holdings": [
                {
                    "asOfDate": "2024-01-03",
                    "maturityDate": "2024-06-03",
                    "coupon": "4.25",
                    "parValue": "1000000000",
                    "inflationCompensation": "",
                    "changeFromPriorWeek": "-250000000",
                    "securityType": "NotesBonds",
                }
            ]
        }
    }
    frame = holdings_frame_from_payload(payload)
    assert len(frame) == 1
    assert pd.api.types.is_datetime64_any_dtype(frame["asOfDate"])
    assert pd.api.types.is_datetime64_any_dtype(frame["maturityDate"])
    assert float(frame.loc[0, "parValue"]) == 1_000_000_000
    assert float(frame.loc[0, "changeFromPriorWeek"]) == -250_000_000


def test_estimate_modified_duration_respects_security_type() -> None:
    curve = {"DGS2": 4.5, "DGS5": 4.2, "DGS10": 4.1, "DGS20": 4.4, "DGS30": 4.3}
    bill_duration = estimate_modified_duration(0.5, 0.0, "Bills", curve)
    frn_duration = estimate_modified_duration(2.0, 0.0, "FRN", curve)
    note_duration = estimate_modified_duration(5.0, 4.0, "NotesBonds", curve)
    assert 0 < bill_duration < 0.5
    assert frn_duration == 0.25
    assert note_duration > 3.0


def test_estimate_runoff_duration_equivalent_counts_declines_only() -> None:
    prior_holdings = pd.DataFrame(
        [
            {
                "cusip": "bill1",
                "maturityDate": pd.Timestamp("2024-07-03"),
                "coupon": 0.0,
                "parValue": 2_000_000_000,
                "inflationCompensation": 0.0,
                "securityType": "Bills",
            },
            {
                "cusip": "note1",
                "maturityDate": pd.Timestamp("2029-01-03"),
                "coupon": 4.0,
                "parValue": 3_000_000_000,
                "inflationCompensation": 0.0,
                "securityType": "NotesBonds",
            },
            {
                "cusip": "frn1",
                "maturityDate": pd.Timestamp("2026-01-03"),
                "coupon": 0.0,
                "parValue": 1_000_000_000,
                "inflationCompensation": 0.0,
                "securityType": "FRN",
            },
        ]
    )
    current_holdings = pd.DataFrame(
        [
            {
                "cusip": "bill1",
                "maturityDate": pd.Timestamp("2024-07-03"),
                "coupon": 0.0,
                "parValue": 1_500_000_000,
                "inflationCompensation": 0.0,
                "securityType": "Bills",
            },
            {
                "cusip": "note1",
                "maturityDate": pd.Timestamp("2029-01-03"),
                "coupon": 4.0,
                "parValue": 2_000_000_000,
                "inflationCompensation": 0.0,
                "securityType": "NotesBonds",
            },
            {
                "cusip": "frn1",
                "maturityDate": pd.Timestamp("2026-01-03"),
                "coupon": 0.0,
                "parValue": 1_250_000_000,
                "inflationCompensation": 0.0,
                "securityType": "FRN",
            },
        ]
    )
    curve = {"DGS2": 4.5, "DGS5": 4.2, "DGS10": 4.1, "DGS20": 4.4, "DGS30": 4.3}
    runoff = estimate_runoff_duration_equivalent(prior_holdings, current_holdings, curve, "2024-01-03")
    assert runoff > 3.0
    assert runoff < 6.5
