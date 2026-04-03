
from __future__ import annotations

from datetime import datetime

import pandas as pd


REFUNDING_MONTH_BY_QUARTER = {1: 2, 2: 5, 3: 8, 4: 11}


def to_timestamp(value) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def quarter_string(value) -> str:
    ts = to_timestamp(value)
    return f"{ts.year}Q{ts.quarter}"


def refunding_date_for_quarter(period: pd.Period | str) -> pd.Timestamp:
    p = pd.Period(period, freq="Q")
    month = REFUNDING_MONTH_BY_QUARTER[p.quarter]
    # Treasury refundings are typically early in Feb/May/Aug/Nov.
    return pd.Timestamp(datetime(p.year, month, 3))


def quarter_start(period: pd.Period | str) -> pd.Timestamp:
    return pd.Period(period, freq="Q").start_time.normalize()


def quarter_end(period: pd.Period | str) -> pd.Timestamp:
    return pd.Period(period, freq="Q").end_time.normalize()
