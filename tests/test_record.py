from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from record import filing_from_submissions, list_earnings, select_window, ticks_from_bars

NY = ZoneInfo("America/New_York")
SUB = {"cik": "723125", "name": "MICRON TECHNOLOGY INC", "filings": {"recent": {
    "accessionNumber": ["0000723125-25-000039", "0000723125-25-000041"],
    "form": ["4", "8-K"],
    "items": ["", "2.02,9.01"],
    "acceptanceDateTime": ["2025-09-20T14:00:00.000Z", "2025-09-23T20:05:12.000Z"],
}}}
FILED = int(datetime(2025, 9, 23, 20, 5, 12, tzinfo=timezone.utc).timestamp() * 1000)


def test_filing_from_submissions():
    f = filing_from_submissions(SUB, "MU", "0000723125-25-000041")
    assert f == {"run": "", "ts": FILED, "ticker": "MU", "cik": "0000723125", "form": "8-K",
                 "items": ["2.02", "9.01"], "accession": "0000723125-25-000041",
                 "url": "https://www.sec.gov/Archives/edgar/data/723125/000072312525000041/",
                 "title": "MICRON TECHNOLOGY INC - 8-K"}


def test_list_earnings_only_returns_item_202_8ks():
    assert list_earnings(SUB, since_ms=0) == [("0000723125-25-000041", FILED)]
    assert list_earnings(SUB, since_ms=FILED + 1) == []


def t(day, hh, mm):
    return datetime(2025, 9, day, hh, mm, tzinfo=NY)


def test_ticks_from_bars_assigns_sessions():
    bars = [(t(24, 9, 25), 150.0, 10), (t(24, 9, 30), 151.0, 20), (t(24, 15, 55), 152.0, 30), (t(24, 16, 0), 153.0, 40)]
    ticks = ticks_from_bars(bars, "MU")
    assert [x["session"] for x in ticks] == ["pre", "regular", "regular", "post"]
    assert ticks[1] == {"run": "", "ts": int(t(24, 9, 30).timestamp() * 1000), "ticker": "MU",
                        "price": 151.0, "volume": 20, "session": "regular"}


def test_select_window_keeps_filing_window_and_next_session_from_8am():
    filing_ts = int(t(23, 16, 5).timestamp() * 1000)
    times = [t(23, 15, 0), t(23, 16, 0), t(23, 19, 0), t(24, 7, 0), t(24, 8, 0), t(24, 9, 30), t(24, 16, 0)]
    ticks = ticks_from_bars([(x, 100.0, 1) for x in times], "MU")
    kept = [datetime.fromtimestamp(x["ts"] / 1000, NY).strftime("%d %H:%M") for x in select_window(ticks, filing_ts)]
    assert kept == ["23 16:00", "24 08:00", "24 09:30"]


def test_select_window_needs_a_regular_session_after_the_filing():
    ticks = ticks_from_bars([(t(23, 16, 0), 100.0, 1)], "MU")
    with pytest.raises(ValueError):
        select_window(ticks, int(t(23, 16, 5).timestamp() * 1000))
