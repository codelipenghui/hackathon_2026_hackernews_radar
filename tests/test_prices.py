from datetime import datetime
from zoneinfo import ZoneInfo

from common import missing_keys
from prices import completed_ticks

NY = ZoneInfo("America/New_York")
BARS = [(datetime(2026, 10, 7, 9, 30, tzinfo=NY), 120.0, 900), (datetime(2026, 10, 7, 9, 35, tzinfo=NY), 121.5, 700),
        (datetime(2026, 10, 7, 9, 40, tzinfo=NY), 122.0, 300)]
T = [int(b[0].timestamp() * 1000) for b in BARS]


def test_only_completed_bars_after_the_last_published_one():
    now = T[2] + 60_000  # the 09:40 bar is still forming
    ticks = completed_ticks(BARS, "NVDA", now_ms=now, after_ts=T[0])
    assert [(t["ts"], t["price"]) for t in ticks] == [(T[1], 121.5)]
    assert ticks[0]["run"] == "live" and ticks[0]["session"] == "regular"
    assert missing_keys("ticks", ticks[0]) == set()


def test_first_poll_publishes_the_whole_session_so_far():
    assert len(completed_ticks(BARS, "NVDA", now_ms=T[2] + 300_000, after_ts=None)) == 3
