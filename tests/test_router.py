from common import missing_keys
from router import Router

WATCH = {"MU": {"cik": "0000723125", "names": ["Micron"]}}
F = {"run": "replay-1", "ts": 1758657912000, "ticker": "MU", "cik": "0000723125", "form": "8-K",
     "items": ["2.02", "9.01"], "accession": "0000723125-25-000041", "url": "u", "title": "t"}


def test_earnings_8k_triggers_earnings_review():
    req = Router(WATCH).on_filing(F)
    assert req == {"run": "replay-1", "ts": 1758657912000, "request_id": "MU-0000723125-25-000041", "ticker": "MU",
                   "skill": "earnings-review", "args": "MU earnings filed 2025-09-23", "reason": "8-K Item 2.02",
                   "trigger": {"topic": "filings", "accession": "0000723125-25-000041"}, "depth": "both"}
    assert missing_keys("skill-requests", req) == set()


def test_8k_without_item_202_is_ignored():
    assert Router(WATCH).on_filing({**F, "items": ["8.01"]}) is None


def test_10q_triggers():
    assert Router(WATCH).on_filing({**F, "form": "10-Q", "items": []})["reason"] == "10-Q"


def test_ticker_not_on_watchlist_is_ignored():
    assert Router(WATCH).on_filing({**F, "ticker": "NVDA"}) is None


def test_one_request_per_ticker_per_run():
    r = Router(WATCH)
    assert r.on_filing(F)
    assert r.on_filing({**F, "form": "10-Q", "items": [], "accession": "acc-2"}) is None


def test_replaying_the_same_filing_in_a_new_run_triggers_again():
    r = Router(WATCH)
    r.on_filing(F)
    assert r.on_filing({**F, "run": "replay-2"})["request_id"] == "MU-0000723125-25-000041"


def test_research_is_anchored_to_the_filing_date_not_today():
    # a replay of an August filing must be priced with August information, not "latest" (October) data
    assert Router(WATCH).on_filing({**F, "ts": 1787775679000})["args"] == "MU earnings filed 2026-08-26"


def test_on_demand_research_bypasses_the_per_ticker_limit_but_not_the_same_filing():
    r = Router(WATCH)
    assert r.on_filing({**F, "run": "live"})
    other = {**F, "run": "live", "accession": "0000723125-25-000099", "on_demand": True}
    assert r.on_filing(other)["request_id"] == "MU-0000723125-25-000099"
    assert r.on_filing(other) is None


def test_research_now_can_be_pressed_again_after_a_minute():
    clock = iter([0, 30, 61]).__next__
    r = Router(WATCH, clock=clock)
    press = {**F, "run": "live", "on_demand": True}
    assert r.on_filing(press)
    assert r.on_filing(press) is None          # double-click
    assert r.on_filing(press)                  # a minute later: e.g. research.py was restarted and missed it


def test_research_now_carries_the_chosen_depth_and_each_depth_is_its_own_press():
    r = Router(WATCH, clock=iter([0, 1, 2]).__next__)
    press = {**F, "run": "live", "on_demand": True}
    assert r.on_filing({**press, "depth": "quick"})["depth"] == "quick"
    assert r.on_filing({**press, "depth": "deep"})["depth"] == "deep"      # not a repeat of the quick press
    assert r.on_filing({**press, "depth": "quick"}) is None                 # this one is
    assert Router(WATCH).on_filing(F)["depth"] == "both"                    # a real filing: quick, then deep
