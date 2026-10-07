from dashboard import REPLAYS, format_id, parse_last_id, replay_path


def test_single_topic_id_is_the_bare_offset_like_the_old_radar():
    assert format_id({"hn-events": 42}, ["hn-events"]) == "42"
    assert parse_last_id("42", ["hn-events"]) == {"hn-events": 42}


def test_multi_topic_id_round_trips():
    topics = ["ticks", "fills"]
    assert parse_last_id(format_id({"ticks": 7, "fills": 3}, topics), topics) == {"ticks": 7, "fills": 3}


def test_unknown_topics_in_an_id_are_ignored():
    assert parse_last_id('{"ticks": 7, "gone": 1}', ["ticks", "fills"]) == {"ticks": 7}


def test_no_id_means_start_from_the_run():
    assert parse_last_id(None, ["ticks"]) == {}


def test_replay_path_rejects_traversal_and_unknown_files(tmp_path):
    assert replay_path("../common.py") is None
    assert replay_path("") is None
    assert replay_path("does-not-exist.jsonl") is None
    REPLAYS.mkdir(parents=True, exist_ok=True)
    probe = REPLAYS / "_probe.jsonl"
    probe.write_text("")
    try:
        assert replay_path("_probe.jsonl") == probe
    finally:
        probe.unlink()


def test_live_view_keeps_a_day_of_market_data_but_only_an_hour_of_hn():
    from dashboard import stream_start_ms
    now = 100 * 3_600_000
    assert stream_start_ms("live", "ticks", now) == now - 24 * 3_600_000
    assert stream_start_ms("live", "hn-events", now) == now - 3_600_000
    assert stream_start_ms("replay-1759840000", "ticks", now) == 1759840000000


def test_research_now_uses_the_latest_earnings_8k():
    from dashboard import latest_earnings_filing
    sub = {"cik": "1045810", "name": "NVIDIA CORP", "filings": {"recent": {
        "accessionNumber": ["0001045810-26-000090", "0001045810-26-000073", "0001045810-26-000041"],
        "form": ["4", "8-K", "8-K"], "items": ["", "2.02,9.01", "2.02,9.01"],
        "acceptanceDateTime": ["2026-09-01T10:00:00.000Z", "2026-08-26T20:21:00.000Z", "2026-05-27T20:20:00.000Z"]}}}
    f = latest_earnings_filing(sub, "NVDA")
    assert (f["run"], f["accession"], f["on_demand"]) == ("live", "0001045810-26-000073", True)
    assert f["depth"] == "quick" and latest_earnings_filing(sub, "NVDA", depth="deep")["depth"] == "deep"
    assert latest_earnings_filing({**sub, "filings": {"recent": {k: v[:1] for k, v in sub["filings"]["recent"].items()}}}, "NVDA") is None


def test_history_uses_the_right_bar_size_and_caches_for_a_minute():
    from datetime import datetime, timezone
    import pytest
    from dashboard import History
    calls = []

    def fetch(ticker, period, interval):
        calls.append((ticker, period, interval))
        return [(datetime(2026, 10, 6, 14, 0, tzinfo=timezone.utc), 236.5, 1000)]
    clock = iter([0, 30, 61]).__next__
    h = History(fetch, clock=clock)
    first = h.get("NVDA", "5d")
    assert calls == [("NVDA", "5d", "15m")] and first[0]["price"] == 236.5 and first[0]["ticker"] == "NVDA"
    assert h.get("NVDA", "5d") == first and len(calls) == 1      # cached
    h.get("NVDA", "5d")
    assert len(calls) == 2                                       # refreshed after 60 s
    assert History(fetch).get("NVDA", "1mo") and calls[-1] == ("NVDA", "1mo", "1d")
    with pytest.raises(ValueError):
        h.get("NVDA", "10y")
