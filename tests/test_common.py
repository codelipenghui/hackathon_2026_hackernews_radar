import json
from pathlib import Path

import pytest

from common import LIVE, REQUIRED, load_watchlist, match_tickers, missing_keys, run_start_ms

FIX = json.loads((Path(__file__).parent / "fixtures" / "contracts.json").read_text())
WATCH = {"MU": {"cik": "0000723125", "names": ["Micron"]},
         "META": {"cik": "0001326801", "names": ["Meta Platforms", "Meta"]}}


@pytest.mark.parametrize("topic", sorted(REQUIRED))
def test_fixture_examples_satisfy_contract(topic):
    assert FIX[topic], f"no example for {topic}"
    for msg in FIX[topic]:
        assert missing_keys(topic, msg) == set()


def test_done_verdict_requires_verdict_fields():
    started = {"run": LIVE, "ts": 1, "request_id": "r", "ticker": "MU", "status": "started", "skill": "earnings-review"}
    assert missing_keys("research-verdicts", started) == set()
    assert "buy_low" in missing_keys("research-verdicts", {**started, "status": "done"})


def test_run_start_for_replay_is_its_creation_second():
    assert run_start_ms("replay-1759840000") == 1759840000000


def test_run_start_for_live_is_one_hour_back():
    assert run_start_ms(LIVE, now=10_000_000) == 10_000_000 - 3_600_000


def test_match_tickers_is_whole_word_and_case_sensitive():
    assert match_tickers("Micron beats on HBM demand", WATCH) == ["MU"]
    assert match_tickers("A 5 micron process node", WATCH) == []
    assert match_tickers("Metadata is hard", WATCH) == []
    assert match_tickers("Meta Platforms and Micron team up", WATCH) == ["MU", "META"]


def test_repo_watchlist_is_well_formed():
    for ticker, info in load_watchlist().items():
        assert ticker.isupper()
        assert len(info["cik"]) == 10 and info["cik"].isdigit()
        assert info["names"]


def test_runs_started_in_the_same_second_get_distinct_ids(monkeypatch):
    import common
    monkeypatch.setattr(common.time, "time", lambda: 1759840000.4)
    first, second = common.new_replay_run(), common.new_replay_run()
    assert first == "replay-1759840000" and second == "replay-1759840000-2"
    assert run_start_ms(second) == 1759840000000
