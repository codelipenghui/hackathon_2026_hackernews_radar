import json

from replay import MAX_GAP_S, load, schedule

H = 3_600_000
LINES = [
    {"topic": "filings", "msg": {"run": "", "ts": 0, "ticker": "MU"}},
    {"topic": "ticks", "msg": {"run": "", "ts": 300_000, "ticker": "MU", "price": 1.0}},
    {"topic": "ticks", "msg": {"run": "", "ts": 600_000, "ticker": "MU", "price": 2.0}},
    {"topic": "ticks", "msg": {"run": "", "ts": 600_000 + 16 * H, "ticker": "MU", "price": 3.0}},
    {"topic": "news", "msg": {"run": "", "ts": 600_000 + 17 * H, "ticker": "MU", "headline": "x"}},
]


def test_sets_run_and_scales_gaps_by_speed():
    s = schedule(LINES, speed=300, run="replay-9")
    assert [round(gap, 3) for gap, _, _ in s[:3]] == [0, 1.0, 1.0]
    assert {msg["run"] for _, _, msg in s} == {"replay-9"}
    assert [topic for _, topic, _ in s] == ["filings", "ticks", "ticks", "ticks", "news"]


def test_overnight_gap_is_capped():
    assert schedule(LINES, speed=300, run="r")[3][0] == MAX_GAP_S


def test_last_tick_is_marked_even_when_news_follows():
    s = schedule(LINES, speed=300, run="r")
    assert s[3][2]["last"] is True
    assert "last" not in s[2][2] and "last" not in s[4][2]


def test_does_not_mutate_the_loaded_lines():
    schedule(LINES, speed=300, run="r")
    assert LINES[1]["msg"]["run"] == "" and "last" not in LINES[3]["msg"]


def test_load_reads_json_lines(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text("".join(json.dumps(line) + "\n" for line in LINES) + "\n")
    assert load(p) == LINES
