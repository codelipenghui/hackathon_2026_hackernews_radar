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
