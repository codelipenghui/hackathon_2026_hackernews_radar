import json
import subprocess

import pytest

from common import missing_keys
from research import Researcher, extract_json, validate_verdict

GOOD = {"verdict": "PASS", "score": 4.3, "masters": {"buffett": 4.4, "munger": 3.5, "duan": 3.7, "lilu": 4.0},
        "buy_low": 140.0, "buy_high": 160.0, "target": 195.0,
        "red_lines": ["gross margin below 40%"], "summary": "Record HBM revenue."}
REQ = {"run": "replay-1", "ts": 5, "request_id": "MU-acc", "ticker": "MU", "skill": "earnings-review",
       "args": "MU latest", "reason": "8-K Item 2.02", "trigger": {"topic": "filings", "accession": "acc"}}


def test_validate_accepts_the_spec_example():
    assert validate_verdict(GOOD) == GOOD


def test_validate_accepts_a_fail_without_a_band():
    v = {**GOOD, "verdict": "FAIL", "buy_low": None, "buy_high": None, "target": None}
    assert validate_verdict(v) == v


@pytest.mark.parametrize("patch", [
    {"verdict": "BUY"}, {"score": 7}, {"score": True}, {"masters": {"buffett": 4}},
    {"buy_low": 170.0}, {"buy_low": None}, {"target": "195"}, {"red_lines": "none"}, {"summary": None},
])
def test_validate_rejects_bad_verdicts(patch):
    with pytest.raises(ValueError):
        validate_verdict({**GOOD, **patch})


def test_extract_json_handles_prose_and_fences():
    assert extract_json('Here is the verdict:\n```json\n{"a": {"b": 1}}\n```\nDone.') == {"a": {"b": 1}}


def test_extract_json_rejects_text_without_an_object():
    with pytest.raises(ValueError):
        extract_json("I could not find a verdict.")


def make(tmp_path, run_skill, extract):
    return Researcher(run_skill, extract, data_dir=tmp_path / "data", cache_delay_s=0,
                      sleep=lambda s: None, clock=iter([100.0, 412.0]).__next__)


def write_cache(tmp_path):
    p = tmp_path / "data" / "verdicts" / "MU-acc.json"
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps({**GOOD, "report_path": "data/reports/MU-acc.md", "duration_s": 300}))


def boom(*_):
    raise AssertionError("must not be called")


def test_live_run_saves_report_and_cache(tmp_path):
    out = []
    make(tmp_path, lambda skill, args: "# report", lambda report: json.dumps(GOOD)).handle({**REQ, "run": "live"}, out.append)
    assert [m["status"] for m in out] == ["started", "done"]
    done = out[1]
    assert done["source"] == "live" and done["verdict"] == "PASS" and done["duration_s"] == 312 and done["ts"] == 5
    assert done["report_path"] == "data/reports/MU-acc.md"
    assert missing_keys("research-verdicts", done) == set()
    assert (tmp_path / "data" / "reports" / "MU-acc.md").read_text() == "# report"
    assert json.loads((tmp_path / "data" / "verdicts" / "MU-acc.json").read_text())["buy_low"] == 140.0


def test_replay_uses_the_cache_without_running_the_skill(tmp_path):
    write_cache(tmp_path)
    out = []
    make(tmp_path, boom, boom).handle(REQ, out.append)
    assert [m["status"] for m in out] == ["started", "done"]
    assert out[1]["source"] == "cache" and out[1]["run"] == "replay-1"


def test_live_failure_falls_back_to_the_cache(tmp_path):
    write_cache(tmp_path)

    def timeout(skill, args):
        raise subprocess.TimeoutExpired("claude", 600)
    out = []
    make(tmp_path, timeout, boom).handle({**REQ, "run": "live"}, out.append)
    assert out[1]["status"] == "done" and out[1]["source"] == "cache"


def test_failure_without_a_cache_emits_failed(tmp_path):
    out = []
    make(tmp_path, lambda s, a: "# report", lambda r: "no json here").handle({**REQ, "run": "live"}, out.append)
    assert out[1]["status"] == "failed" and "error" in out[1]
    assert missing_keys("research-verdicts", out[1]) == set()


def test_extraction_is_retried_once(tmp_path):
    answers = iter(["sorry, here it is in prose", json.dumps(GOOD)])
    out = []
    make(tmp_path, lambda s, a: "# report", lambda r: next(answers)).handle({**REQ, "run": "live"}, out.append)
    assert out[1]["status"] == "done"
