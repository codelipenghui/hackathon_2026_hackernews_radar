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
    assert extract_json('Here is the verdict:\n