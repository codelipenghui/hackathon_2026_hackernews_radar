"""Run ai-berkshire research for each skill request and publish a structured verdict to `research-verdicts`.

Runs on a team laptop, not in Docker: it shells out to the Claude Code CLI, which needs your login, with the
ai-berkshire commands installed (its scripts/install-claude-commands.sh). A deep-research run takes minutes, so:
  - replays with a cached verdict (data/verdicts/<request_id>.json) wait CACHE_DELAY_S so the "researching..."
    card is visible, then emit the cache; the demo never depends on a live run
  - a live run that times out or fails falls back to the cache when there is one
The skill's free-text report (often in Chinese) is turned into JSON by a second, short Claude call.
"""
import json
import os
import time
from pathlib import Path

from common import LIVE

DATA = Path(__file__).with_name("data")
CACHE_DELAY_S = float(os.environ.get("CACHE_DELAY_S", 20))
VERDICTS = {"PASS", "GRAY", "FAIL"}
MASTERS = ("buffett", "munger", "duan", "lilu")
FIELDS = ("verdict", "score", "masters", "buy_low", "buy_high", "target", "red_lines", "summary")


def _num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def extract_json(text):
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        raise ValueError("no JSON object in model output")
    return json.loads(text[start:end + 1])  # JSONDecodeError is a ValueError


def validate_verdict(d):
    if not isinstance(d, dict):
        raise ValueError("verdict is not an object")
    if d.get("verdict") not in VERDICTS:
        raise ValueError(f"verdict must be one of {sorted(VERDICTS)}")
    if not _num(d.get("score")) or not 0 <= d["score"] <= 5:
        raise ValueError("score must be a number 0-5")
    masters = d.get("masters")
    if not isinstance(masters, dict) or set(masters) != set(MASTERS) or not all(_num(v) for v in masters.values()):
        raise ValueError(f"masters must score exactly {MASTERS}")
    low, high, target = d.get("buy_low"), d.get("buy_high"), d.get("target")
    if any(x is not None and not _num(x) for x in (low, high, target)):
        raise ValueError("buy_low, buy_high and target must be numbers or null")
    if (low is None) != (high is None) or (low is not None and low > high):
        raise ValueError("buy band must be both null or low <= high")
    if not isinstance(d.get("red_lines"), list) or not all(isinstance(x, str) for x in d["red_lines"]):
        raise ValueError("red_lines must be a list of strings")
    if not isinstance(d.get("summary"), str):
        raise ValueError("summary must be a string")
    return {k: d[k] for k in FIELDS}


class Researcher:
    def __init__(self, run_skill, extract, data_dir=DATA, cache_delay_s=CACHE_DELAY_S, sleep=time.sleep, clock=time.time):
        self.run_skill, self.extract = run_skill, extract
        self.data_dir, self.cache_delay_s, self.sleep, self.clock = Path(data_dir), cache_delay_s, sleep, clock

    def handle(self, req, out):
        base = {k: req[k] for k in ("run", "ts", "request_id", "ticker", "skill")}
        out({**base, "status": "started"})
        cache = self.data_dir / "verdicts" / f"{req['request_id']}.json"
        if req["run"] != LIVE and cache.exists():
            self.sleep(self.cache_delay_s)
            return out({**base, **json.loads(cache.read_text()), "status": "done", "source": "cache"})
        started = self.clock()
        try:
            report = self.run_skill(req["skill"], req["args"])
            verdict = self._verdict(report)
        except Exception as e:
            print(f"research {req['request_id']}: {e!r}")
            if cache.exists():
                return out({**base, **json.loads(cache.read_text()), "status": "done", "source": "cache"})
            return out({**base, "status": "failed", "error": repr(e)[:300]})
        report_path = self.data_dir / "reports" / f"{req['request_id']}.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report)
        saved = {**verdict, "report_path": str(report_path.relative_to(self.data_dir.parent)),
                 "duration_s": round(self.clock() - started)}
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(saved, indent=2, ensure_ascii=False))
        out({**base, **saved, "status": "done", "source": "live"})

    def _verdict(self, report):
        try:
            return validate_verdict(extract_json(self.extract(report)))
        except ValueError:  # models occasionally wrap or truncate JSON; one retry is cheap
            return validate_verdict(extract_json(self.extract(report)))
