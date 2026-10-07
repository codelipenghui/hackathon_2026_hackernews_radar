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
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common import LIVE, consume, emit, load_watchlist, producer

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


BERKSHIRE_DIR = Path(os.environ.get("BERKSHIRE_DIR", Path.home() / "ai-berkshire"))
SKILL_TIMEOUT_S = 600
MAX_PARALLEL = 2
TOOLS = "WebSearch,WebFetch,Read,Write,Bash,Task,Agent"
EXTRACT_PROMPT = """The text on stdin is an investment research report (it may be in Chinese).
Return ONLY a JSON object, no prose, with exactly these keys:
  "verdict": "PASS" | "GRAY" | "FAIL"   (Pass/通过/准出 -> PASS, Gray zone/灰色地带 -> GRAY, Fail/不通过/打回 -> FAIL)
  "score": overall score, a number 0-5
  "masters": {"buffett": n, "munger": n, "duan": n, "lilu": n}  each 0-5, from the report's per-master view
  "buy_low", "buy_high": the USD buy range for the aggressive or moderate strategy, or null if none is given
  "target": the USD price at which the report would take profit or consider the stock fully valued, or null
  "red_lines": short English strings, conditions that would break the thesis
  "summary": 2-3 English sentences"""


def _claude(args, stdin=None, timeout=SKILL_TIMEOUT_S):
    p = subprocess.run(["claude", "-p", *args, "--output-format", "json"], input=stdin, cwd=BERKSHIRE_DIR,
                       capture_output=True, text=True, timeout=timeout, check=True)
    return json.loads(p.stdout)["result"]


def run_skill_cli(skill, args):
    """Run an ai-berkshire command headless; return the report it saved under reports/, else its final answer."""
    started = time.time()
    answer = _claude([f"/{skill} {args}", "--allowedTools", TOOLS])
    reports = [p for p in (BERKSHIRE_DIR / "reports").glob("*.md") if p.stat().st_mtime >= started]
    return max(reports, key=lambda p: p.stat().st_mtime).read_text() if reports else answer


def extract_cli(report):
    return _claude([EXTRACT_PROMPT], stdin=report[:150_000], timeout=180)


def warm(ticker, accession):
    """Run research for a recorded demo filing now and save the cache (data/verdicts), before the demo."""
    from record import filing_from_submissions, submissions
    from router import Router

    filing = filing_from_submissions(submissions(load_watchlist()[ticker]["cik"]), ticker, accession)
    req = Router(load_watchlist()).on_filing({**filing, "run": "warm"})
    if (DATA / "verdicts" / f"{req['request_id']}.json").exists():
        sys.exit(f"cache already exists for {req['request_id']}; delete it to re-run")
    Researcher(run_skill_cli, extract_cli).handle({**req, "run": LIVE}, lambda m: print(json.dumps(m, indent=2, ensure_ascii=False)))


def main():
    prod, researcher = producer(), Researcher(run_skill_cli, extract_cli)
    pool = ThreadPoolExecutor(MAX_PARALLEL)  # at most 2 deep-research runs at a time

    def out(msg):
        emit(prod, "research-verdicts", msg["ticker"], msg)
        prod.flush()
        print(f"{msg['run']} {msg['request_id']}: {msg['status']} {msg.get('verdict', '')} {msg.get('source', '')}")

    def job(req):
        try:
            researcher.handle(req, out)
        except Exception as e:  # never let one request kill the worker silently
            print(f"research job {req.get('request_id')}: {e!r}")

    print(f"research: ai-berkshire at {BERKSHIRE_DIR}, waiting for skill-requests")
    for _, req in consume(["skill-requests"]):
        pool.submit(job, req)


if __name__ == "__main__":
    warm(*sys.argv[2:4]) if sys.argv[1:2] == ["warm"] else main()
