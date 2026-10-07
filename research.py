"""Research for each skill request, published as verdicts on `research-verdicts` in two speeds.

Runs on a team laptop, not in Docker: it shells out to the Claude Code CLI (your login), with ai-berkshire's commands
installed in its checkout. Each verdict carries a `tier`:
  quick  one Claude session (Sonnet, web search only, a few minutes) scoring the filing like the four masters;
         tradeable right away
  deep   ai-berkshire /earnings-review in the background (10-30+ minutes); replaces the quick verdict when done
  news   a quick re-check after material news (see triage.py), updating the previous verdict
Replays use cached verdicts (data/verdicts/) so the demo never waits on a live run; a live run that fails falls back
to the cache when there is one. Deep reports (often in Chinese) become JSON through a second, short Claude call.
"""
import json
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common import LIVE, consume, emit, load_watchlist, producer

DATA = Path(__file__).with_name("data")
CACHE_DELAY_S = float(os.environ.get("CACHE_DELAY_S", 20))
VERDICTS = {"PASS", "GRAY", "FAIL"}
MASTERS = ("buffett", "munger", "duan", "lilu")
FIELDS = ("verdict", "score", "masters", "buy_low", "buy_high", "target", "red_lines", "summary")
DEPTHS = ("quick", "deep", "both")
# requests reach Claude (with tools) and name files on disk, so only the router's and triage's exact shapes get through
SHAPES = {
    "earnings-review": (re.compile(r"[A-Z.]{1,6} earnings filed \d{4}-\d{2}-\d{2}"),
                        re.compile(r"[A-Z.]{1,6}-\d{10}-\d{2}-\d{6}")),
    "news-recheck": (re.compile(r"[A-Z.]{1,6} 3d"), re.compile(r"[A-Z.]{1,6}-news-[0-9a-f]{8}")),
}


def _num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def extract_json(text):
    """The first JSON object in text; models wrap it in prose, fences, or stray {braces}."""
    decoder = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch == "{":
            try:
                obj, _ = decoder.raw_decode(text, i)
            except ValueError:
                continue
            if isinstance(obj, dict):
                return obj
    raise ValueError("no JSON object in model output")


def check_request(req):
    if req.get("skill") not in SHAPES:
        raise ValueError(f"skill not allowed: {req.get('skill')!r}")
    args, request_id = SHAPES[req["skill"]]
    if not args.fullmatch(str(req.get("args", ""))):
        raise ValueError(f"unexpected args: {str(req.get('args'))[:80]!r}")
    if not request_id.fullmatch(str(req.get("request_id", ""))):
        raise ValueError(f"unexpected request_id: {str(req.get('request_id'))[:80]!r}")
    if req["skill"] == "news-recheck":
        validate_verdict(req.get("prior"))
    if req.get("depth", "both") not in DEPTHS:
        raise ValueError(f"depth must be one of {DEPTHS}")


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
    return {k: d.get(k) for k in FIELDS}


VERDICT_KEYS = """Return ONLY a JSON object, no prose, with exactly these keys:
  "verdict": "PASS" | "GRAY" | "FAIL"
  "score": overall score, a number 0-5
  "masters": {"buffett": n, "munger": n, "duan": n, "lilu": n}  each 0-5
  "buy_low", "buy_high": a USD buy range, or null if you would not buy
  "target": the USD price at which you would take profit, or null
  "red_lines": short English strings, conditions that would break the thesis
  "summary": 2-3 English sentences"""

QUICK_PROMPT = """You are AI Berkshire's quick-verdict analyst. {ticker} published its earnings (SEC 8-K, accession
{accession}) on {date}. In a few minutes and at most 6 web searches, judge the results and the business as
Buffett (financials, cash, valuation), Munger (inversion: how could this fail?), Duan Yongping (business model,
culture) and Li Lu (10-year certainty) would. Price the buy range from the share price around {date}.
"""

NEWS_PROMPT = """You are AI Berkshire's news analyst for {ticker}. Stdin holds the current investment verdict as JSON.
Search the most important news about {ticker} from the last 3 days (at most 5 web searches) and decide whether it
changes the thesis. Keep every value the news does not change; say in the summary what changed and why.
"""


def quick_prompt(req):
    ticker, _, _, date = req["args"].split()
    return QUICK_PROMPT.format(ticker=ticker, date=date, accession=req["request_id"].split("-", 1)[1]) + VERDICT_KEYS


class Researcher:
    def __init__(self, quick, deep, extract, data_dir=DATA, cache_delay_s=CACHE_DELAY_S, sleep=time.sleep,
                 clock=time.time):
        self.quick, self.deep, self.extract = quick, deep, extract
        self.data_dir, self.cache_delay_s, self.sleep, self.clock = Path(data_dir), cache_delay_s, sleep, clock
        self.running, self.lock = set(), threading.Lock()

    def _claim(self, key):
        with self.lock:
            if key in self.running:
                return False
            self.running.add(key)
            return True

    def _release(self, key):
        with self.lock:
            self.running.discard(key)

    @staticmethod
    def _stamped(out):
        """Messages carry `at` (wall clock, ms) so the cockpit can tell when research was lost to a restart."""
        return lambda m: out({**m, "at": int(time.time() * 1000)})

    def _cache(self, req, tier):
        return self.data_dir / "verdicts" / f"{req['request_id']}{'.quick' if tier == 'quick' else ''}.json"

    def is_cached(self, req):
        """Replays with a cached verdict are served at once, never queued behind a live run."""
        try:
            check_request(req)
        except ValueError:
            return False
        return req["run"] != LIVE and any(self._cache(req, t).exists() for t in ("deep", "quick"))

    def handle(self, req, out, schedule_deep):
        """Answer a request with a quick (or news) verdict now; hand earnings to schedule_deep for the deep tier."""
        key = (req.get("run"), req.get("request_id"), "handle")
        if not self._claim(key):
            return  # the same request is already being answered (a repeated press)
        try:
            self._handle(req, self._stamped(out), schedule_deep)
        finally:
            self._release(key)

    def _handle(self, req, out, schedule_deep):
        base = {k: req.get(k) for k in ("run", "ts", "request_id", "ticker", "skill")}
        try:
            check_request(req)
        except ValueError as e:
            print(f"research: rejected request: {e}")
            return out({**base, "tier": "quick", "status": "failed", "error": f"rejected: {e}"})
        if req["skill"] == "news-recheck":
            return self._run_quick(req, {**base, "tier": "news"}, out, NEWS_PROMPT.format(ticker=req["ticker"]) + VERDICT_KEYS,
                                   json.dumps(req["prior"]))
        depth = req.get("depth", "both")
        if depth == "deep":  # "Deep research" pressed: straight to /earnings-review
            return schedule_deep(req)
        if req["run"] != LIVE:
            for tier in ("deep", "quick"):
                if self._cache(req, tier).exists():
                    self._emit_cache(req, {**base, "tier": tier}, out, delay=True)
                    if tier == "quick":
                        schedule_deep(req)
                    return
        self._run_quick(req, {**base, "tier": "quick"}, out, quick_prompt(req))
        if depth == "both":
            schedule_deep(req)

    def _emit_cache(self, req, base, out, delay=False):
        if delay:
            out({**base, "status": "started"})
            self.sleep(self.cache_delay_s)
        out({**base, **json.loads(self._cache(req, base["tier"]).read_text()), "status": "done", "source": "cache"})

    def _run_quick(self, req, base, out, prompt, stdin=None):
        out({**base, "status": "started"})
        if req["run"] != LIVE and self._cache(req, base["tier"]).exists():
            self.sleep(self.cache_delay_s)
            return self._emit_cache(req, base, out)
        started = self.clock()
        try:
            verdict = self._json(lambda: self.quick(prompt, stdin))
        except Exception as e:
            print(f"research {req['request_id']} {base['tier']}: {e!r}")
            return out({**base, "status": "failed", "error": repr(e)[:300]})
        self._save(req, base, out, {**verdict, "report_path": None, "duration_s": round(self.clock() - started)})

    def deep_job(self, req, out):
        """ai-berkshire /earnings-review; its verdict replaces the quick one."""
        key = (req.get("run"), req.get("request_id"), "deep")
        if not self._claim(key):
            return
        try:
            self._deep_job(req, self._stamped(out))
        finally:
            self._release(key)

    def _deep_job(self, req, out):
        base = {k: req.get(k) for k in ("run", "ts", "request_id", "ticker", "skill")} | {"tier": "deep"}
        out({**base, "status": "started"})
        started = self.clock()
        report_path = self.data_dir / "reports" / f"{req['request_id']}.md"
        try:
            report = self.deep(req["skill"], req["args"])
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(report)  # keep the long research even if extraction fails
            verdict = self._json(lambda: self.extract(report))
        except Exception as e:
            print(f"research {req['request_id']} deep: {e!r}")
            if self._cache(req, "deep").exists():
                return self._emit_cache(req, base, out)
            return out({**base, "status": "failed", "error": repr(e)[:300]})
        self._save(req, base, out, {**verdict, "report_path": str(report_path.relative_to(self.data_dir.parent)),
                                    "duration_s": round(self.clock() - started)})

    def _save(self, req, base, out, saved):
        cache = self._cache(req, base["tier"])
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(saved, indent=2, ensure_ascii=False))
        out({**base, **saved, "status": "done", "source": "live"})

    @staticmethod
    def _json(call):
        try:
            return validate_verdict(extract_json(call()))
        except ValueError:  # models occasionally wrap or truncate JSON; one retry is cheap
            return validate_verdict(extract_json(call()))


BERKSHIRE_DIR = Path(os.environ.get("BERKSHIRE_DIR", Path.home() / "ai-berkshire"))
QUICK_TIMEOUT_S = 300
DEEP_TIMEOUT_S = 45 * 60
QUICK_MODEL = os.environ.get("QUICK_MODEL", "sonnet")
DEEP_TOOLS = "WebSearch,WebFetch,Read,Write,Bash,Task,Agent"
QUICK_TOOLS = "WebSearch,WebFetch"
# without this, a headless run can end its turn while its research agents are still working (seen on NVDA)
DEEP_SUFFIX = ("(Non-interactive run: wait for every agent you start to finish, write the final report file, and end "
               "with the verdict. Do not end your turn while any research is still running.)")
EXTRACT_PROMPT = """The text on stdin is an investment research report (it may be in Chinese).
Map Pass/通过/准出 -> PASS, Gray zone/灰色地带 -> GRAY, Fail/不通过/打回 -> FAIL; take the buy range from the
aggressive or moderate strategy; take per-master scores from the report's per-master view.
""" + VERDICT_KEYS


def _claude(args, stdin=None, timeout=QUICK_TIMEOUT_S):
    p = subprocess.run(["claude", "-p", *args, "--output-format", "json"], input=stdin, cwd=BERKSHIRE_DIR,
                       capture_output=True, text=True, timeout=timeout, check=True)
    return json.loads(p.stdout)["result"]


def newest_report(root, since):
    """ai-berkshire saves reports in per-company folders (reports/英伟达/...md)."""
    found = [p for p in Path(root).rglob("*.md") if p.stat().st_mtime >= since]
    return max(found, key=lambda p: p.stat().st_mtime) if found else None


def run_quick_cli(prompt, stdin=None):
    return _claude([prompt, "--model", QUICK_MODEL, "--allowedTools", QUICK_TOOLS], stdin=stdin)


def run_deep_cli(skill, args):
    """Run an ai-berkshire command headless; return the report it saved, else its final answer."""
    started = time.time()
    answer = _claude([f"/{skill} {args} {DEEP_SUFFIX}", "--allowedTools", DEEP_TOOLS], timeout=DEEP_TIMEOUT_S)
    report = newest_report(BERKSHIRE_DIR / "reports", started)
    return report.read_text() if report else answer


def extract_cli(report):
    return _claude([EXTRACT_PROMPT], stdin=report[:150_000], timeout=180)


def researcher():
    return Researcher(run_quick_cli, run_deep_cli, extract_cli)


def warm(ticker, accession):
    """Cache quick and deep verdicts for a recorded demo filing before the demo (takes up to ~45 minutes)."""
    from record import filing_from_submissions, submissions
    from router import Router

    filing = filing_from_submissions(submissions(load_watchlist()[ticker]["cik"]), ticker, accession)
    req = {**Router(load_watchlist()).on_filing({**filing, "run": "warm"}), "run": LIVE}
    show = lambda m: print(json.dumps(m, indent=2, ensure_ascii=False))
    r = researcher()
    r.handle(req, show, lambda _: r.deep_job(req, show))


def main():
    prod, r = producer(), researcher()
    quick_pool = ThreadPoolExecutor(2)  # quick verdicts and news re-checks: a few minutes each
    deep_pool = ThreadPoolExecutor(1)   # one /earnings-review at a time: its report is found by mtime
    cache_pool = ThreadPoolExecutor(4)  # cache hits never wait behind live research

    def out(msg):
        emit(prod, "research-verdicts", msg["ticker"], msg)
        prod.flush()
        print(f"{msg['run']} {msg['request_id']} {msg['tier']}: {msg['status']} {msg.get('verdict') or ''} "
              f"{msg.get('source') or ''}")

    def guarded(fn, *args):
        try:
            fn(*args)
        except Exception as e:  # never let one request kill a worker silently
            print(f"research job: {e!r}")

    schedule_deep = lambda req: deep_pool.submit(guarded, r.deep_job, req, out)
    print(f"research: ai-berkshire at {BERKSHIRE_DIR}, waiting for skill-requests")
    for _, req in consume(["skill-requests"]):
        (cache_pool if r.is_cached(req) else quick_pool).submit(guarded, r.handle, req, out, schedule_deep)


if __name__ == "__main__":
    warm(*sys.argv[2:4]) if sys.argv[1:2] == ["warm"] else main()
