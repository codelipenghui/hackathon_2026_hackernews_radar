# Alpha Radar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn HN Radar into a real-time trading pipeline: SEC earnings filings start ai-berkshire research, and a fast rules engine paper-trades inside the research verdict, all over Kafka, with a live cockpit dashboard.

**Architecture:** Small single-purpose Python services, each a Kafka consumer → pure logic → Kafka producer. The logic of every service is a pure class or function, unit-tested without Kafka; a thin `main()` wires it to Kafka. Every message carries `run` (`live` or `replay-<unix s>`) and `ts` (event time), so recorded earnings days replay through the same topics as live data.

**Tech Stack:** Python 3.11+ (3.13 in Docker), kafka-python 3.0.11, Apache Kafka 4.1 (KRaft, existing compose), pytest, yfinance (recording only, on a host), Claude Code CLI + ai-berkshire (research, on a host), vanilla JS + canvas (dashboard, like the existing `index.html`).

**Spec:** `docs/superpowers/specs/2026-10-07-alpha-radar-design.md` — read §3 (message contracts) before any task.

## Team tracks

Task 1 is done by everyone together in hour 1. After that the three tracks are independent until Task 13 (integration):

| Track | Tasks | Person |
|---|---|---|
| Shared | 1 | all |
| A — data sources & replay | 2, 3, 4, 5 | A |
| B — slow brain | 6, 7, 8 | B |
| C — fast brain & dashboard | 9, 10, 11, 12 | C |
| Integration | 13 | all, from 6:00 |

Each track builds against `tests/fixtures/contracts.json`, never against another track's code, until Task 13.

## Global Constraints

- Every message on every topic has `run` (string: `"live"` or `"replay-<unix seconds>"`) and `ts` (int, epoch **ms**, **event time**). Logic uses `ts`, never the wall clock.
- Topic names, keys and fields are exactly spec §3; `common.REQUIRED` is the machine-checked copy.
- All topics: 1 partition, created by `common.ensure_topics`.
- Prices are USD floats. Paper trading only. Starting cash **$100,000**.
- Trading constants: pivot = first **6** regular-session 5-min bars after the filing; volume ≥ **1.5×** the average of the previous **12** regular bars; ATR **14**; risk **1%** of equity per trade; stop **2N**; position ≤ **25%** of equity; slippage **5 bps**; target = verdict `target`, else `buy_high × 1.10`.
- Research: `claude -p` timeout **600 s**; at most **2** concurrent research jobs; replays with a cached verdict wait `CACHE_DELAY_S` (**20 s**) then emit `source: "cache"`.
- Replay: gaps between messages are `(Δts / speed)` seconds, capped at **5 s**; default speed **300**.
- SEC requests send `User-Agent` from env `SEC_UA`, and stay under 10 requests/s.
- Python must run on 3.11 (team laptops) and 3.13 (Docker): no 3.12-only syntax (e.g. no same-quote nesting inside f-strings).
- Code style matches the existing repo: flat layout, module docstring explaining the "why", short comments only where non-obvious.

## Review Focus

1. **Research slower than the breakout** — a verdict that arrives after the price already broke the pivot must still allow an entry on the *next* qualifying bar (and never a retroactive fill). → test in Task 9.
2. **Same earnings day replayed twice** — the second replay must trigger research again (served from cache) and trade on a fresh $100k book, without touching the first run. → tests in Task 6 and Task 10.
3. **Model output that is almost-JSON** — prose or ```` ```json ```` fences around the verdict must still parse; an inverted buy band or missing master score must be rejected, not traded. → tests in Task 7.
4. **Browser reconnect mid-replay** — EventSource reconnecting with a multi-topic `Last-Event-ID` must resume without gaps or duplicates, and the old radar page (single topic, numeric id) must keep working. → tests in Task 11.
5. **Overnight gaps and trailing news in a replay** — a 16-hour gap must not stall the demo (capped at 5 s), and the end-of-run marker must land on the last *tick* even if news lines come after it. → tests in Task 5.

---

### Task 1: Contracts, shared helpers, watchlist (everyone, hour 1)

**Files:**
- Create: `common.py`, `watchlist.json`, `tests/fixtures/contracts.json`, `tests/test_common.py`, `pytest.ini`, `requirements.txt`, `requirements-dev.txt`
- Modify: `producer.py:51-52` (tag messages with `run`), `Dockerfile` (install from requirements)

**Interfaces:**
- Produces (used by every later task):
  - `common.KAFKA: str`, `common.LIVE = "live"`, `common.HOUR_MS = 3_600_000`
  - `common.REQUIRED: dict[str, set[str]]`, `common.missing_keys(topic: str, msg: dict) -> set[str]`
  - `common.now_ms() -> int`, `common.new_replay_run() -> str`, `common.run_start_ms(run: str, now: int | None = None) -> int`
  - `common.load_watchlist(path=WATCHLIST) -> dict[str, dict]` — `{"MU": {"cik": "0000723125", "names": ["Micron"], "aliases": []}}`
  - `common.match_tickers(text: str, watchlist: dict) -> list[str]` (whole word, case-sensitive, watchlist order)
  - `common.ensure_topics(topics: list[str]) -> None`, `common.producer() -> KafkaProducer`, `common.emit(prod, topic: str, key, msg: dict) -> None`
  - `common.consume(topics: list[str], start_ms: int | None = None)` — generator of `(topic: str, msg: dict)`; `None` = from now on

- [ ] **Step 1: Dev setup**

Create `requirements.txt`:
```
kafka-python==3.0.11
```
Create `requirements-dev.txt`:
```
-r requirements.txt
pytest>=8
yfinance>=0.2.40
```
Create `pytest.ini`:
```ini
[pytest]
pythonpath = .
testpaths = tests
```
Run: `python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt`
Add `.venv/` and `__pycache__/` to `.gitignore` (create it).

- [ ] **Step 2: Write the contract fixtures** — the examples from spec §3 with real values. Every track builds against these.

Create `tests/fixtures/contracts.json`:
```json
{
  "filings": [
    {"run": "replay-1759840000", "ts": 1758657912000, "ticker": "MU", "cik": "0000723125", "form": "8-K",
     "items": ["2.02", "9.01"], "accession": "0000723125-25-000041",
     "url": "https://www.sec.gov/Archives/edgar/data/723125/000072312525000041/",
     "title": "MICRON TECHNOLOGY INC - 8-K"}
  ],
  "ticks": [
    {"run": "replay-1759840000", "ts": 1758658200000, "ticker": "MU", "price": 157.42, "volume": 182300, "session": "post"},
    {"run": "replay-1759840000", "ts": 1758720600000, "ticker": "MU", "price": 161.2, "volume": 950000, "session": "regular", "last": true}
  ],
  "news": [
    {"run": "replay-1759840000", "ts": 1758657960000, "ticker": "MU", "source": "globenewswire",
     "headline": "Micron Technology, Inc. Reports Results for the Fourth Quarter and Full Year of Fiscal 2025",
     "url": "https://investors.micron.com/news-releases"}
  ],
  "skill-requests": [
    {"run": "replay-1759840000", "ts": 1758657912000, "request_id": "MU-0000723125-25-000041", "ticker": "MU",
     "skill": "earnings-review", "args": "MU latest", "reason": "8-K Item 2.02",
     "trigger": {"topic": "filings", "accession": "0000723125-25-000041"}}
  ],
  "research-verdicts": [
    {"run": "replay-1759840000", "ts": 1758657912000, "request_id": "MU-0000723125-25-000041", "ticker": "MU",
     "status": "started", "skill": "earnings-review"},
    {"run": "replay-1759840000", "ts": 1758657912000, "request_id": "MU-0000723125-25-000041", "ticker": "MU",
     "status": "done", "source": "cache", "skill": "earnings-review", "verdict": "PASS", "score": 4.3,
     "masters": {"buffett": 4.4, "munger": 3.5, "duan": 3.7, "lilu": 4.0},
     "buy_low": 140.0, "buy_high": 160.0, "target": 195.0,
     "red_lines": ["gross margin below 40%", "HBM share loss to Samsung"],
     "summary": "Record HBM revenue; guidance above consensus.",
     "report_path": "data/reports/MU-0000723125-25-000041.md", "duration_s": 312}
  ],
  "orders": [
    {"run": "replay-1759840000", "ts": 1758720600000, "order_id": "o-17", "ticker": "MU", "side": "BUY", "qty": 120,
     "rule": "livermore_pivot", "reason": "broke 30-min pivot 158.10 on 1.8x volume; inside band 140-160",
     "request_id": "MU-0000723125-25-000041"}
  ],
  "fills": [
    {"run": "replay-1759840000", "ts": 1758720600000, "order_id": "o-17", "ticker": "MU", "side": "BUY", "qty": 120, "price": 158.25}
  ],
  "pnl": [
    {"run": "replay-1759840000", "ts": 1758720600000, "cash": 81010.0, "equity": 100412.5, "realized": 0.0,
     "unrealized": 412.5, "positions": {"MU": {"qty": 120, "avg": 158.25, "last": 161.69, "stop": 151.9}}}
  ]
}
```

- [ ] **Step 3: Write the failing tests**

Create `tests/test_common.py`:
```python
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
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `pytest tests/test_common.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'common'`

- [ ] **Step 5: Implement `common.py`**

```python
"""Shared plumbing for every Alpha Radar service: Kafka helpers, run ids, the watchlist, message contracts.

Every message on every topic carries `run` ("live", or "replay-<unix seconds>" for a recorded earnings day played
back into Kafka) and `ts` (event time, epoch ms). Services key their state by run and reason only about ts, so a
replay at 300x behaves exactly like the day it recorded, and never mixes with live data.
"""
import json
import os
import re
import time
from pathlib import Path

KAFKA = os.environ.get("KAFKA", "localhost:9092")
LIVE = "live"
HOUR_MS = 60 * 60 * 1000
WATCHLIST = Path(__file__).with_name("watchlist.json")

# required keys per topic (spec §3); tests check fixtures and service output against these
REQUIRED = {
    "filings": {"run", "ts", "ticker", "cik", "form", "items", "accession", "url", "title"},
    "ticks": {"run", "ts", "ticker", "price", "volume", "session"},
    "news": {"run", "ts", "ticker", "source", "headline", "url"},
    "skill-requests": {"run", "ts", "request_id", "ticker", "skill", "args", "reason", "trigger"},
    "research-verdicts": {"run", "ts", "request_id", "ticker", "status", "skill"},
    "orders": {"run", "ts", "order_id", "ticker", "side", "qty", "rule", "reason", "request_id"},
    "fills": {"run", "ts", "order_id", "ticker", "side", "qty", "price"},
    "pnl": {"run", "ts", "cash", "equity", "realized", "unrealized", "positions"},
}
VERDICT_DONE = {"source", "verdict", "score", "masters", "buy_low", "buy_high", "target",
                "red_lines", "summary", "report_path", "duration_s"}


def missing_keys(topic, msg):
    need = set(REQUIRED[topic])
    if topic == "research-verdicts" and msg.get("status") == "done":
        need |= VERDICT_DONE
    return need - msg.keys()


def now_ms():
    return int(time.time() * 1000)


def new_replay_run():
    return f"replay-{int(time.time())}"


def run_start_ms(run, now=None):
    """Where a run begins in Kafka (record timestamps are produce time): a replay starts at the second in its id,
    live shows the last hour."""
    if run.startswith("replay-"):
        return int(run.split("-", 1)[1]) * 1000
    return (now_ms() if now is None else now) - HOUR_MS


def load_watchlist(path=WATCHLIST):
    return json.loads(Path(path).read_text())


def match_tickers(text, watchlist):
    """Watchlist tickers whose company name or alias appears in text as a whole word. Case-sensitive, so
    "Micron" matches but "5 micron process" does not."""
    return [ticker for ticker, info in watchlist.items()
            if any(re.search(rf"\b{re.escape(n)}\b", text) for n in info["names"] + info.get("aliases", []))]


def ensure_topics(topics):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=KAFKA)
    try:
        new = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t not in set(admin.list_topics())]
        if new:
            try:
                admin.create_topics(new)
            except TopicAlreadyExistsError:
                pass  # another service created it first
    finally:
        admin.close()


def producer():
    from kafka import KafkaProducer

    return KafkaProducer(bootstrap_servers=KAFKA, key_serializer=str.encode,
                         value_serializer=lambda v: json.dumps(v).encode(), linger_ms=20)


def emit(prod, topic, key, msg):
    prod.send(topic, key=str(key), value=msg)


def consume(topics, start_ms=None):
    """Yield (topic, msg) from partition 0 of each topic, starting at start_ms (produce time), or from now on if None."""
    from kafka import KafkaConsumer, TopicPartition

    ensure_topics(topics)
    consumer = KafkaConsumer(bootstrap_servers=KAFKA, value_deserializer=json.loads)
    tps = [TopicPartition(t, 0) for t in topics]
    consumer.assign(tps)
    if start_ms is None:
        consumer.seek_to_end(*tps)
    else:
        found = consumer.offsets_for_times({tp: start_ms for tp in tps})
        for tp in tps:
            consumer.seek(tp, found[tp].offset) if found[tp] else consumer.seek_to_end(tp)
    while True:
        for tp, records in consumer.poll(timeout_ms=1000).items():
            for r in records:
                yield tp.topic, r.value
```

- [ ] **Step 6: Create `watchlist.json`** (US AI names from ai-berkshire's `data/watchlist.json`; TSM is left out because foreign issuers file 6-K, not 8-K)

```json
{
  "MU":   {"cik": "0000723125", "names": ["Micron"], "aliases": []},
  "NVDA": {"cik": "0001045810", "names": ["NVIDIA", "Nvidia"], "aliases": []},
  "AMD":  {"cik": "0000002488", "names": ["AMD", "Advanced Micro Devices"], "aliases": []},
  "AVGO": {"cik": "0001730168", "names": ["Broadcom"], "aliases": []},
  "MRVL": {"cik": "0001835632", "names": ["Marvell"], "aliases": []},
  "ORCL": {"cik": "0001341439", "names": ["Oracle"], "aliases": []},
  "GOOG": {"cik": "0001652044", "names": ["Alphabet", "Google"], "aliases": []},
  "META": {"cik": "0001326801", "names": ["Meta Platforms"], "aliases": []},
  "MSFT": {"cik": "0000789019", "names": ["Microsoft"], "aliases": []},
  "AMZN": {"cik": "0001018724", "names": ["Amazon"], "aliases": []},
  "CRM":  {"cik": "0001108524", "names": ["Salesforce"], "aliases": []},
  "NOW":  {"cik": "0001373715", "names": ["ServiceNow"], "aliases": []},
  "PLTR": {"cik": "0001321655", "names": ["Palantir"], "aliases": []}
}
```
Verify the CIKs against SEC's official map (replace `you@team.dev` with the team email):
```bash
curl -s -A "AlphaRadar hackathon you@team.dev" https://www.sec.gov/files/company_tickers.json | python3 -c '
import json,sys; sec={v["ticker"]: str(v["cik_str"]).zfill(10) for v in json.load(sys.stdin).values()}
w=json.load(open("watchlist.json")); bad={t: (i["cik"], sec.get(t)) for t,i in w.items() if sec.get(t)!=i["cik"]}
print("all CIKs match" if not bad else bad)'
```
Expected: `all CIKs match` (if not, copy the SEC value into `watchlist.json`).

- [ ] **Step 7: Tag live HN messages with `run`** — in `producer.py`, change `emit`:
```python
def emit(kind, key, **data):
    producer.send(TOPIC, key=str(key), value={"kind": kind, "run": "live", "ts": int(time.time() * 1000), **data})
```

- [ ] **Step 8: Docker installs from requirements** — replace `Dockerfile` with:
```dockerfile
FROM python:3.13-slim
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY . .
```
Create `.dockerignore`:
```
.git
.venv
tests
docs
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `pytest tests/test_common.py -v`
Expected: all PASS

- [ ] **Step 10: Commit**
```bash
git add common.py watchlist.json tests pytest.ini requirements*.txt producer.py Dockerfile .dockerignore .gitignore
git commit -m "feat: shared contracts, Kafka helpers and watchlist for Alpha Radar"
```

---

### Task 2: `edgar.py` — live SEC filings poller (Person A)

**Files:**
- Create: `edgar.py`, `tests/test_edgar.py`

**Interfaces:**
- Consumes: `common.LIVE`, `common.emit`, `common.load_watchlist`, `common.producer`
- Produces: `edgar.parse_atom(xml_bytes: bytes, by_cik: dict[str, str]) -> list[dict]` (filings messages); topic `filings`

- [ ] **Step 1: Write the failing test**

Create `tests/test_edgar.py`:
```python
from datetime import datetime

from common import missing_keys
from edgar import parse_atom

FEED = b"""<?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
<entry>
<title>8-K - MICRON TECHNOLOGY INC (0000723125) (Filer)</title>
<link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/723125/000072312525000041/0000723125-25-000041-index.htm"/>
<summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; 2025-09-23 &lt;b&gt;AccNo:&lt;/b&gt; 0000723125-25-000041 &lt;b&gt;Size:&lt;/b&gt; 1 MB&lt;br&gt;Item 2.02: Results of Operations and Financial Condition&lt;br&gt;Item 9.01: Financial Statements and Exhibits</summary>
<updated>2025-09-23T16:05:12-04:00</updated>
</entry>
<entry>
<title>8-K - SOME OTHER CORP (0000999999) (Filer)</title>
<link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/999999/000099999925000001/0000999999-25-000001-index.htm"/>
<summary type="html"> &lt;b&gt;AccNo:&lt;/b&gt; 0000999999-25-000001 &lt;br&gt;Item 2.02: Results</summary>
<updated>2025-09-23T16:06:00-04:00</updated>
</entry>
</feed>"""


def test_parses_watchlist_filing():
    [f] = parse_atom(FEED, {"0000723125": "MU"})
    assert f == {
        "run": "live",
        "ts": int(datetime.fromisoformat("2025-09-23T16:05:12-04:00").timestamp() * 1000),
        "ticker": "MU", "cik": "0000723125", "form": "8-K", "items": ["2.02", "9.01"],
        "accession": "0000723125-25-000041",
        "url": "https://www.sec.gov/Archives/edgar/data/723125/000072312525000041/",
        "title": "MICRON TECHNOLOGY INC - 8-K",
    }
    assert missing_keys("filings", f) == set()


def test_ignores_companies_not_on_watchlist():
    assert parse_atom(FEED, {"0001045810": "NVDA"}) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_edgar.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'edgar'`

- [ ] **Step 3: Implement `edgar.py`**

```python
"""Poll SEC EDGAR's latest-filings feed and publish watchlist filings (8-K, 10-Q, 10-K) to topic `filings`.

EDGAR has no push API; its "current events" Atom feed lists the newest filings of a form type within minutes of
acceptance. The entry summary lists 8-K item numbers: Item 2.02 ("Results of Operations") is an earnings release,
which is what the router turns into an ai-berkshire research run.
"""
import os
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime

from common import LIVE, emit, load_watchlist, producer

A = "{http://www.w3.org/2005/Atom}"
FEED = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&count=100&output=atom"
FORMS = ("8-K", "10-Q", "10-K")
UA = os.environ.get("SEC_UA", "AlphaRadar hackathon contact@example.com")  # SEC rejects requests without a contact
POLL_S = 60


def parse_atom(xml_bytes, by_cik):
    """Filings messages for entries whose CIK is in by_cik ({cik: ticker})."""
    out = []
    for e in ET.fromstring(xml_bytes).iter(f"{A}entry"):
        m = re.match(r"(\S+) - (.+?) \((\d{10})\)", e.findtext(f"{A}title", ""))
        if not m or m[3] not in by_cik:
            continue
        form, name, cik = m.groups()
        summary = e.findtext(f"{A}summary", "")
        acc = re.search(r"AccNo:</b>\s*([\d-]+)", summary)
        if not acc:
            continue
        href = e.find(f"{A}link").get("href")
        out.append({
            "run": LIVE,
            "ts": int(datetime.fromisoformat(e.findtext(f"{A}updated")).timestamp() * 1000),
            "ticker": by_cik[cik], "cik": cik, "form": form,
            "items": re.findall(r"Item (\d+\.\d+)", summary),
            "accession": acc[1],
            "url": href.rsplit("/", 1)[0] + "/",
            "title": f"{name} - {form}",
        })
    return out


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read()


def main():
    by_cik = {info["cik"]: ticker for ticker, info in load_watchlist().items()}
    prod, seen, baseline = producer(), set(), True
    while True:
        for form in FORMS:
            try:
                for f in parse_atom(fetch(FEED.format(form=form)), by_cik):
                    if f["accession"] in seen:
                        continue
                    seen.add(f["accession"])
                    if not baseline:  # the first poll only records what is already out (like producer.py's maxitem)
                        emit(prod, "filings", f["ticker"], f)
                        print(f"filing: {f['ticker']} {f['form']} items={f['items']} {f['accession']}")
            except Exception as e:
                print(f"edgar {form}: {e!r}")
            time.sleep(1)
        baseline = False
        prod.flush()
        time.sleep(POLL_S)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_edgar.py -v`
Expected: PASS

- [ ] **Step 5: Smoke-test against the real feed** (no Kafka needed)

Run:
```bash
SEC_UA="AlphaRadar hackathon you@team.dev" python3 -c '
import edgar, common
by_cik = {i["cik"]: t for t, i in common.load_watchlist().items()}
print(len(edgar.parse_atom(edgar.fetch(edgar.FEED.format(form="8-K")), by_cik)), "watchlist 8-Ks in the latest 100")'
```
Expected: prints a number (often 0, which is fine). An HTTP 403 means the `User-Agent` is missing a contact.

- [ ] **Step 6: Commit**
```bash
git add edgar.py tests/test_edgar.py
git commit -m "feat: EDGAR latest-filings poller publishing watchlist filings"
```

---

### Task 3: `news.py` — newswire RSS poller (Person A)

**Files:**
- Create: `news.py`, `tests/test_news.py`

**Interfaces:**
- Consumes: `common.LIVE`, `common.emit`, `common.load_watchlist`, `common.match_tickers`, `common.now_ms`, `common.producer`
- Produces: `news.parse_rss(xml_bytes: bytes, source: str, watchlist: dict) -> list[dict]` (news messages); topic `news`

- [ ] **Step 1: Write the failing test**

Create `tests/test_news.py`:
```python
from datetime import datetime, timezone

from common import missing_keys
from news import parse_rss

WATCH = {"MU": {"cik": "0000723125", "names": ["Micron"]}}
RSS = b"""<?xml version="1.0"?><rss version="2.0"><channel>
<item><title>Micron Technology, Inc. Reports Results for the Fourth Quarter</title>
<link>https://example.com/mu-q4</link><pubDate>Tue, 23 Sep 2025 20:01:00 GMT</pubDate></item>
<item><title>Acme Corp Declares Quarterly Dividend</title>
<link>https://example.com/acme</link><pubDate>Tue, 23 Sep 2025 20:02:00 GMT</pubDate></item>
</channel></rss>"""


def test_matched_and_unmatched_headlines():
    mu, other = parse_rss(RSS, "globenewswire", WATCH)
    assert mu == {"run": "live", "ts": int(datetime(2025, 9, 23, 20, 1, tzinfo=timezone.utc).timestamp() * 1000),
                  "ticker": "MU", "source": "globenewswire",
                  "headline": "Micron Technology, Inc. Reports Results for the Fourth Quarter",
                  "url": "https://example.com/mu-q4"}
    assert other["ticker"] == "_"
    assert missing_keys("news", mu) == set() and missing_keys("news", other) == set()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_news.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'news'`

- [ ] **Step 3: Implement `news.py`**

```python
"""Poll press-release newswires (RSS) and publish headlines to topic `news`, tagged with watchlist tickers.

Companies often put earnings out on a newswire minutes before the 8-K lands on EDGAR. In the 1-day build news is
display-only: the cockpit shows it next to HN mentions; trading acts on prices and verdicts.
"""
import time
import urllib.request
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

from common import LIVE, emit, load_watchlist, match_tickers, now_ms, producer

# if a feed 404s on the day, swap the URL; news is display-only so the demo does not depend on it
FEEDS = {
    "prnewswire": "https://www.prnewswire.com/rss/news-releases-list.rss",
    "globenewswire": "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
}
POLL_S = 60


def parse_rss(xml_bytes, source, watchlist):
    """One news message per (headline, matched ticker); unmatched headlines get ticker "_"."""
    out = []
    for item in ET.fromstring(xml_bytes).iter("item"):
        title = (item.findtext("title") or "").strip()
        pub = item.findtext("pubDate")
        ts = int(parsedate_to_datetime(pub).timestamp() * 1000) if pub else now_ms()
        for ticker in match_tickers(title, watchlist) or ["_"]:
            out.append({"run": LIVE, "ts": ts, "ticker": ticker, "source": source,
                        "headline": title, "url": (item.findtext("link") or "").strip()})
    return out


def main():
    watchlist, prod, seen = load_watchlist(), producer(), set()
    while True:
        for source, url in FEEDS.items():
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "AlphaRadar hackathon"})
                with urllib.request.urlopen(req, timeout=20) as resp:
                    for n in parse_rss(resp.read(), source, watchlist):
                        if (n["url"], n["ticker"]) not in seen:
                            seen.add((n["url"], n["ticker"]))
                            emit(prod, "news", n["ticker"], n)
            except Exception as e:
                print(f"news {source}: {e!r}")
        prod.flush()
        time.sleep(POLL_S)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_news.py -v`
Expected: PASS

- [ ] **Step 5: Smoke-test the feeds**

Run: `python3 -c 'import news,urllib.request,common; w=common.load_watchlist(); [print(s, len(news.parse_rss(urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"x"}), timeout=20).read(), s, w))) for s,u in news.FEEDS.items()]'`
Expected: each source prints a count > 0. If one errors, replace its URL with a working press-release RSS feed.

- [ ] **Step 6: Commit**
```bash
git add news.py tests/test_news.py
git commit -m "feat: newswire RSS poller publishing ticker-tagged headlines"
```

---

### Task 4: `record.py` — record a real earnings day (Person A)

**Files:**
- Create: `record.py`, `tests/test_record.py`, `data/replays/` (recorded files, committed in Step 6)

**Interfaces:**
- Consumes: `common.load_watchlist`, `common.HOUR_MS`, `edgar.UA`, `edgar.fetch`
- Produces:
  - `record.filing_from_submissions(sub: dict, ticker: str, accession: str) -> dict` (filings message, `run: ""`)
  - `record.list_earnings(sub: dict, since_ms: int) -> list[tuple[str, int]]` — `(accession, ts)` of 8-K Item 2.02 filings
  - `record.ticks_from_bars(bars: list[tuple[datetime, float, float]], ticker: str) -> list[dict]` (ticks, `run: ""`)
  - `record.select_window(ticks: list[dict], filing_ts: int) -> list[dict]`
  - Replay file format (used by Task 5): JSON Lines, each `{"topic": <topic>, "msg": <message with "run": "">}`, sorted by `msg.ts`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_record.py`:
```python
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from record import filing_from_submissions, list_earnings, select_window, ticks_from_bars

NY = ZoneInfo("America/New_York")
SUB = {"cik": "723125", "name": "MICRON TECHNOLOGY INC", "filings": {"recent": {
    "accessionNumber": ["0000723125-25-000039", "0000723125-25-000041"],
    "form": ["4", "8-K"],
    "items": ["", "2.02,9.01"],
    "acceptanceDateTime": ["2025-09-20T14:00:00.000Z", "2025-09-23T20:05:12.000Z"],
}}}
FILED = int(datetime(2025, 9, 23, 20, 5, 12, tzinfo=timezone.utc).timestamp() * 1000)


def test_filing_from_submissions():
    f = filing_from_submissions(SUB, "MU", "0000723125-25-000041")
    assert f == {"run": "", "ts": FILED, "ticker": "MU", "cik": "0000723125", "form": "8-K",
                 "items": ["2.02", "9.01"], "accession": "0000723125-25-000041",
                 "url": "https://www.sec.gov/Archives/edgar/data/723125/000072312525000041/",
                 "title": "MICRON TECHNOLOGY INC - 8-K"}


def test_list_earnings_only_returns_item_202_8ks():
    assert list_earnings(SUB, since_ms=0) == [("0000723125-25-000041", FILED)]
    assert list_earnings(SUB, since_ms=FILED + 1) == []


def t(day, hh, mm):
    return datetime(2025, 9, day, hh, mm, tzinfo=NY)


def test_ticks_from_bars_assigns_sessions():
    bars = [(t(24, 9, 25), 150.0, 10), (t(24, 9, 30), 151.0, 20), (t(24, 15, 55), 152.0, 30), (t(24, 16, 0), 153.0, 40)]
    ticks = ticks_from_bars(bars, "MU")
    assert [x["session"] for x in ticks] == ["pre", "regular", "regular", "post"]
    assert ticks[1] == {"run": "", "ts": int(t(24, 9, 30).timestamp() * 1000), "ticker": "MU",
                        "price": 151.0, "volume": 20, "session": "regular"}


def test_select_window_keeps_filing_window_and_next_session_from_8am():
    filing_ts = int(t(23, 16, 5).timestamp() * 1000)
    times = [t(23, 15, 0), t(23, 16, 0), t(23, 19, 0), t(24, 7, 0), t(24, 8, 0), t(24, 9, 30), t(24, 16, 0)]
    ticks = ticks_from_bars([(x, 100.0, 1) for x in times], "MU")
    kept = [datetime.fromtimestamp(x["ts"] / 1000, NY).strftime("%d %H:%M") for x in select_window(ticks, filing_ts)]
    assert kept == ["23 16:00", "24 08:00", "24 09:30"]


def test_select_window_needs_a_regular_session_after_the_filing():
    ticks = ticks_from_bars([(t(23, 16, 0), 100.0, 1)], "MU")
    with pytest.raises(ValueError):
        select_window(ticks, int(t(23, 16, 5).timestamp() * 1000))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_record.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'record'`

- [ ] **Step 3: Implement `record.py`**

```python
"""Record one real earnings day into a replay file, so the demo never depends on live markets or APIs.

  python record.py --list                      recent earnings 8-Ks (Item 2.02) for the watchlist, with the move
  python record.py MU 0000723125-25-000041 [--news news.json]

Writes data/replays/<TICKER>-<date>.jsonl: the filing (from SEC submissions), 5-minute price bars (yfinance, which
keeps ~60 days of them) trimmed to the filing window and the next regular session, and optional hand-picked news.
Each line is {"topic": ..., "msg": ...} with "run" left blank; replay.py assigns the run.
"""
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from common import HOUR_MS, load_watchlist
from edgar import fetch

NY = ZoneInfo("America/New_York")
OUT = Path(__file__).with_name("data") / "replays"
SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"


def _ms(iso):
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def _et(ts):
    return datetime.fromtimestamp(ts / 1000, NY)


def filing_from_submissions(sub, ticker, accession):
    r = sub["filings"]["recent"]
    i = r["accessionNumber"].index(accession)
    cik = str(sub["cik"]).zfill(10)
    return {"run": "", "ts": _ms(r["acceptanceDateTime"][i]), "ticker": ticker, "cik": cik, "form": r["form"][i],
            "items": [x for x in r["items"][i].split(",") if x], "accession": accession,
            "url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/",
            "title": f"{sub['name']} - {r['form'][i]}"}


def list_earnings(sub, since_ms):
    r = sub["filings"]["recent"]
    return [(acc, _ms(at)) for acc, form, items, at in
            zip(r["accessionNumber"], r["form"], r["items"], r["acceptanceDateTime"])
            if form == "8-K" and "2.02" in items.split(",") and _ms(at) >= since_ms]


def ticks_from_bars(bars, ticker):
    """bars: [(tz-aware bar start, close, volume)] -> ticks messages with the US session of each bar."""
    out = []
    for when, close, volume in bars:
        et = when.astimezone(NY)
        minute = et.hour * 60 + et.minute
        session = "pre" if minute < 9 * 60 + 30 else "regular" if minute < 16 * 60 else "post"
        out.append({"run": "", "ts": int(when.timestamp() * 1000), "ticker": ticker,
                    "price": round(float(close), 2), "volume": int(volume), "session": session})
    return out


def select_window(ticks, filing_ts):
    """Keep 30 min before to 2 h after the filing, plus 08:00-16:00 ET of the first regular session after it."""
    after = [x for x in ticks if x["session"] == "regular" and x["ts"] > filing_ts]
    if not after:
        raise ValueError("no regular session after the filing in the fetched bars")
    day = _et(after[0]["ts"]).date()

    def keep(x):
        et = _et(x["ts"])
        return (filing_ts - HOUR_MS // 2 <= x["ts"] <= filing_ts + 2 * HOUR_MS
                or (et.date() == day and 8 <= et.hour < 16))
    return [x for x in ticks if keep(x)]


def fetch_bars(ticker, start, end, interval="5m"):
    import yfinance as yf

    df = yf.Ticker(ticker).history(start=start, end=end, interval=interval, prepost=True)
    return [(when.to_pydatetime(), row.Close, row.Volume) for when, row in df.iterrows()]


def submissions(cik):
    return json.loads(fetch(SUBMISSIONS.format(cik=cik)))


def list_main():
    since = int((datetime.now(NY) - timedelta(days=58)).timestamp() * 1000)  # 5-minute bars only go back ~60 days
    for ticker, info in load_watchlist().items():
        for acc, ts in list_earnings(submissions(info["cik"]), since):
            day = _et(ts).date()
            daily = fetch_bars(ticker, day - timedelta(days=5), day + timedelta(days=5), interval="1d")
            before = [c for when, c, _ in daily if when.date() <= day]
            after = [c for when, c, _ in daily if when.date() > day]
            move = f"{(after[0] / before[-1] - 1) * 100:+.1f}%" if before and after else "n/a"
            print(f"{ticker:5} {acc}  filed {_et(ts):%Y-%m-%d %H:%M} ET  next-day move {move}")


def record_main(ticker, accession, news_file):
    info = load_watchlist()[ticker]
    filing = filing_from_submissions(submissions(info["cik"]), ticker, accession)
    day = _et(filing["ts"]).date()
    ticks = select_window(ticks_from_bars(fetch_bars(ticker, day, day + timedelta(days=5)), ticker), filing["ts"])
    lines = [{"topic": "filings", "msg": filing}] + [{"topic": "ticks", "msg": x} for x in ticks]
    if news_file:  # [{"time": "2025-09-24T10:12:00-04:00", "source": ..., "headline": ..., "url": ...}]
        for n in json.loads(Path(news_file).read_text()):
            lines.append({"topic": "news", "msg": {"run": "", "ts": _ms(n["time"]), "ticker": ticker,
                                                   "source": n["source"], "headline": n["headline"], "url": n["url"]}})
    lines.sort(key=lambda line: line["msg"]["ts"])
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{ticker}-{day}.jsonl"
    path.write_text("".join(json.dumps(line) + "\n" for line in lines))
    print(f"wrote {path}: 1 filing, {len(ticks)} ticks, {len(lines) - 1 - len(ticks)} news")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker", nargs="?")
    ap.add_argument("accession", nargs="?")
    ap.add_argument("--news")
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()
    list_main() if a.list else record_main(a.ticker, a.accession, a.news)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_record.py -v`
Expected: all PASS

- [ ] **Step 5: Commit the recorder**
```bash
git add record.py tests/test_record.py
git commit -m "feat: record a real earnings day into a replay file"
```

- [ ] **Step 6: Record the demo day and a backup (do this early; the team needs the files)**

Run: `SEC_UA="AlphaRadar hackathon you@team.dev" python record.py --list`
Pick the two filings with the largest absolute next-day move (≥5%). For each, write `data/replays/<TICKER>-news.json` with 2–4 real headlines from that window (at least one negative one *after* the next session's open), using the `time/source/headline/url` format in the comment above. Then:
```bash
SEC_UA="AlphaRadar hackathon you@team.dev" python record.py MU <ACCESSION> --news data/replays/MU-news.json
```
Expected: `wrote data/replays/MU-<date>.jsonl: 1 filing, ~130 ticks, N news`. Post the ticker and accession in the team chat (Person B needs them for Task 8 Step 4).
```bash
git add data/replays
git commit -m "data: recorded earnings-day replays for the demo"
```

---

### Task 5: `replay.py` — play a recorded day into Kafka (Person A)

**Files:**
- Create: `replay.py`, `tests/test_replay.py`

**Interfaces:**
- Consumes: `common.emit`, `common.new_replay_run`, `common.producer`; replay file format from Task 4
- Produces:
  - `replay.MAX_GAP_S = 5`
  - `replay.load(path) -> list[dict]`
  - `replay.schedule(lines: list[dict], speed: float, run: str) -> list[tuple[float, str, dict]]` — `(sleep_before_s, topic, msg)`; sets `run`, marks the last tick `"last": True`
  - `replay.play(path, speed: float, prod, run: str | None = None) -> str` (blocks until done; returns the run) — used by the dashboard (Task 11) and the e2e check (Task 13)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_replay.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_replay.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'replay'`

- [ ] **Step 3: Implement `replay.py`**

```python
"""Play a recorded earnings day (data/replays/*.jsonl) into Kafka as a new run, N times faster than it happened.

  python replay.py data/replays/MU-2025-09-23.jsonl [--speed 300]

Messages keep their historical ts; only the pacing is compressed, and any single gap (overnight, weekend) is capped
at MAX_GAP_S so the demo never stalls. The last tick is marked "last" so the trader closes out and the scorer shows
realized P&L.
"""
import argparse
import json
import time
from pathlib import Path

from common import emit, new_replay_run, producer

MAX_GAP_S = 5


def load(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def schedule(lines, speed, run):
    last_tick = max((i for i, line in enumerate(lines) if line["topic"] == "ticks"), default=None)
    out, prev = [], None
    for i, line in enumerate(lines):
        msg = {**line["msg"], "run": run}
        if i == last_tick:
            msg["last"] = True
        gap = 0 if prev is None else min(MAX_GAP_S, max(0, (msg["ts"] - prev) / 1000 / speed))
        prev = msg["ts"]
        out.append((gap, line["topic"], msg))
    return out


def play(path, speed, prod, run=None):
    run = run or new_replay_run()
    for gap, topic, msg in schedule(load(path), speed, run):
        time.sleep(gap)
        emit(prod, topic, msg.get("ticker", "_"), msg)
    prod.flush()
    return run


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--speed", type=float, default=300)
    a = ap.parse_args()
    run = new_replay_run()
    print(f"replaying {a.file} as {run} at {a.speed:g}x")
    play(a.file, a.speed, producer(), run)
    print("done")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_replay.py -v`
Expected: all PASS

- [ ] **Step 5: Try it against Kafka**

Run (with `docker compose up -d kafka`):
```bash
python replay.py data/replays/<file>.jsonl --speed 3000
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ticks --from-beginning --max-messages 2
```
Expected: `replaying … done`, then two tick JSON lines with `"run": "replay-…"`.

- [ ] **Step 6: Commit**
```bash
git add replay.py tests/test_replay.py
git commit -m "feat: replay recorded earnings days into Kafka as a new run"
```

---

### Task 6: `router.py` — trigger rules (Person B)

**Files:**
- Create: `router.py`, `tests/test_router.py`

**Interfaces:**
- Consumes: `common.consume`, `common.emit`, `common.load_watchlist`, `common.producer`; topic `filings`
- Produces: `router.Router(watchlist: dict)` with `.on_filing(filing: dict) -> dict | None` (a skill-requests message); topic `skill-requests`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_router.py`:
```python
from common import missing_keys
from router import Router

WATCH = {"MU": {"cik": "0000723125", "names": ["Micron"]}}
F = {"run": "replay-1", "ts": 1, "ticker": "MU", "cik": "0000723125", "form": "8-K",
     "items": ["2.02", "9.01"], "accession": "acc-1", "url": "u", "title": "t"}


def test_earnings_8k_triggers_earnings_review():
    req = Router(WATCH).on_filing(F)
    assert req == {"run": "replay-1", "ts": 1, "request_id": "MU-acc-1", "ticker": "MU",
                   "skill": "earnings-review", "args": "MU latest", "reason": "8-K Item 2.02",
                   "trigger": {"topic": "filings", "accession": "acc-1"}}
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
    assert r.on_filing({**F, "run": "replay-2"})["request_id"] == "MU-acc-1"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_router.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'router'`

- [ ] **Step 3: Implement `router.py`**

```python
"""Turn events into research requests: an earnings filing for a watchlist company -> ai-berkshire /earnings-review.

Rule R1 of the spec. One request per ticker per run: the 8-K earnings release usually lands before the 10-Q for the
same quarter, and a second deep-research run would cost minutes and tokens for nothing. A new replay is a new run,
so replaying the same filing triggers again (and research.py serves it from cache).
"""
from common import consume, emit, load_watchlist, producer

EARNINGS_ITEM = "2.02"  # 8-K Item 2.02: Results of Operations and Financial Condition


class Router:
    def __init__(self, watchlist):
        self.watchlist = watchlist
        self.seen = set()  # (run, ticker)

    def on_filing(self, f):
        ticker = f["ticker"]
        is_8k_earnings = f["form"] == "8-K" and EARNINGS_ITEM in f["items"]
        if ticker not in self.watchlist or not (is_8k_earnings or f["form"] in ("10-Q", "10-K")):
            return None
        if (f["run"], ticker) in self.seen:
            return None
        self.seen.add((f["run"], ticker))
        return {"run": f["run"], "ts": f["ts"], "request_id": f"{ticker}-{f['accession']}", "ticker": ticker,
                "skill": "earnings-review", "args": f"{ticker} latest",
                "reason": f"8-K Item {EARNINGS_ITEM}" if is_8k_earnings else f["form"],
                "trigger": {"topic": "filings", "accession": f["accession"]}}


def main():
    router, prod = Router(load_watchlist()), producer()
    for _, filing in consume(["filings"]):
        req = router.on_filing(filing)
        if req:
            emit(prod, "skill-requests", req["ticker"], req)
            prod.flush()
            print(f"{req['run']}: {req['reason']} -> /{req['skill']} {req['args']}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_router.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**
```bash
git add router.py tests/test_router.py
git commit -m "feat: router turns earnings filings into research requests"
```

---

### Task 7: `research.py` core — verdict validation and the Researcher (Person B)

**Files:**
- Create: `research.py`, `tests/test_research.py`

**Interfaces:**
- Consumes: `common.LIVE`; skill-requests messages (contract fixture)
- Produces:
  - `research.VERDICTS = {"PASS", "GRAY", "FAIL"}`, `research.MASTERS = ("buffett", "munger", "duan", "lilu")`
  - `research.extract_json(text: str) -> dict` (raises `ValueError`)
  - `research.validate_verdict(d) -> dict` — the 8 verdict fields, raises `ValueError`
  - `research.Researcher(run_skill, extract, data_dir=DATA, cache_delay_s=CACHE_DELAY_S, sleep=time.sleep, clock=time.time)` with `.handle(req: dict, out: Callable[[dict], None]) -> None` — calls `out` with `started`, then `done` or `failed`
    - `run_skill(skill: str, args: str) -> str` (report text); `extract(report: str) -> str` (model text containing JSON)
  - Files: `data/verdicts/<request_id>.json` (cached verdict incl. `report_path`, `duration_s`), `data/reports/<request_id>.md`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_research.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_research.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'research'`

- [ ] **Step 3: Implement the core of `research.py`** (the CLI runner and `main()` come in Task 8)

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_research.py -v`
Expected: all PASS

- [ ] **Step 5: Commit**
```bash
git add research.py tests/test_research.py
git commit -m "feat: research verdict validation and cached/live research handling"
```

---

### Task 8: `research.py` — Claude Code runner, service loop, warm cache (Person B)

**Files:**
- Modify: `research.py` (append the runner and `main()`)
- Create: `data/verdicts/*.json`, `data/reports/*.md` (Step 4, committed)

**Interfaces:**
- Consumes: `research.Researcher`, `router.Router` (to build the same request the router would), `common.consume/emit/producer/load_watchlist`, `record.submissions`, `record.filing_from_submissions`
- Produces: `research.run_skill_cli(skill: str, args: str) -> str`, `research.extract_cli(report: str) -> str`; CLI `python research.py` (service) and `python research.py warm <TICKER> <ACCESSION>`

- [ ] **Step 1: Install ai-berkshire on the research laptop**

```bash
git clone https://github.com/xbtlin/ai-berkshire ~/ai-berkshire
cd ~/ai-berkshire && ./scripts/install-claude-commands.sh
ls ~/.claude/commands/earnings-review.md                      # the ai-berkshire command is installed
claude -p "reply with ok" --output-format json | head -c 200   # you are logged in (does not start research)
```
Expected: the file path is printed, then JSON starting with `{"type":"result"`.

- [ ] **Step 2: Append the runner and service to `research.py`**

Add to the imports at the top: `import subprocess`, `import sys`, `from concurrent.futures import ThreadPoolExecutor`, and change the `common` import to `from common import LIVE, consume, emit, load_watchlist, producer`. Then append:
```python
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
```
Note: `warm` runs with `run: "live"` so `handle` takes the live path, and the cache it writes is what every replay of that filing uses.

- [ ] **Step 3: Run the unit tests (nothing should break)**

Run: `pytest tests/test_research.py -v`
Expected: all PASS

- [ ] **Step 4: Warm the cache for both demo filings** (needs the ticker + accession from Task 4 Step 6; start as early as possible, each run takes minutes)

Run from the repo root:
```bash
SEC_UA="AlphaRadar hackathon you@team.dev" BERKSHIRE_DIR=~/ai-berkshire python research.py warm MU <ACCESSION>
```
Expected: a `started` JSON, then several minutes later a `done` JSON with `"source": "live"`, a verdict, scores and a buy band; files `data/verdicts/MU-<ACCESSION>.json` and `data/reports/MU-<ACCESSION>.md` exist. Note `duration_s` for the pitch.

If the verdict is not `PASS` or the band does not overlap the replayed prices, the demo will not trade. That is a real result: either pick the backup filing, or keep it and show "Berkshire said no, so the fast brain stayed out" as the story. Do **not** edit the verdict by hand.

- [ ] **Step 5: Commit**
```bash
git add research.py data/verdicts data/reports
git commit -m "feat: run ai-berkshire via Claude Code and cache demo verdicts"
```

---

### Task 9: `trader.py` — indicators, Livermore entry, Turtle sizing, paper broker (Person C)

**Files:**
- Create: `trader.py`, `tests/test_trader.py`

**Interfaces:**
- Consumes: research-verdicts and ticks messages (contract fixtures)
- Produces:
  - constants `CASH0, SLIPPAGE, PIVOT_BARS, VOL_LOOKBACK, VOL_MULT, ATR_N, RISK, MAX_POSITION`
  - `trader.atr(closes: list[float], n=ATR_N) -> float`
  - `trader.entry_signal(bars: list[dict], verdict: dict) -> tuple[float, float] | None` — `(pivot, volume_ratio)`
  - `trader.Trader()` with `.on_verdict(v: dict) -> list[tuple[str, dict]]`, `.on_tick(t: dict) -> list[tuple[str, dict]]`, `.books: dict[str, Book]` — outputs are `(topic, msg)` for `orders`, `fills`, `pnl`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_trader.py`:
```python
import pytest

from common import missing_keys
from trader import Trader, atr, entry_signal

T0 = 1_000_000_000_000
V = {"run": "replay-1", "ts": T0, "request_id": "MU-acc", "ticker": "MU", "status": "done", "source": "cache",
     "skill": "earnings-review", "verdict": "PASS", "score": 4.3,
     "masters": {"buffett": 4.4, "munger": 3.5, "duan": 3.7, "lilu": 4.0},
     "buy_low": 140.0, "buy_high": 160.0, "target": 170.0, "red_lines": [], "summary": "",
     "report_path": "data/reports/MU-acc.md", "duration_s": 1}
OPENING = [130, 134, 138, 142, 146, 150]  # first 30 minutes after the filing: pivot = 150, N = 4


def tick(i, price, volume=1000, session="regular", run="replay-1", **extra):
    return {"run": run, "ts": T0 + (i + 1) * 300_000, "ticker": "MU", "price": price,
            "volume": volume, "session": session, **extra}


def feed(trader, prices_volumes, start=0, run="replay-1"):
    out = []
    for i, (p, vol) in enumerate(prices_volumes, start):
        out += trader.on_tick(tick(i, p, vol, run=run))
    return out


def opened(run="replay-1"):
    trader = Trader()
    trader.on_verdict({**V, "run": run})
    feed(trader, [(p, 1000) for p in OPENING], run=run)
    return trader


def orders(out):
    return [m for topic, m in out if topic == "orders"]


def test_atr_is_mean_abs_close_change_over_last_n():
    assert atr([130, 134, 138, 142]) == 4
    assert atr([1, 2, 4, 7], n=2) == 2.5
    assert atr([5]) == 0


def test_breakout_above_pivot_on_volume_inside_band_buys():
    trader = opened()
    out = trader.on_tick(tick(6, 154, 2000))
    [o] = orders(out)
    assert (o["side"], o["qty"], o["rule"], o["request_id"]) == ("BUY", 125, "livermore_pivot", "MU-acc")
    assert "pivot 150.00" in o["reason"] and "2.0x volume" in o["reason"]
    [f] = [m for topic, m in out if topic == "fills"]
    assert f["price"] == pytest.approx(154.077)
    pnl = out[-1][1]
    assert pnl["positions"]["MU"] == {"qty": 125, "avg": pytest.approx(154.077), "last": 154, "stop": 146.08}
    for topic, msg in out:
        assert missing_keys(topic, msg) == set()


def test_no_entry_inside_the_first_30_minutes():
    trader = Trader()
    trader.on_verdict(V)
    assert orders(feed(trader, [(p, 9000) for p in OPENING])) == []


def test_no_entry_on_low_volume():
    assert orders(opened().on_tick(tick(6, 154, 1400))) == []


def test_no_entry_outside_the_band():
    assert orders(opened().on_tick(tick(6, 161, 2000))) == []


def test_no_entry_without_a_pass_verdict():
    trader = Trader()
    trader.on_verdict({**V, "verdict": "GRAY"})
    feed(trader, [(p, 1000) for p in OPENING])
    assert orders(trader.on_tick(tick(6, 154, 2000))) == []


def test_no_entry_without_any_verdict():
    trader = Trader()
    feed(trader, [(p, 1000) for p in OPENING])
    assert orders(trader.on_tick(tick(6, 154, 2000))) == []


def test_position_is_capped_at_25_percent_of_equity():
    trader = Trader()
    trader.on_verdict({**V, "buy_low": 10.0, "buy_high": 20.0})
    feed(trader, [(p, 1000) for p in [13.0, 13.1, 13.2, 13.3, 13.4, 13.5]])  # N = 0.1 -> risk size 5000 shares
    [o] = orders(trader.on_tick(tick(6, 13.6, 2000)))
    assert o["qty"] == 1838  # floor(25_000 / 13.6)


def test_late_verdict_still_enters_on_the_next_breakout_bar():
    trader = Trader()
    feed(trader, [(p, 1000) for p in OPENING] + [(154, 2000)])  # breakout happens before research is done
    trader.on_verdict(V)
    assert orders(trader.on_tick(tick(7, 155, 3000)))[0]["side"] == "BUY"


def test_only_one_entry_per_ticker_per_run():
    trader = opened()
    trader.on_tick(tick(6, 154, 2000))
    trader.on_tick(tick(7, 145, 1000))  # stopped out (Task 10)
    assert orders(trader.on_tick(tick(8, 155, 5000))) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_trader.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'trader'`

- [ ] **Step 3: Implement `trader.py`**

```python
"""Fast brain: trade only inside the Berkshire verdict, timing entries like Livermore and sizing/stopping like the Turtles.

  thesis gate   no position without a done PASS verdict with a buy band; a later FAIL sells
  entry         Livermore pivotal point: a close above the first 30 minutes' high, on 1.5x volume, inside the band
  size          Turtle: hitting the 2N stop costs 1% of equity (N = ATR), capped at 25% of equity
  exit          Turtle 2N stop, the verdict's target, or the end of a replay

Consumes research-verdicts and ticks; emits orders, fills (paper broker: same tick, 5 bps slippage) and pnl.
State is per run, so each replay trades a fresh $100k book and never touches live.
"""
import math
from collections import defaultdict

from common import consume, emit, producer

CASH0 = 100_000.0
SLIPPAGE = 0.0005
PIVOT_BARS = 6        # 6 x 5-minute bars = the first 30 minutes of the session
VOL_LOOKBACK = 12
VOL_MULT = 1.5
ATR_N = 14
RISK = 0.01
MAX_POSITION = 0.25


def atr(closes, n=ATR_N):
    """Average true range from closes only (ticks carry the bar close): mean |change| over the last n bars."""
    ranges = [abs(b - a) for a, b in zip(closes, closes[1:])][-n:]
    return sum(ranges) / len(ranges) if ranges else 0.0


def entry_signal(bars, verdict):
    """(pivot, volume ratio) if the newest bar is a Livermore breakout inside the buy band, else None."""
    if verdict["verdict"] != "PASS" or verdict["buy_low"] is None:
        return None
    regular = [b for b in bars if b["session"] == "regular" and b["ts"] > verdict["ts"]]
    if len(regular) <= PIVOT_BARS or regular[-1] is not bars[-1]:
        return None
    pivot = max(b["price"] for b in regular[:PIVOT_BARS])
    cur, prev = regular[-1], regular[-1 - VOL_LOOKBACK:-1]
    avg = sum(b["volume"] for b in prev) / len(prev)
    if (cur["price"] > pivot and avg > 0 and cur["volume"] >= VOL_MULT * avg
            and verdict["buy_low"] <= cur["price"] <= verdict["buy_high"]):
        return pivot, cur["volume"] / avg
    return None


class Book:
    """One run's paper account."""

    def __init__(self):
        self.cash, self.realized = CASH0, 0.0
        self.positions = {}              # ticker -> {qty, avg, last, stop, target, request_id}
        self.verdicts = {}               # ticker -> latest done verdict
        self.bars = defaultdict(list)    # ticker -> ticks, oldest first
        self.entered = set()             # one entry per ticker per run

    def equity(self):
        return self.cash + sum(p["qty"] * p["last"] for p in self.positions.values())


class Trader:
    def __init__(self):
        self.books = defaultdict(Book)
        self.order_seq = 0

    def on_verdict(self, v):
        if v.get("status") != "done":
            return []
        self.books[v["run"]].verdicts[v["ticker"]] = v
        return []

    def on_tick(self, t):
        book, ticker = self.books[t["run"]], t["ticker"]
        book.bars[ticker].append(t)
        out = []
        if ticker in book.positions:
            book.positions[ticker]["last"] = t["price"]
        elif ticker in book.verdicts and ticker not in book.entered:
            out += self._try_entry(book, t)
        out.append(("pnl", self.pnl(book, t["run"], t["ts"])))
        return out

    def _try_entry(self, book, t):
        v, bars = book.verdicts[t["ticker"]], book.bars[t["ticker"]]
        signal = entry_signal(bars, v)
        if not signal:
            return []
        pivot, volume_ratio = signal
        n = atr([b["price"] for b in bars])
        equity = book.equity()
        qty = min(math.floor(RISK * equity / (2 * n)), math.floor(MAX_POSITION * equity / t["price"])) if n > 0 else 0
        if qty < 1:
            return []
        book.entered.add(t["ticker"])
        reason = (f"broke 30-min pivot {pivot:.2f} on {volume_ratio:.1f}x volume; inside band "
                  f"{v['buy_low']:g}-{v['buy_high']:g}; Turtle size: 1% risk, N={n:.2f}; "
                  f"thesis {v['verdict']} {v['score']}/5")
        out = self._trade(book, t, t["ticker"], "BUY", qty, t["price"], "livermore_pivot", reason, v["request_id"])
        pos = book.positions[t["ticker"]]
        pos["stop"] = round(pos["avg"] - 2 * n, 2)
        pos["target"] = v["target"] if v["target"] is not None else round(v["buy_high"] * 1.10, 2)
        return out

    def _trade(self, book, at, ticker, side, qty, price, rule, reason, request_id):
        """Paper broker: fill right away at price +/- slippage and update the book. `at` supplies run and ts."""
        self.order_seq += 1
        order_id = f"o-{self.order_seq}"
        fill_price = round(price * (1 + SLIPPAGE if side == "BUY" else 1 - SLIPPAGE), 4)
        if side == "BUY":
            book.cash -= fill_price * qty
            book.positions[ticker] = {"qty": qty, "avg": fill_price, "last": price, "stop": None,
                                      "target": None, "request_id": request_id}
        else:
            pos = book.positions.pop(ticker)
            book.cash += fill_price * qty
            book.realized += (fill_price - pos["avg"]) * qty
        run, ts = at["run"], at["ts"]
        return [("orders", {"run": run, "ts": ts, "order_id": order_id, "ticker": ticker, "side": side, "qty": qty,
                            "rule": rule, "reason": reason, "request_id": request_id}),
                ("fills", {"run": run, "ts": ts, "order_id": order_id, "ticker": ticker, "side": side, "qty": qty,
                           "price": fill_price})]

    def pnl(self, book, run, ts):
        unrealized = sum((p["last"] - p["avg"]) * p["qty"] for p in book.positions.values())
        return {"run": run, "ts": ts, "cash": round(book.cash, 2), "equity": round(book.equity(), 2),
                "realized": round(book.realized, 2), "unrealized": round(unrealized, 2),
                "positions": {tk: {k: p[k] for k in ("qty", "avg", "last", "stop")} for tk, p in book.positions.items()}}


def main():
    trader, prod = Trader(), producer()
    for topic, msg in consume(["research-verdicts", "ticks"]):
        out = trader.on_verdict(msg) if topic == "research-verdicts" else trader.on_tick(msg)
        for out_topic, m in out:
            emit(prod, out_topic, m["run"] if out_topic == "pnl" else m["ticker"], m)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/test_trader.py -v`
Expected: all PASS (`test_only_one_entry_per_ticker_per_run` passes here because the position is still held; after Task 10 it passes because the stop closed it and `entered` blocks a re-entry).

- [ ] **Step 5: Commit**
```bash
git add trader.py tests/test_trader.py
git commit -m "feat: fast brain with Livermore entries, Turtle sizing and a paper broker"
```

---

### Task 10: `trader.py` — exits, thesis fail, end of run, run isolation (Person C)

**Files:**
- Modify: `trader.py` (`Trader.on_verdict`, `Trader.on_tick`)
- Modify: `tests/test_trader.py` (append)

**Interfaces:**
- Consumes/Produces: same as Task 9; adds rules `turtle_stop`, `target_hit`, `thesis_fail`, `end_of_run`

- [ ] **Step 1: Append the failing tests to `tests/test_trader.py`**

```python
def bought(run="replay-1"):
    trader = opened(run)
    trader.on_tick(tick(6, 154, 2000, run=run))
    return trader


def test_stop_at_2n_sells():
    [o] = orders(bought().on_tick(tick(7, 146, 1000)))
    assert (o["side"], o["qty"], o["rule"]) == ("SELL", 125, "turtle_stop")


def test_target_hit_sells_and_books_realized_pnl():
    trader = bought()
    assert orders(trader.on_tick(tick(7, 160, 1000))) == []
    out = trader.on_tick(tick(8, 170, 1000))
    assert orders(out)[0]["rule"] == "target_hit"
    pnl = out[-1][1]
    assert pnl["positions"] == {}
    assert pnl["realized"] == pytest.approx((169.915 - 154.077) * 125, abs=0.01)
    assert pnl["cash"] == pytest.approx(100_000 + pnl["realized"], abs=0.01)


def test_target_defaults_to_110_percent_of_buy_high():
    trader = Trader()
    trader.on_verdict({**V, "target": None})
    feed(trader, [(p, 1000) for p in OPENING] + [(154, 2000)])
    assert orders(trader.on_tick(tick(7, 175, 1000))) == []
    assert orders(trader.on_tick(tick(8, 176, 1000)))[0]["rule"] == "target_hit"


def test_fail_verdict_sells_a_held_position():
    trader = bought()
    out = trader.on_verdict({**V, "verdict": "FAIL", "score": 1.5, "buy_low": None, "buy_high": None})
    assert orders(out)[0]["rule"] == "thesis_fail"
    assert out[-1] == ("pnl", trader.pnl(trader.books["replay-1"], "replay-1", V["ts"]))


def test_last_tick_closes_everything():
    trader = bought()
    out = trader.on_tick(tick(7, 155, 1000, last=True))
    assert orders(out)[0]["rule"] == "end_of_run"
    assert out[-1][1]["positions"] == {}


def test_runs_have_separate_books():
    trader = bought("replay-1")
    trader.on_verdict({**V, "run": "replay-2"})
    assert trader.pnl(trader.books["replay-2"], "replay-2", 0)["equity"] == 100_000
    out = feed(trader, [(p, 1000) for p in OPENING] + [(154, 2000)], run="replay-2")
    assert orders(out)[0]["qty"] == 125  # a fresh $100k book, same sizing as run 1
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `pytest tests/test_trader.py -v`
Expected: the six new tests FAIL (no SELL orders yet); Task 9's tests PASS.

- [ ] **Step 3: Replace `Trader.on_verdict` and `Trader.on_tick` in `trader.py`**

```python
    def on_verdict(self, v):
        if v.get("status") != "done":
            return []
        book = self.books[v["run"]]
        book.verdicts[v["ticker"]] = v
        pos = book.positions.get(v["ticker"])
        if v["verdict"] != "FAIL" or not pos:
            return []
        out = self._trade(book, v, v["ticker"], "SELL", pos["qty"], pos["last"], "thesis_fail",
                          f"new verdict FAIL ({v['score']}/5): thesis broken", v["request_id"])
        return out + [("pnl", self.pnl(book, v["run"], v["ts"]))]

    def on_tick(self, t):
        book, ticker, price = self.books[t["run"]], t["ticker"], t["price"]
        book.bars[ticker].append(t)
        out = []
        pos = book.positions.get(ticker)
        if pos:
            pos["last"] = price
            if price <= pos["stop"]:
                out += self._trade(book, t, ticker, "SELL", pos["qty"], price, "turtle_stop",
                                   f"price {price:.2f} hit the 2N stop {pos['stop']:.2f}", pos["request_id"])
            elif price >= pos["target"]:
                out += self._trade(book, t, ticker, "SELL", pos["qty"], price, "target_hit",
                                   f"price {price:.2f} reached the target {pos['target']:.2f}", pos["request_id"])
        elif ticker in book.verdicts and ticker not in book.entered:
            out += self._try_entry(book, t)
        if t.get("last"):  # end of a replay: close out so the scorer shows realized P&L
            for other, p in list(book.positions.items()):
                out += self._trade(book, t, other, "SELL", p["qty"], p["last"], "end_of_run",
                                   "replay finished: closing out", p["request_id"])
        out.append(("pnl", self.pnl(book, t["run"], t["ts"])))
        return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_trader.py -v`
Expected: all PASS (including `test_only_one_entry_per_ticker_per_run`)

- [ ] **Step 5: Commit**
```bash
git add trader.py tests/test_trader.py
git commit -m "feat: Turtle stops, targets, thesis-fail and end-of-run exits"
```

---

### Task 11: `dashboard.py` — multi-topic SSE and replay endpoint (Person C)

**Files:**
- Modify: `dashboard.py` (rewrite; the radar keeps working at `/radar`)
- Create: `tests/test_dashboard.py`

**Interfaces:**
- Consumes: `common.KAFKA, LIVE, ensure_topics, new_replay_run, producer, run_start_ms`, `replay.play`
- Produces (HTTP, used by Task 12):
  - `GET /` → `cockpit.html`; `GET /radar` → `index.html`; `GET /watchlist.json`
  - `GET /events?topics=a,b&run=R` → SSE; `data` = message + `"topic"`; `id` = bare offset (one topic) or `{"topic": offset}` JSON
  - `GET /replays` → `["MU-2025-09-23.jsonl", ...]`; `GET /runs` → `["live", "replay-…", ...]` (newest replay first)
  - `POST /replay?file=F&speed=N` → `{"run": "replay-…"}`; 400 for an unknown file
- Python: `dashboard.parse_last_id(value: str | None, topics: list[str]) -> dict[str, int]`, `dashboard.format_id(offsets: dict[str, int], topics: list[str]) -> str`, `dashboard.replay_path(name: str) -> Path | None`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_dashboard.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Do **not** run it against the old `dashboard.py`: that file starts its HTTP server at import time, so pytest would hang. The rewrite below adds a `__main__` guard. Go to Step 3.

- [ ] **Step 3: Rewrite `dashboard.py`**

```python
"""Serve the cockpit and radar pages and stream Kafka topics to browsers over Server-Sent Events.

GET  /events?topics=a,b&run=R   one Kafka consumer per tab over partition 0 of each topic, starting at the run's
                                start (live: the last hour). Messages of other runs are skipped; hn-events always
                                passes, it is the live backdrop. The SSE id carries the offsets, so EventSource
                                reconnects resume exactly; with one topic it is the bare offset, as the radar expects.
POST /replay?file=F&speed=N     plays data/replays/F into Kafka as a new run and returns {"run": ...}
"""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from kafka import KafkaConsumer, TopicPartition

import replay
from common import KAFKA, LIVE, ensure_topics, new_replay_run, producer, run_start_ms

PORT = int(os.environ.get("PORT", 8080))
HERE = Path(__file__).parent
REPLAYS = HERE / "data" / "replays"
PAGES = {"/": ("cockpit.html", "text/html; charset=utf-8"),
         "/radar": ("index.html", "text/html; charset=utf-8"),
         "/watchlist.json": ("watchlist.json", "application/json")}
RUNS = [LIVE]       # live, then replays started from this dashboard, newest first
PRODUCER = None     # created in main(); only POST /replay writes to Kafka


def parse_last_id(value, topics):
    if not value:
        return {}
    if value.startswith("{"):
        return {t: int(o) for t, o in json.loads(value).items() if t in topics}
    return {topics[0]: int(value)}


def format_id(offsets, topics):
    return str(offsets[topics[0]]) if len(topics) == 1 else json.dumps(offsets, separators=(",", ":"))


def replay_path(name):
    path = REPLAYS / name
    return path if name and path.parent == REPLAYS and path.is_file() else None


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        q = parse_qs(url.query)
        if url.path == "/events":
            return self.stream(q.get("topics", ["hn-events"])[0].split(","), q.get("run", [None])[0])
        if url.path == "/replays":
            return self.send_json(sorted(p.name for p in REPLAYS.glob("*.jsonl")))
        if url.path == "/runs":
            return self.send_json(RUNS)
        if url.path not in PAGES:
            return self.send_error(404)
        name, ctype = PAGES[url.path]
        body = (HERE / name).read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        url = urlsplit(self.path)
        q = parse_qs(url.query)
        path = replay_path(q.get("file", [""])[0])
        if url.path != "/replay" or not path:
            return self.send_error(400, "unknown replay file")
        run = new_replay_run()
        RUNS.insert(1, run)
        threading.Thread(target=replay.play, args=(path, float(q.get("speed", ["300"])[0]), PRODUCER, run),
                         daemon=True).start()
        self.send_json({"run": run})

    def send_json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def stream(self, topics, run):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        ensure_topics(topics)
        tps = [TopicPartition(t, 0) for t in topics]
        consumer = KafkaConsumer(bootstrap_servers=KAFKA)
        try:
            consumer.assign(tps)
            offsets = parse_last_id(self.headers.get("Last-Event-ID"), topics)  # sent by EventSource on reconnect
            starts = consumer.offsets_for_times({tp: run_start_ms(run or LIVE) for tp in tps})
            for tp in tps:
                if tp.topic in offsets:
                    consumer.seek(tp, offsets[tp.topic] + 1)
                elif starts[tp]:
                    consumer.seek(tp, starts[tp].offset)
                else:
                    consumer.seek_to_end(tp)
            while True:
                chunk = []
                for tp, records in consumer.poll(timeout_ms=15000).items():
                    for r in records:
                        offsets[tp.topic] = r.offset
                        msg = json.loads(r.value)
                        if run and tp.topic != "hn-events" and msg.get("run", LIVE) != run:
                            continue
                        msg["topic"] = tp.topic
                        chunk.append(f"id: {format_id(offsets, topics)}\ndata: {json.dumps(msg)}\n\n")
                self.wfile.write(("".join(chunk) or ": keep-alive\n\n").encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass  # browser tab went away
        finally:
            consumer.close()


def main():
    global PRODUCER
    PRODUCER = producer()
    print(f"Alpha Radar on http://localhost:{PORT}  (radar at /radar, kafka {KAFKA})")
    ThreadingHTTPServer(("", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_dashboard.py -v`
Expected: all PASS

- [ ] **Step 5: Check the radar still works**

Run: `docker compose up -d kafka producer && KAFKA=localhost:9092 python dashboard.py`, open `http://localhost:8080/radar`.
Expected: the radar fills with HN data as before. (`/` serves `cockpit.html`, which arrives in Task 12; until then `/` errors, which is expected.)

- [ ] **Step 6: Commit**
```bash
git add dashboard.py tests/test_dashboard.py
git commit -m "feat: multi-topic SSE with run filtering and a replay endpoint"
```

---

### Task 12: `cockpit.html` — the trading cockpit (Person C)

**Files:**
- Create: `cockpit.html`
- Modify: `index.html` header (one link back to the cockpit)

**Interfaces:**
- Consumes: the HTTP API from Task 11; message shapes from spec §3 (+ `topic`)
- Produces: the demo screen. Panels: price chart (band, pivot, stop, trade markers), research card, decision timeline, P&L vs buy-and-hold, news + HN mentions, links to the radar.

- [ ] **Step 1: Create `cockpit.html`**

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Alpha Radar</title>
<style>
  :root {
    --bg: #070b10; --panel: #0c131bcc; --line: #1a2531; --text: #d3dce6; --dim: #6f8296;
    --buy: #3ddc84; --sell: #ff5c6c; --pivot: #ffd166; --accent: #4da3ff; --gray: #b48cff;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body {
    display: flex; flex-direction: column; color: var(--text);
    background: radial-gradient(ellipse at 15% -20%, #102331, var(--bg) 60%) fixed;
    font: 13px/1.5 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  }
  a { color: inherit; } .dim { color: var(--dim); }
  header { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; padding: 12px 18px; border-bottom: 1px solid var(--line); }
  .logo { font-weight: 700; letter-spacing: .22em; }
  .logo b { display: inline-block; width: 22px; margin-right: 10px; background: var(--buy); color: #04110a; text-align: center; letter-spacing: 0; }
  select, button { font: inherit; color: var(--text); background: #0c131b; border: 1px solid var(--line); border-radius: 4px; padding: 3px 8px; }
  button { border-color: var(--buy); color: var(--buy); cursor: pointer; }
  #status { margin-left: auto; color: var(--dim); }
  #status i { display: inline-block; width: 8px; height: 8px; margin-right: 6px; border-radius: 50%; background: var(--sell); }
  #status.live i { background: var(--buy); box-shadow: 0 0 10px var(--buy); }
  main { flex: 1; min-height: 0; display: grid; grid-template-columns: 1.6fr 1fr; grid-template-rows: 1.3fr 1fr; gap: 12px; padding: 12px 18px 18px; }
  .panel { min-height: 0; display: flex; flex-direction: column; padding: 10px 12px; background: var(--panel); border: 1px solid var(--line); border-radius: 6px; overflow: hidden; }
  h2 { display: flex; gap: 10px; margin: 0 0 8px; color: var(--dim); font-size: 11px; font-weight: 600; letter-spacing: .14em; text-transform: uppercase; }
  h2 small { margin-left: auto; font-weight: 400; letter-spacing: 0; text-transform: none; }
  .canvas { position: relative; flex: 1; min-height: 160px; }
  canvas { position: absolute; inset: 0; width: 100%; height: 100%; }
  ol, ul { flex: 1; min-height: 0; margin: 0; padding: 0; overflow-y: auto; list-style: none; }
  li { padding: 3px 6px; border-radius: 4px; }
  li:hover { background: #ffffff0a; }
  .badge { display: inline-block; padding: 2px 10px; border-radius: 4px; font-weight: 700; color: #04110a; }
  .PASS { background: var(--buy); } .GRAY { background: var(--gray); } .FAIL { background: var(--sell); }
  .kv { display: grid; grid-template-columns: auto 1fr; gap: 2px 12px; margin: 8px 0; }
  .num { font-variant-numeric: tabular-nums; }
  .up { color: var(--buy); } .down { color: var(--sell); }
  @media (max-width: 900px) { main { grid-template-columns: 1fr; grid-template-rows: none; } }
</style>
</head>
<body>
<header>
  <span class="logo"><b>α</b>ALPHA RADAR</span>
  <label class="dim">run <select id="run"></select></label>
  <label class="dim">day <select id="file"></select></label>
  <label class="dim">speed <select id="speed"><option>300</option><option>600</option><option>60</option></select>×</label>
  <button id="go">▶ Replay earnings day</button>
  <a href="radar" target="_blank" class="dim">HN radar ↗</a>
  <span id="status"><i></i><b id="state">CONNECTING</b></span>
</header>
<main>
  <section class="panel">
    <h2>Price <small id="tk"></small></h2>
    <div class="canvas"><canvas id="chart"></canvas></div>
    <p class="dim" style="margin:6px 0 0;font-size:11px">green band = Berkshire buy range · yellow = 30-min pivot (Livermore) · red = 2N stop (Turtle) · ▲ buy ▼ sell</p>
  </section>
  <section class="panel">
    <h2>Research · ai-berkshire <small id="rsrc"></small></h2>
    <div id="research" class="dim">Waiting for an earnings filing…</div>
  </section>
  <section class="panel">
    <h2>Decision timeline</h2>
    <ol id="timeline"></ol>
  </section>
  <section class="panel">
    <h2>P&amp;L <small id="bh"></small></h2>
    <div id="pnl" class="num"></div>
    <h2 style="margin-top:10px">News · HN mentions</h2>
    <ul id="news"></ul>
  </section>
</main>
<script>
const $ = s => document.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[c]));
const usd = x => x == null ? '–' : '$' + Number(x).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
const pct = x => (x >= 0 ? '+' : '') + (x * 100).toFixed(2) + '%';
const et = ts => new Date(ts).toLocaleString('en-US', {timeZone: 'America/New_York', month: 'short', day: 'numeric',
  hour: '2-digit', minute: '2-digit', hour12: false}) + ' ET';
const TOPICS = 'filings,ticks,news,hn-events,skill-requests,research-verdicts,orders,fills,pnl';
const CASH0 = 100000;

let watch = {};
fetch('watchlist.json').then(r => r.json()).then(w => watch = w);
const reEsc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const mentions = title => Object.entries(watch)
  .filter(([, w]) => w.names.concat(w.aliases || []).some(n => new RegExp(`\\b${reEsc(n)}\\b`).test(title)))
  .map(([t]) => t);

// ---- state for the selected run, rebuilt from Kafka on every (re)connect ----
let S, es;
function reset() {
  S = {filing: null, ticks: [], started: null, verdict: null, orders: new Map(), fills: [], pnl: null, news: [], events: []};
}

function ingest(m) {
  switch (m.topic) {
    case 'filings':
      S.filing = m;
      S.ticks = S.ticks.filter(t => t.ticker === m.ticker);
      S.events.push([m.ts, '📄', `${m.ticker} filed ${m.form}${m.items.length ? ' · items ' + m.items.join(', ') : ''}`, m.url]);
      break;
    case 'ticks':
      if (!S.filing || m.ticker === S.filing.ticker) S.ticks.push(m);
      break;
    case 'news':
      if (m.ticker !== '_') S.news.unshift({ts: m.ts, src: m.source, tk: m.ticker, text: m.headline, url: m.url});
      break;
    case 'hn-events': {
      const it = m.item;
      if (m.kind === 'new' && it?.type === 'story' && it.title)
        for (const tk of mentions(it.title))
          S.news.unshift({ts: m.ts, src: 'HN', tk, text: it.title, url: `https://news.ycombinator.com/item?id=${it.id}`});
      break;
    }
    case 'skill-requests':
      S.events.push([m.ts, '⚡', `router: ${m.reason} → /${m.skill} ${m.args}`]);
      break;
    case 'research-verdicts':
      if (m.status === 'started') { S.started = {...m, wall: Date.now()}; S.verdict = null; S.events.push([m.ts, '🔬', `ai-berkshire /${m.skill} started`]); }
      else if (m.status === 'done') { S.verdict = m; S.events.push([m.ts, '⚖️', `verdict ${m.verdict} ${m.score}/5 · buy ${usd(m.buy_low)}–${usd(m.buy_high)} · ${m.source}`]); }
      else { S.verdict = m; S.events.push([m.ts, '⚠️', `research failed: ${m.error || ''}`]); }
      break;
    case 'orders': S.orders.set(m.order_id, m); break;
    case 'fills': S.fills.push(m); break;
    case 'pnl': S.pnl = m; break;
  }
  if (S.news.length > 100) S.news.length = 100;
}

function connect(run) {
  es?.close();
  reset();
  es = new EventSource(`events?topics=${TOPICS}&run=${encodeURIComponent(run)}`);
  es.onopen = () => { $('#status').className = 'live'; $('#state').textContent = run.toUpperCase(); };
  es.onerror = () => { $('#status').className = ''; $('#state').textContent = 'RECONNECTING'; };
  es.onmessage = e => ingest(JSON.parse(e.data));
}

async function loadRuns(selected) {
  const runs = await (await fetch('runs')).json();
  $('#run').innerHTML = runs.map(r => `<option ${r === selected ? 'selected' : ''}>${esc(r)}</option>`).join('');
}
$('#run').onchange = e => connect(e.target.value);
$('#go').onclick = async () => {
  const resp = await fetch(`replay?file=${encodeURIComponent($('#file').value)}&speed=${$('#speed').value}`, {method: 'POST'});
  if (!resp.ok) return alert('Replay failed: ' + resp.status + ' (is there a file in data/replays?)');
  const {run} = await resp.json();
  await loadRuns(run);
  connect(run);
};
fetch('replays').then(r => r.json()).then(fs => $('#file').innerHTML = fs.map(f => `<option>${esc(f)}</option>`).join(''));

// ---- rendering, twice a second ----
const shown = {};
const html = (sel, s) => { if (shown[sel] !== s) $(sel).innerHTML = shown[sel] = s; };

function fit(c) {
  const r = c.getBoundingClientRect(), d = devicePixelRatio || 1;
  if (c.width !== Math.round(r.width * d) || c.height !== Math.round(r.height * d)) { c.width = Math.round(r.width * d); c.height = Math.round(r.height * d); }
  const g = c.getContext('2d');
  g.setTransform(d, 0, 0, d, 0, 0);
  return [g, r.width, r.height];
}

function pivotOf(ticks, filingTs) {  // same rule as trader.py: high of the first 6 regular bars after the filing
  const reg = ticks.filter(t => t.session === 'regular' && t.ts > filingTs);
  return reg.length >= 6 ? Math.max(...reg.slice(0, 6).map(t => t.price)) : null;
}

function drawChart() {
  const [g, W, H] = fit($('#chart'));
  g.clearRect(0, 0, W, H);
  g.font = '11px ui-monospace, Menlo, monospace';
  const T = S.ticks;
  if (T.length < 2) { g.fillStyle = '#6f8296'; g.fillText('No prices yet: pick a recorded day and press Replay', 12, 20); return; }
  const v = S.verdict?.status === 'done' ? S.verdict : null;
  const pos = S.pnl?.positions?.[T[0].ticker];
  const piv = S.filing ? pivotOf(T, S.filing.ts) : null;
  const levels = [...T.map(t => t.price), v?.buy_low, v?.buy_high, pos?.stop, piv].filter(x => x != null);
  let lo = Math.min(...levels), hi = Math.max(...levels);
  const pad = (hi - lo) * 0.08 || 1; lo -= pad; hi += pad;
  const L = 8, R = W - 70, TOP = 8, B = H - 20;
  const x = i => L + (R - L) * i / (T.length - 1), y = p => B - (B - TOP) * (p - lo) / (hi - lo);
  const hline = (p, color, dash, label) => {
    g.strokeStyle = color; g.setLineDash(dash); g.beginPath(); g.moveTo(L, y(p)); g.lineTo(R, y(p)); g.stroke(); g.setLineDash([]);
    g.fillStyle = color; g.fillText(label, R + 4, y(p) + 4);
  };
  if (v?.buy_low != null) { g.fillStyle = '#3ddc8422'; g.fillRect(L, y(v.buy_high), R - L, y(v.buy_low) - y(v.buy_high)); }
  T.forEach((t, i) => { if (t.session !== 'regular') { g.fillStyle = '#ffffff06'; g.fillRect(x(i) - (R - L) / T.length / 2, TOP, (R - L) / T.length, B - TOP); } });
  if (S.filing) {
    const i = T.findIndex(t => t.ts >= S.filing.ts);
    if (i >= 0) { g.strokeStyle = '#4da3ff'; g.beginPath(); g.moveTo(x(i), TOP); g.lineTo(x(i), B); g.stroke(); g.fillStyle = '#4da3ff'; g.fillText('8-K', x(i) + 3, TOP + 10); }
  }
  if (piv != null) hline(piv, '#ffd166', [4, 4], `pivot ${piv.toFixed(2)}`);
  if (pos?.stop) hline(pos.stop, '#ff5c6c', [2, 3], `stop ${pos.stop.toFixed(2)}`);
  g.strokeStyle = '#d3dce6'; g.lineWidth = 1.5; g.beginPath();
  T.forEach((t, i) => i ? g.lineTo(x(i), y(t.price)) : g.moveTo(x(i), y(t.price)));
  g.stroke(); g.lineWidth = 1;
  for (const f of S.fills) {
    const i = T.findIndex(t => t.ts >= f.ts);
    if (i < 0) continue;
    const buy = f.side === 'BUY', px = x(i), py = y(f.price), d = buy ? 1 : -1;
    g.fillStyle = buy ? '#3ddc84' : '#ff5c6c';
    g.beginPath(); g.moveTo(px, py + 4 * d); g.lineTo(px - 6, py + 13 * d); g.lineTo(px + 6, py + 13 * d); g.fill();
    g.fillText(S.orders.get(f.order_id)?.rule || f.side, px + 8, py + 13 * d);
  }
  g.fillStyle = '#6f8296';
  g.fillText(et(T[0].ts), L, H - 4);
  const end = et(T.at(-1).ts); g.fillText(end, R - g.measureText(end).width, H - 4);
}

function renderResearch() {
  const v = S.verdict, s = S.started;
  if (v?.status === 'done') {
    const m = v.masters;
    html('#rsrc', `${esc(v.source)} · ${v.duration_s}s`);
    html('#research', `<div><span class="badge ${esc(v.verdict)}">${esc(v.verdict)}</span> <b class="num">${v.score}/5</b> · ${esc(v.ticker)} /${esc(v.skill)}</div>
      <div class="kv"><span class="dim">Buffett</span><span>${m.buffett}</span><span class="dim">Munger</span><span>${m.munger}</span>
      <span class="dim">Duan</span><span>${m.duan}</span><span class="dim">Li Lu</span><span>${m.lilu}</span>
      <span class="dim">Buy band</span><span>${usd(v.buy_low)} – ${usd(v.buy_high)}</span><span class="dim">Target</span><span>${usd(v.target)}</span></div>
      <div>${esc(v.summary)}</div>
      ${v.red_lines.length ? `<div class="dim" style="margin-top:6px">Red lines: ${v.red_lines.map(esc).join(' · ')}</div>` : ''}
      <div class="dim" style="margin-top:6px">${esc(v.report_path)}</div>`);
  } else if (v?.status === 'failed') {
    html('#rsrc', ''); html('#research', `<span class="down">Research failed</span> <span class="dim">${esc(v.error)}</span>`);
  } else if (s) {
    const secs = Math.round((Date.now() - s.wall) / 1000);
    html('#rsrc', ''); html('#research', `🔬 4 masters researching <b>${esc(s.ticker)}</b>… <span class="num">${Math.floor(secs / 60)}:${String(secs % 60).padStart(2, '0')}</span>
      <div class="dim">Buffett · Munger · Duan Yongping · Li Lu are reading the filing</div>`);
  }
}

function renderTimeline() {
  const fills = S.fills.map(f => {
    const o = S.orders.get(f.order_id) || {};
    return [f.ts, f.side === 'BUY' ? '🟢' : '🔴', `${f.side} ${f.qty} ${f.ticker} @ ${usd(f.price)} · ${o.rule || ''}${o.reason ? ' — ' + o.reason : ''}`];
  });
  const rows = [...S.events, ...fills].sort((a, b) => a[0] - b[0]);
  html('#timeline', rows.map(([ts, icon, text, url]) =>
    `<li><span class="dim">${et(ts)}</span> ${icon} ${url ? `<a href="${esc(url)}" target="_blank" rel="noopener">${esc(text)}</a>` : esc(text)}</li>`).join(''));
}

function renderPnl() {
  const p = S.pnl, T = S.ticks;
  const first = S.filing ? T.find(t => t.ts > S.filing.ts) : null;
  const bh = first && T.length ? T.at(-1).price / first.price - 1 : null;
  const strat = p ? p.equity / CASH0 - 1 : null;
  html('#bh', bh == null ? '' : `strategy <span class="${strat >= 0 ? 'up' : 'down'}">${pct(strat ?? 0)}</span> vs buy &amp; hold <span class="${bh >= 0 ? 'up' : 'down'}">${pct(bh)}</span>`);
  if (!p) return html('#pnl', '<span class="dim">No trades yet.</span>');
  const rows = Object.entries(p.positions).map(([tk, q]) => `<div>${esc(tk)} ${q.qty} @ ${usd(q.avg)} · last ${usd(q.last)} · stop ${usd(q.stop)}</div>`).join('');
  html('#pnl', `<div class="kv"><span class="dim">Equity</span><b>${usd(p.equity)}</b><span class="dim">Realized</span><span class="${p.realized >= 0 ? 'up' : 'down'}">${usd(p.realized)}</span>
    <span class="dim">Unrealized</span><span class="${p.unrealized >= 0 ? 'up' : 'down'}">${usd(p.unrealized)}</span><span class="dim">Cash</span><span>${usd(p.cash)}</span></div>${rows || '<div class="dim">Flat.</div>'}`);
}

function renderNews() {
  html('#news', S.news.slice(0, 40).map(n =>
    `<li><span class="dim">${et(n.ts)} · ${esc(n.src)} · ${esc(n.tk)}</span><br><a href="${esc(n.url)}" target="_blank" rel="noopener">${esc(n.text)}</a></li>`).join('')
    || '<li class="dim">No watchlist news yet.</li>');
}

function render() {
  const last = S.ticks.at(-1);
  html('#tk', last ? `${esc(last.ticker)} ${usd(last.price)} · ${esc(last.session)} · ${et(last.ts)}` : '');
  drawChart(); renderResearch(); renderTimeline(); renderPnl(); renderNews();
}

reset();
loadRuns('live');
connect('live');
setInterval(render, 500);
</script>
</body>
</html>
```

- [ ] **Step 2: Link the radar back to the cockpit** — in `index.html`, inside `<header>` right after the `.logo` span, add:
```html
<a href="./" class="dim">← cockpit</a>
```

- [ ] **Step 3: Check it renders with fixture-shaped data**

Run: `docker compose up -d kafka && KAFKA=localhost:9092 python dashboard.py` and, in another shell, `python replay.py data/replays/<file>.jsonl --speed 600` (or Task 4's file once it exists; until then, a hand-made 10-line `.jsonl` built from `tests/fixtures/contracts.json` works).
Open `http://localhost:8080`, select the replay run.
Expected: price line draws, the 8-K marker appears, timeline shows the filing. (Research and trades appear once Tasks 8/10 run; Task 13 checks everything together.)

- [ ] **Step 4: Commit**
```bash
git add cockpit.html index.html
git commit -m "feat: trading cockpit with research card, chart, timeline and P&L"
```

---

### Task 13: Integration — offline pipeline test, compose services, e2e check, README (everyone, from 6:00)

**Files:**
- Create: `tests/test_pipeline.py`, `e2e.py`
- Modify: `docker-compose.yml`, `README.md`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the offline pipeline test** — router → research (cache) → trader, wired exactly as the services wire them, no Kafka

Create `tests/test_pipeline.py`:
```python
import json

import pytest

from common import missing_keys
from replay import schedule
from research import Researcher
from router import Router
from trader import Trader

WATCH = {"MU": {"cik": "0000723125", "names": ["Micron"]}}
T0 = 1_000_000_000_000
FILING = {"run": "", "ts": T0, "ticker": "MU", "cik": "0000723125", "form": "8-K", "items": ["2.02", "9.01"],
          "accession": "acc-1", "url": "u", "title": "MICRON TECHNOLOGY INC - 8-K"}
VERDICT = {"verdict": "PASS", "score": 4.3, "masters": {"buffett": 4.4, "munger": 3.5, "duan": 3.7, "lilu": 4.0},
           "buy_low": 140.0, "buy_high": 160.0, "target": 170.0, "red_lines": [], "summary": "s",
           "report_path": "data/reports/MU-acc-1.md", "duration_s": 300}
PRICES = [(130, 1000), (134, 1000), (138, 1000), (142, 1000), (146, 1000), (150, 1000),
          (154, 2000), (160, 1000), (170, 1000), (171, 1000)]


def test_recorded_day_flows_from_filing_to_realized_pnl(tmp_path):
    cache = tmp_path / "data" / "verdicts" / "MU-acc-1.json"
    cache.parent.mkdir(parents=True)
    cache.write_text(json.dumps(VERDICT))
    lines = [{"topic": "filings", "msg": FILING}] + [
        {"topic": "ticks", "msg": {"run": "", "ts": T0 + (i + 1) * 300_000, "ticker": "MU", "price": p,
                                   "volume": v, "session": "regular"}} for i, (p, v) in enumerate(PRICES)]

    def never(*_):
        raise AssertionError("a replay with a cached verdict must not run research")
    router, trader = Router(WATCH), Trader()
    researcher = Researcher(never, never, data_dir=tmp_path / "data", cache_delay_s=0, sleep=lambda s: None)
    out = []
    for _, topic, msg in schedule(lines, speed=1e9, run="replay-1"):
        if topic == "filings":
            req = router.on_filing(msg)
            out.append(("skill-requests", req))
            verdicts = []
            researcher.handle(req, verdicts.append)
            for v in verdicts:
                out.append(("research-verdicts", v))
                out += trader.on_verdict(v)
        else:
            out += trader.on_tick(msg)

    for topic, msg in out:
        assert missing_keys(topic, msg) == set(), topic
        assert msg["run"] == "replay-1"
    assert [(m["side"], m["rule"]) for t, m in out if t == "orders"] == [("BUY", "livermore_pivot"), ("SELL", "target_hit")]
    final = [m for t, m in out if t == "pnl"][-1]
    assert final["positions"] == {}
    assert final["realized"] == pytest.approx((170 * 0.9995 - 154 * 1.0005) * 125, abs=0.01)
```

- [ ] **Step 2: Run the whole suite**

Run: `pytest -v`
Expected: all PASS. If `test_pipeline` fails, the failing assertion names the contract or rule that broke between tracks; fix it in the owning task's file.

- [ ] **Step 3: Add the services to `docker-compose.yml`** — append under `services:` (after `dashboard:`):
```yaml
  edgar:  # SEC latest filings -> filings
    build: .
    command: python edgar.py
    restart: unless-stopped
    environment: { KAFKA: "kafka:19092", SEC_UA: "${SEC_UA:-AlphaRadar hackathon contact@example.com}" }
    depends_on: { kafka: { condition: service_healthy } }

  news:  # newswire RSS -> news
    build: .
    command: python news.py
    restart: unless-stopped
    environment: { KAFKA: "kafka:19092" }
    depends_on: { kafka: { condition: service_healthy } }

  router:  # filings -> skill-requests
    build: .
    command: python router.py
    restart: unless-stopped
    environment: { KAFKA: "kafka:19092" }
    depends_on: { kafka: { condition: service_healthy } }

  trader:  # research-verdicts + ticks -> orders, fills, pnl
    build: .
    command: python trader.py
    restart: unless-stopped
    environment: { KAFKA: "kafka:19092" }
    depends_on: { kafka: { condition: service_healthy } }
```
(`research.py` is not a compose service: it runs on the laptop with Claude Code.)

- [ ] **Step 4: Write the end-to-end check against real Kafka**

Create `e2e.py`:
```python
"""End-to-end check against the running stack: replay a recorded day and expect research, a buy, an exit, and a
flat book. Needs `docker compose up -d --build`, `python research.py` running, and a cached verdict for the file.

  python e2e.py data/replays/MU-2025-09-23.jsonl
"""
import json
import sys
import threading
import time
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

import replay
from common import KAFKA, ensure_topics, new_replay_run, producer, run_start_ms

TOPICS = ["research-verdicts", "orders", "pnl"]


def main(path):
    ensure_topics(TOPICS)
    run = new_replay_run()
    player = threading.Thread(target=replay.play, args=(Path(path), 600, producer(), run))
    player.start()
    consumer = KafkaConsumer(bootstrap_servers=KAFKA, value_deserializer=json.loads)
    tps = [TopicPartition(t, 0) for t in TOPICS]
    consumer.assign(tps)
    starts = consumer.offsets_for_times({tp: run_start_ms(run) for tp in tps})
    for tp in tps:
        consumer.seek(tp, starts[tp].offset) if starts[tp] else consumer.seek_to_end(tp)
    seen, last_pnl, deadline = {"verdict": None, "orders": []}, None, time.time() + 240
    while time.time() < deadline:
        for tp, records in consumer.poll(timeout_ms=1000).items():
            for r in records:
                m = r.value
                if m.get("run") != run:
                    continue
                if tp.topic == "research-verdicts" and m["status"] != "started":
                    seen["verdict"] = m
                elif tp.topic == "orders":
                    seen["orders"].append(f"{m['side']} {m['qty']} {m['rule']}")
                elif tp.topic == "pnl":
                    last_pnl = m
        if not player.is_alive() and last_pnl and not last_pnl["positions"] and seen["orders"]:
            break
    print(f"run {run}\nverdict: {seen['verdict'] and seen['verdict'].get('verdict')} ({seen['verdict'] and seen['verdict'].get('source')})")
    print("orders:", seen["orders"])
    print("final pnl:", last_pnl)
    ok = seen["verdict"] and seen["verdict"]["status"] == "done" and len(seen["orders"]) >= 2 and last_pnl and not last_pnl["positions"]
    print("E2E PASS" if ok else "E2E FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main(sys.argv[1])
```

- [ ] **Step 5: Run the full stack end to end**

```bash
SEC_UA="AlphaRadar hackathon you@team.dev" docker compose up -d --build
KAFKA=localhost:9092 BERKSHIRE_DIR=~/ai-berkshire python research.py &     # on the research laptop
KAFKA=localhost:9092 python e2e.py data/replays/<demo-file>.jsonl
```
Expected: `verdict: PASS (cache)`, `orders: ['BUY … livermore_pivot', 'SELL … <rule>']`, `E2E PASS`.
If the verdict was not PASS (see Task 8 Step 4), expect `orders: []` and `E2E FAIL` — use the backup day for the demo.
Then open `http://localhost:8080`, press **Replay earnings day** at 300×, and walk the demo script (spec §8) once.

Run this check after every merge from here on.

- [ ] **Step 6: Update `README.md`** — replace the title and intro, keep the HN sections below as "HN radar":

````markdown
# Alpha Radar

Real-time earnings trading on Kafka. SEC filings start an AI research team ([ai-berkshire](https://github.com/xbtlin/ai-berkshire)),
and a fast rules engine paper-trades only inside its verdict.

> Buffett decides **what**. Livermore decides **when**. The Turtles decide **how much** and **when to quit**.

```
edgar.py ─▶ filings ─▶ router.py ─▶ skill-requests ─▶ research.py (Claude Code + ai-berkshire) ─▶ research-verdicts ─┐
news.py ──▶ news                                                                                                    ▼
producer.py ▶ hn-events                                                    replay.py ─▶ ticks ─▶ trader.py ─▶ orders, fills, pnl
                                         dashboard.py: every topic ─SSE─▶ cockpit (/) and HN radar (/radar)
```

## Run

```sh
SEC_UA="AlphaRadar you@team.dev" docker compose up -d --build
KAFKA=localhost:9092 BERKSHIRE_DIR=~/ai-berkshire python research.py   # on a machine logged in to Claude Code
open http://localhost:8080        # pick a recorded day, press "Replay earnings day"
```

Record another day: `python record.py --list`, then `python record.py <TICKER> <ACCESSION> [--news news.json]`,
then `python research.py warm <TICKER> <ACCESSION>` to cache its verdict. Tests: `pytest`. Full stack: `python e2e.py data/replays/<file>.jsonl`.

Paper trading only. Not investment advice.
````

- [ ] **Step 7: Commit**
```bash
git add tests/test_pipeline.py e2e.py docker-compose.yml README.md
git commit -m "feat: wire Alpha Radar services end to end"
```
