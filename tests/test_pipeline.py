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
