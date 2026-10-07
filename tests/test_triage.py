from common import missing_keys
from research import check_request
from triage import COOLDOWN_MS, Triage

WATCH = {"MU": {"cik": "0000723125", "names": ["Micron"]}, "NVDA": {"cik": "0001045810", "names": ["NVIDIA"]}}
VERDICT = {"run": "live", "ts": 1, "request_id": "MU-0000723125-25-000041", "ticker": "MU", "skill": "earnings-review",
           "tier": "quick", "status": "done", "source": "live", "verdict": "PASS", "score": 4.3,
           "masters": {"buffett": 4.4, "munger": 3.5, "duan": 3.7, "lilu": 4.0}, "buy_low": 140.0, "buy_high": 160.0,
           "target": 195.0, "red_lines": ["gross margin below 40%"], "summary": "s", "report_path": None, "duration_s": 60}
T0 = 1_000_000_000_000


def news(ts=T0, headline="Micron lowers fiscal Q1 guidance", url="https://example.com/a", ticker="MU", run="live"):
    return {"run": run, "ts": ts, "ticker": ticker, "source": "prnewswire", "headline": headline, "url": url}


def material(headline, verdict):
    return {"material": True, "direction": "negative", "reason": "guidance cut"}


def triage(classify=material):
    calls = []

    def spy(headline, verdict):
        calls.append(headline)
        return classify(headline, verdict)
    t = Triage(WATCH, spy)
    t.on_verdict(VERDICT)
    return t, calls


def topics(out):
    return [topic for topic, _ in out]


def test_material_news_on_a_researched_ticker_requests_a_recheck():
    t, _ = triage()
    out = t.on_news(news())
    assert topics(out) == ["news-triage", "skill-requests"]
    decision, req = out[0][1], out[1][1]
    assert decision["material"] is True and decision["request_id"] == req["request_id"]
    assert missing_keys("news-triage", decision) == set() and missing_keys("skill-requests", req) == set()
    assert (req["skill"], req["args"], req["run"], req["ts"]) == ("news-recheck", "MU 3d", "live", T0)
    assert req["prior"]["buy_low"] == 140.0
    check_request(req)  # research.py accepts exactly this shape
    assert "lowers" not in req["args"]  # the headline is not passed on


def test_immaterial_news_is_logged_but_not_rechecked():
    t, _ = triage(lambda h, v: {"material": False, "direction": "neutral", "reason": "routine PR"})
    out = t.on_news(news())
    assert topics(out) == ["news-triage"] and out[0][1]["request_id"] is None


def test_tickers_without_a_verdict_are_not_classified():
    t, calls = triage()
    assert t.on_news(news(ticker="NVDA", headline="NVIDIA news")) == [] and calls == []


def test_one_recheck_per_ticker_per_cooldown():
    t, calls = triage()
    t.on_news(news())
    t.on_verdict({**VERDICT, "tier": "news", "status": "done"})  # the re-check reported back
    assert t.on_news(news(ts=T0 + 60_000, url="https://example.com/b")) == []
    assert topics(t.on_news(news(ts=T0 + COOLDOWN_MS + 1, url="https://example.com/c"))) == ["news-triage", "skill-requests"]
    assert len(calls) == 2


def test_no_recheck_while_research_is_running():
    t, calls = triage()
    t.on_verdict({**VERDICT, "tier": "deep", "status": "started"})
    assert t.on_news(news()) == [] and calls == []
    t.on_verdict({**VERDICT, "tier": "deep", "status": "done"})
    assert topics(t.on_news(news(url="https://example.com/z"))) == ["news-triage", "skill-requests"]


def test_the_same_headline_is_classified_once():
    t, calls = triage(lambda h, v: {"material": False, "direction": "neutral", "reason": "x"})
    t.on_news(news())
    assert t.on_news(news()) == [] and len(calls) == 1


def test_hn_stories_are_matched_to_tickers():
    t, _ = triage()
    out = t.on_hn({"kind": "new", "run": "live", "ts": T0, "item": {"id": 7, "type": "story", "title": "Micron recalls HBM chips"}})
    assert out[0][1]["source"] == "HN" and out[0][1]["url"] == "https://news.ycombinator.com/item?id=7"


def test_classifier_errors_do_not_trigger_a_recheck():
    def broken(h, v):
        raise ValueError("bad json")
    t, _ = triage(broken)
    out = t.on_news(news())
    assert topics(out) == ["news-triage"] and out[0][1]["material"] is False
