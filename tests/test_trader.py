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
