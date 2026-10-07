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
        if book.bars[ticker] and t["ts"] <= book.bars[ticker][-1]["ts"]:
            return []  # a bar we already have (e.g. the live price feed restarted and re-sent its session)
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
