"""Publish live 5-minute bars for the watchlist to topic `ticks` (run "live"), polled from yfinance.

Same bar size as the recorded replays, so the trader's rules (6 bars = the first 30 minutes) mean the same thing live.
Only completed bars are published, each once; the first poll sends the current (or last) session so far, so the
cockpit has a chart even when the market is closed. yfinance data is delayed and unofficial: fine for paper trading.
"""
import time

from common import LIVE, emit, load_watchlist, now_ms, producer
from record import ticks_from_bars

BAR_MS = 5 * 60 * 1000
POLL_S = 60


def completed_ticks(bars, ticker, now_ms, after_ts):
    """Live ticks for bars that have closed by now_ms and are newer than after_ts (None = all of them)."""
    return [{**t, "run": LIVE} for t in ticks_from_bars(bars, ticker)
            if t["ts"] + BAR_MS <= now_ms and (after_ts is None or t["ts"] > after_ts)]


def fetch_session(ticker):
    import yfinance as yf

    df = yf.Ticker(ticker).history(period="1d", interval="5m", prepost=True)
    return [(when.to_pydatetime(), row.Close, row.Volume) for when, row in df.iterrows()]


def main():
    tickers, prod, last = list(load_watchlist()), producer(), {}
    while True:
        for ticker in tickers:
            try:
                ticks = completed_ticks(fetch_session(ticker), ticker, now_ms(), last.get(ticker))
            except Exception as e:
                print(f"prices {ticker}: {e!r}")
                continue
            for t in ticks:
                emit(prod, "ticks", ticker, t)
            if ticks:
                last[ticker] = ticks[-1]["ts"]
                print(f"prices {ticker}: {len(ticks)} bars, last {ticks[-1]['price']}")
        prod.flush()
        time.sleep(POLL_S)


if __name__ == "__main__":
    main()
