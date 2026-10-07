"""Turn events into research requests: an earnings filing for a watchlist company -> ai-berkshire /earnings-review.

Rule R1 of the spec. One request per ticker per run: the 8-K earnings release usually lands before the 10-Q for the
same quarter, and a second deep-research run would cost minutes and tokens for nothing. A new replay is a new run,
so replaying the same filing triggers again (and research.py serves it from cache).
"""
from datetime import datetime, timezone

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
        # anchor research to the filing, so a replay of an August report is judged with August information
        filed = datetime.fromtimestamp(f["ts"] / 1000, timezone.utc).date()
        return {"run": f["run"], "ts": f["ts"], "request_id": f"{ticker}-{f['accession']}", "ticker": ticker,
                "skill": "earnings-review", "args": f"{ticker} earnings filed {filed}",
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
