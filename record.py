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
