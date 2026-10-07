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
    "prnewswire-earnings": "https://www.prnewswire.com/rss/financial-services-latest-news/earnings-list.rss",
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
