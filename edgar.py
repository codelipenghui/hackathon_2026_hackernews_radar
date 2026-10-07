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
