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
