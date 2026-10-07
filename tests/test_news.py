from datetime import datetime, timezone

from common import missing_keys
from news import parse_rss

WATCH = {"MU": {"cik": "0000723125", "names": ["Micron"]}}
RSS = b"""<?xml version="1.0"?><rss version="2.0"><channel>
<item><title>Micron Technology, Inc. Reports Results for the Fourth Quarter</title>
<link>https://example.com/mu-q4</link><pubDate>Tue, 23 Sep 2025 20:01:00 GMT</pubDate></item>
<item><title>Acme Corp Declares Quarterly Dividend</title>
<link>https://example.com/acme</link><pubDate>Tue, 23 Sep 2025 20:02:00 GMT</pubDate></item>
</channel></rss>"""


def test_matched_and_unmatched_headlines():
    mu, other = parse_rss(RSS, "globenewswire", WATCH)
    assert mu == {"run": "live", "ts": int(datetime(2025, 9, 23, 20, 1, tzinfo=timezone.utc).timestamp() * 1000),
                  "ticker": "MU", "source": "globenewswire",
                  "headline": "Micron Technology, Inc. Reports Results for the Fourth Quarter",
                  "url": "https://example.com/mu-q4"}
    assert other["ticker"] == "_"
    assert missing_keys("news", mu) == set() and missing_keys("news", other) == set()
