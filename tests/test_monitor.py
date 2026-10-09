"""
Offline tests for the scraper. No network needed.

Run:  python -m unittest discover -s tests -v
"""
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scraper"))
import monitor as m  # noqa: E402


class DateTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(m.IST)
        self.cutoff = self.now - timedelta(hours=24)

    def test_date_only_from_yesterday_is_recent(self):
        """The bug that hid every official circular: date-only sources."""
        yesterday = (self.now - timedelta(days=1)).strftime("%b %d, %Y")
        self.assertEqual(m.is_within_24h(yesterday, self.cutoff), (True, True))

    def test_date_only_from_last_week_is_old(self):
        old = (self.now - timedelta(days=7)).strftime("%d-%m-%Y")
        self.assertEqual(m.is_within_24h(old, self.cutoff), (False, True))

    def test_timestamp_uses_exact_time(self):
        fresh = (self.now - timedelta(hours=2)).strftime("%a, %d %b %Y %H:%M:%S +0530")
        stale = (self.now - timedelta(hours=30)).strftime("%a, %d %b %Y %H:%M:%S +0530")
        self.assertTrue(m.is_within_24h(fresh, self.cutoff)[0])
        self.assertFalse(m.is_within_24h(stale, self.cutoff)[0])

    def test_gmt_is_not_read_as_ist(self):
        dt, has_time = m.parse_datetime_precise("Fri, 09 Oct 2026 05:31:03 GMT")
        self.assertTrue(has_time)
        self.assertEqual(dt.utcoffset(), timedelta(0))

    def test_rbi_timestamp_without_zone_is_ist(self):
        dt, has_time = m.parse_datetime_precise("Wed, 07 Oct 2026 17:40:00")
        self.assertTrue(has_time)
        self.assertEqual(dt.utcoffset(), timedelta(hours=5, minutes=30))

    def test_midnight_counts_as_date_only(self):
        self.assertFalse(m.parse_datetime_precise("Thu, 8 Oct 2026 00:00:00 +0530")[1])

    def test_unparseable_and_future(self):
        self.assertEqual(m.is_within_24h("Non-Archived", self.cutoff), (False, False))
        self.assertEqual(m.is_within_24h("", self.cutoff), (False, False))
        future = (self.now + timedelta(days=40)).strftime("%d-%m-%Y")
        self.assertEqual(m.is_within_24h(future, self.cutoff), (False, True))

    def test_no_endless_loop_on_date_lookalike(self):
        self.assertIsNone(m.parse_date_only("12 Regular 2026"))
        self.assertIsNone(m.parse_date_only("07/11/0003"))

    def test_find_date_in_text(self):
        self.assertEqual(m.find_date_in_text("Ref: X/1 Issue Date: 06-10-2026 more"), "06-10-2026")
        self.assertEqual(m.find_date_in_text("Posted October 8, 2026 by desk"), "October 8, 2026")
        self.assertEqual(m.find_date_in_text("no date here 12345"), "")


class KeywordTests(unittest.TestCase):
    def test_whole_word_matching(self):
        self.assertEqual(m.keyword_score("Material criteria for international arbitration"), 0)
        self.assertGreater(m.keyword_score("RIA norms eased; SIP and NAV rules change"), 0)

    def test_plural_not_counted_twice(self):
        kws = [k for k, _ in m.match_keywords("Mutual funds see record SIPs")]
        self.assertIn("mutual fund", kws)
        self.assertNotIn("mutual funds", kws)
        self.assertIn("sip", kws)

    def test_noise(self):
        self.assertTrue(m.is_noise("Recruitment of Grade A (Assistant Manager) 2026"))
        self.assertTrue(m.is_noise("RBI imposes monetary penalty on ABC Co-operative Bank Limited"))
        self.assertFalse(m.is_noise("SEBI revises expense ratio norms for mutual funds"))

    def test_regulator_detection(self):
        self.assertEqual(m.detect_regulator("IRDAI issues new health cover norms"), "IRDAI")
        self.assertEqual(m.detect_regulator("Irda panel on surrender value"), "IRDAI")
        self.assertEqual(m.detect_regulator("Carbide prices rise"), "MoF/Other")


RSS = b"""\xef\xbb\xbf<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel>
<item><title>SEBI tweaks mutual fund expense ratio</title><link>https://x.test/a</link>
<pubDate>%s</pubDate><description>&lt;p&gt;TER limits change&lt;/p&gt;</description>
<content:encoded><![CDATA[<p>%s</p>]]></content:encoded></item>
<item><title>Cricket score update</title><link>https://x.test/b</link><pubDate>%s</pubDate></item>
<item><title>Old income tax story</title><link>https://x.test/c</link><pubDate>Mon, 05 Jan 2015 10:00:00 +0530</pubDate></item>
</channel></rss>"""

ATOM = b"""<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
<entry><title>NPS withdrawal rules eased</title><link rel="alternate" href="https://x.test/n"/>
<published>2026-10-08T10:00:00+05:30</published><summary>PFRDA update</summary></entry></feed>"""

LISTING = b"""<html><body><table>
<tr><th>Date</th><th>Title</th></tr>
<tr><td>%s</td><td><a href="/legal/circulars/c1.html">Ease of doing investment for mutual fund investors</a></td></tr>
<tr><td>Jan 02, 2019</td><td><a href="/legal/circulars/c0.html">An old circular about demat accounts</a></td></tr>
</table>
<div class="card"><a href="/w/p1"><h2 class="t">Circular on NPS exit rules for subscribers</h2>
<div class="meta">Ref: P/1 <b>Issue Date:</b> %s</div></a></div>
<table><tr><td>1</td><td>Guidelines on pension fund charges</td><td>%s</td><td><a href="/f/doc.pdf">Download (20 KB)</a></td></tr></table>
</body></html>"""


class FakeScraper(m.SourceScraper):
    def __init__(self, payload, seen=None):
        super().__init__(seen)
        self.payload = payload

    def fetch(self, url, timeout=20):
        return 200, self.payload


class ScraperTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(m.IST)
        self.cutoff = self.now - timedelta(hours=24)
        self.fresh = (self.now - timedelta(hours=3)).strftime("%a, %d %b %Y %H:%M:%S +0530")
        self.yday = (self.now - timedelta(days=1)).strftime("%b %d, %Y")
        self.tmp = Path(tempfile.mkdtemp()) / "seen.json"

    def test_parse_feed_rss_with_bom_and_content(self):
        items = m.parse_feed(RSS % (self.fresh.encode(), b"body " * 200, self.fresh.encode()))
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0]["link"], "https://x.test/a")
        self.assertIn("body", items[0]["content"])

    def test_parse_feed_atom(self):
        items = m.parse_feed(ATOM)
        self.assertEqual(items[0]["link"], "https://x.test/n")
        self.assertTrue(items[0]["date"].startswith("2026-10-08"))

    def test_rss_source_filters_and_keeps_feed_body(self):
        src = m.Source(name="T", url="https://x.test/rss", tier="tier1_news")
        sc = FakeScraper(RSS % (self.fresh.encode(), b"body " * 200, self.fresh.encode()))
        res = sc.scrape(src, self.cutoff)
        self.assertEqual((res.status, res.fetched, res.recent, res.relevant), ("ok", 3, 2, 1))
        self.assertEqual(res.updates[0].regulator, "SEBI")
        self.assertIn("https://x.test/a", sc.feed_bodies)

    def test_listing_table_card_and_download_link(self):
        d = self.yday.encode()
        dmy = (self.now - timedelta(days=1)).strftime("%d-%m-%Y").encode()
        page = LISTING % (d, dmy, dmy)
        src = m.Source(name="L", url="https://reg.test/list", type="listing", row="table tr, .card",
                       tier="official", regulator="SEBI", lenient=True)
        res = FakeScraper(page).scrape(src, self.cutoff)
        titles = [u.title for u in res.updates]
        self.assertEqual(len(titles), 3, titles)
        self.assertIn("Guidelines on pension fund charges", titles)      # not "Download (20 KB)"
        self.assertTrue(all(u.url.startswith("https://reg.test/") for u in res.updates))
        self.assertTrue(all(u.source_tier == "official" and u.relevance in ("HIGH", "MEDIUM") for u in res.updates))

    def test_empty_page_is_flagged(self):
        src = m.Source(name="E", url="https://x.test/e", type="listing", row="table tr")
        res = FakeScraper(b"<html><body>redesigned</body></html>").scrape(src, self.cutoff)
        self.assertEqual(res.status, "empty")

    def test_frozen_feed_is_flagged_stale(self):
        feed = RSS.replace(b"%s", b"Mon, 05 Jan 2015 10:00:00 +0530")
        res = FakeScraper(feed).scrape(m.Source(name="S", url="https://x.test/s"), self.cutoff)
        self.assertEqual(res.status, "stale")

    def test_seen_store_stops_repeats_across_days(self):
        d = self.yday.encode()
        page = LISTING % (d, d, d)
        src = m.Source(name="L", url="https://reg.test/list", type="listing", row="table tr",
                       tier="official", lenient=True)
        day1 = m.SeenStore(self.tmp, self.now.date() - timedelta(days=1))
        self.assertEqual(len(FakeScraper(page, day1).scrape(src, self.cutoff - timedelta(days=1)).updates), 2)
        day1.save()
        same_day = m.SeenStore(self.tmp, self.now.date() - timedelta(days=1))
        self.assertEqual(len(FakeScraper(page, same_day).scrape(src, self.cutoff - timedelta(days=1)).updates), 2)
        day2 = m.SeenStore(self.tmp, self.now.date())
        self.assertEqual(len(FakeScraper(page, day2).scrape(src, self.cutoff).updates), 0)

    def test_dateless_source_reports_only_new_items(self):
        feed1 = b"<rss><channel><item><title>Old income tax press note</title><link>https://p.test/1</link></item></channel></rss>"
        feed2 = feed1.replace(b"<channel>", b"<channel><item><title>New small savings interest rate notified</title><link>https://p.test/2</link></item>")
        src = m.Source(name="P", url="https://p.test/rss", tier="official")
        day1 = m.SeenStore(self.tmp, self.now.date() - timedelta(days=1))
        self.assertEqual(FakeScraper(feed1, day1).scrape(src, self.cutoff).updates, [])   # first run: learn only
        day1.save()
        day2 = m.SeenStore(self.tmp, self.now.date())
        ups = FakeScraper(feed2, day2).scrape(src, self.cutoff).updates
        self.assertEqual([u.url for u in ups], ["https://p.test/2"])

    def test_source_cap(self):
        items = b"".join(
            b"<item><title>Income tax rule %d for mutual fund SIP investors</title><link>https://x.test/%d</link><pubDate>%s</pubDate></item>"
            % (i, i, self.fresh.encode()) for i in range(20))
        src = m.Source(name="C", url="https://x.test/c", max_keep=5)
        res = FakeScraper(b"<rss><channel>" + items + b"</channel></rss>").scrape(src, self.cutoff)
        self.assertEqual((res.relevant, len(res.updates)), (20, 5))


class FullTextTests(unittest.TestCase):
    def test_enrich_keeps_no_body(self):
        u = m.build_update("MoF/Other", "New rules for investors", "New rules for investors",
                           "https://x.test/a", "", "tier1_news", "T")
        body = ("SEBI said the nominee rules change from 1 January 2027. " * 3 +
                "The circular SEBI/HO/IMD/POD1/P/CIR/2026/101 covers nominee updates. " +
                "Investors must update nominee details. " * 5)
        before = u.relevance_score
        m.enrich_with_fulltext(u, body, "ok")
        self.assertEqual(u.regulator, "SEBI")
        self.assertTrue(u.circular_ref.startswith("SEBI/HO/IMD"))
        self.assertGreaterEqual(u.relevance_score, before)
        self.assertLessEqual(u.relevance_score, before + 2)
        self.assertLess(len(u.summary), 400)
        self.assertFalse(any(isinstance(v, str) and len(v) > 600 for v in m.asdict(u).values()))

    def test_article_extraction_and_paywall(self):
        html = (b"<html><head><script type='application/ld+json'>{\"@type\":\"NewsArticle\","
                b"\"isAccessibleForFree\":\"False\",\"articleBody\":\"" + b"Paid text. " * 40 + b"\"}</script></head>"
                b"<body><nav><p>menu menu menu menu menu menu menu menu menu menu</p></nav></body></html>")

        class F(m.ArticleFetcher):
            def fetch(self, url, timeout=15):
                return 200, html
        status, text = F().fetch_article("https://x.test/story")
        self.assertEqual(status, "paywalled")
        self.assertIn("Paid text", text)
        self.assertEqual(F().fetch_article("https://x.test/doc.pdf")[0], "not_html")


class ConfigTests(unittest.TestCase):
    def test_sources_yaml_loads(self):
        sources, settings = m.load_sources()
        self.assertGreater(len(sources), 50)
        self.assertEqual(len({s.name for s in sources}), len(sources))
        for s in sources:
            self.assertTrue(s.url.startswith("https://"), s.name)
        groups = {s.group for s in sources}
        for g in ("SEBI", "RBI", "CBDT", "IRDAI", "PFRDA", "AMFI", "PIB"):
            self.assertIn(g, groups)


if __name__ == "__main__":
    unittest.main()
