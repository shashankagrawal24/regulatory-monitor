"""
Indian Financial Regulatory Monitor
====================================
Daily scraper that fetches circulars, notifications, and press releases
from Indian financial regulators relevant to personal finance.

Regulators: SEBI, RBI, IRDAI, PFRDA, CBDT, AMFI, PIB/MoF
Output: data/briefings/{date}.json + data/briefings/{date}.md + data/latest.json

Run: python scraper/monitor.py
Schedule: Daily via GitHub Actions (see .github/workflows/monitor.yml)
"""

import json
import re
import os
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DATA_DIR = Path("data")
BRIEFINGS_DIR = DATA_DIR / "briefings"
LATEST_FILE = DATA_DIR / "latest.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-IN,en;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)
DATE_STR = TODAY.isoformat()

# Relevance keywords for filtering
RELEVANCE_KEYWORDS = {
    "high": [
        "mutual fund", "capital gains", "LTCG", "STCG", "TDS", "ITR",
        "NPS", "PPF", "ELSS", "tax slab", "surcharge", "indexation",
        "SGB", "sovereign gold", "insurance", "term plan", "health insurance",
        "RIA", "investment advisor", "KYC", "demat", "nominee", "SIP",
        "expense ratio", "exit load", "NAV", "NFO", "dividend",
        "repo rate", "savings account", "FD rate", "lending rate",
        "income tax", "section 80", "rebate", "standard deduction",
        "new tax regime", "old tax regime", "pension", "annuity",
        "ULIP", "surrender value", "claim settlement",
    ],
    "medium": [
        "circular", "notification", "amendment", "regulation",
        "compliance", "disclosure", "reporting", "framework",
        "investor protection", "grievance", "stock exchange",
        "NBFC", "payment bank", "digital lending", "UPI",
        "REIT", "InvIT", "AIF", "PMS", "portfolio management",
        "credit score", "CIBIL", "loan", "EMI", "moratorium",
    ],
}


# ---------------------------------------------------------------------------
# DATA MODEL
# ---------------------------------------------------------------------------
@dataclass
class RegUpdate:
    regulator: str
    title: str
    summary: str
    url: str
    date: str
    category: str           # e.g. "MF Regulation", "Taxation", "Insurance"
    relevance: str          # HIGH / MEDIUM / LOW
    circular_ref: str = ""  # e.g. SEBI/HO/IMD/...
    action_required: bool = False
    affects: str = ""       # who is affected
    nw_implication: str = ""  # Novelty Wealth action
    source_type: str = "official"  # official / news


# ---------------------------------------------------------------------------
# BASE SCRAPER
# ---------------------------------------------------------------------------
class BaseFetcher:
    def get(self, url: str, timeout: int = 20) -> Optional[str]:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            log.warning(f"  Failed: {url} — {e}")
            return None

    def soup(self, url: str) -> Optional[BeautifulSoup]:
        text = self.get(url)
        return BeautifulSoup(text, "html.parser") if text else None

    def parse_rss(self, url: str) -> list[dict]:
        """Parse RSS/Atom feed and return list of {title, link, date, description}."""
        text = self.get(url)
        if not text:
            return []

        items = []
        try:
            root = ET.fromstring(text)
            # Handle both RSS and Atom
            ns = {"atom": "http://www.w3.org/2005/Atom"}

            # RSS 2.0
            for item in root.findall(".//item"):
                title = item.findtext("title", "").strip()
                link = item.findtext("link", "").strip()
                pub_date = item.findtext("pubDate", "").strip()
                desc = item.findtext("description", "").strip()
                items.append({"title": title, "link": link, "date": pub_date, "description": desc})

            # Atom
            for entry in root.findall(".//atom:entry", ns):
                title = entry.findtext("atom:title", "", ns).strip()
                link_el = entry.find("atom:link", ns)
                link = link_el.get("href", "") if link_el is not None else ""
                pub_date = entry.findtext("atom:updated", "", ns).strip()
                desc = entry.findtext("atom:summary", "", ns).strip()
                items.append({"title": title, "link": link, "date": pub_date, "description": desc})

        except ET.ParseError as e:
            log.warning(f"  RSS parse error: {e}")

        return items


# ---------------------------------------------------------------------------
# SEBI SCRAPER
# ---------------------------------------------------------------------------
class SEBIScraper(BaseFetcher):
    CIRCULARS_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=2&smid=0"
    PRESS_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=3&ssid=0&smid=0"
    BASE = "https://www.sebi.gov.in"

    def scrape(self) -> list[RegUpdate]:
        log.info("Scraping SEBI...")
        updates = []

        # Scrape circulars listing page
        s = self.soup(self.CIRCULARS_URL)
        if s:
            updates.extend(self._parse_listing(s, "circular"))

        # Scrape press releases
        s = self.soup(self.PRESS_URL)
        if s:
            updates.extend(self._parse_listing(s, "press_release"))

        log.info(f"  SEBI: found {len(updates)} items")
        return updates

    def _parse_listing(self, soup: BeautifulSoup, doc_type: str) -> list[RegUpdate]:
        results = []
        rows = soup.select("table tr") or soup.select(".listingTable tr")

        for row in rows[1:20]:  # first 20 items
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            date_text = cells[0].get_text(strip=True) if cells else ""
            link_el = row.find("a")
            if not link_el:
                continue

            title = link_el.get_text(strip=True)
            href = link_el.get("href", "")
            if href and not href.startswith("http"):
                href = urljoin(self.BASE, href)

            # Check if recent (last 48 hours to account for timezone)
            if self._is_recent(date_text):
                relevance, category = self._classify(title)
                if relevance:
                    # Extract circular reference
                    circ_ref = ""
                    ref_match = re.search(r'SEBI/HO/[\w/\-]+', title)
                    if ref_match:
                        circ_ref = ref_match.group()

                    results.append(RegUpdate(
                        regulator="SEBI",
                        title=title[:200],
                        summary=title[:300],
                        url=href,
                        date=date_text,
                        category=category,
                        relevance=relevance,
                        circular_ref=circ_ref,
                        source_type="official",
                    ))
        return results

    def _is_recent(self, date_text: str) -> bool:
        for fmt in ["%b %d, %Y", "%d %b %Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"]:
            try:
                d = datetime.strptime(date_text.strip(), fmt).date()
                return d >= YESTERDAY
            except ValueError:
                continue
        return True  # if can't parse, include it (let downstream filter)

    def _classify(self, text: str) -> tuple:
        text_lower = text.lower()
        for kw in RELEVANCE_KEYWORDS["high"]:
            if kw.lower() in text_lower:
                cat = self._categorize(text_lower)
                return "HIGH", cat
        for kw in RELEVANCE_KEYWORDS["medium"]:
            if kw.lower() in text_lower:
                cat = self._categorize(text_lower)
                return "MEDIUM", cat
        return "LOW", "General"

    def _categorize(self, text: str) -> str:
        if any(w in text for w in ["mutual fund", "nfo", "expense ratio", "sip", "nav"]):
            return "MF Regulation"
        if any(w in text for w in ["tax", "tds", "capital gains", "ltcg", "stcg"]):
            return "Taxation"
        if any(w in text for w in ["ria", "investment advi"]):
            return "RIA Compliance"
        if any(w in text for w in ["insurance", "ulip", "term plan"]):
            return "Insurance"
        if any(w in text for w in ["nps", "pension"]):
            return "Pension"
        return "Capital Markets"


# ---------------------------------------------------------------------------
# RBI SCRAPER
# ---------------------------------------------------------------------------
class RBIScraper(BaseFetcher):
    PRESS_URL = "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"
    NOTIF_URL = "https://www.rbi.org.in/Scripts/NotificationUser.aspx"
    RSS_URL = "https://www.rbi.org.in/pressreleases_rss.xml"
    BASE = "https://www.rbi.org.in"

    def scrape(self) -> list[RegUpdate]:
        log.info("Scraping RBI...")
        updates = []

        # Try RSS first
        rss_items = self.parse_rss(self.RSS_URL)
        for item in rss_items[:15]:
            relevance, category = self._classify(item["title"])
            if relevance:
                updates.append(RegUpdate(
                    regulator="RBI",
                    title=item["title"][:200],
                    summary=item.get("description", item["title"])[:300],
                    url=item["link"],
                    date=item.get("date", DATE_STR),
                    category=category,
                    relevance=relevance,
                    source_type="official",
                ))

        # Scrape notifications page
        s = self.soup(self.NOTIF_URL)
        if s:
            for link in s.select("a[href*='Notification']")[:15]:
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if href and not href.startswith("http"):
                    href = urljoin(self.BASE, href)
                relevance, category = self._classify(title)
                if relevance and title:
                    updates.append(RegUpdate(
                        regulator="RBI",
                        title=title[:200],
                        summary=title[:300],
                        url=href,
                        date=DATE_STR,
                        category=category,
                        relevance=relevance,
                        source_type="official",
                    ))

        log.info(f"  RBI: found {len(updates)} items")
        return updates

    def _classify(self, text: str) -> tuple:
        text_lower = text.lower()
        for kw in RELEVANCE_KEYWORDS["high"]:
            if kw.lower() in text_lower:
                return "HIGH", self._cat(text_lower)
        for kw in RELEVANCE_KEYWORDS["medium"]:
            if kw.lower() in text_lower:
                return "MEDIUM", self._cat(text_lower)
        return None, None

    def _cat(self, t):
        if any(w in t for w in ["repo", "rate", "monetary", "interest"]):
            return "Monetary Policy"
        if any(w in t for w in ["lending", "loan", "nbfc"]):
            return "Lending/NBFC"
        if any(w in t for w in ["upi", "payment", "digital"]):
            return "Payments"
        if any(w in t for w in ["saving", "deposit", "fd"]):
            return "Deposits"
        return "Banking Regulation"


# ---------------------------------------------------------------------------
# PFRDA SCRAPER
# ---------------------------------------------------------------------------
class PFRDAScraper(BaseFetcher):
    URL = "https://www.pfrda.org.in/index1.cshtml?lsid=1063"
    BASE = "https://www.pfrda.org.in"

    def scrape(self) -> list[RegUpdate]:
        log.info("Scraping PFRDA...")
        updates = []
        s = self.soup(self.URL)
        if s:
            for link in s.select("a")[:20]:
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if href and not href.startswith("http"):
                    href = urljoin(self.BASE, href)
                text_lower = title.lower()
                if any(kw in text_lower for kw in ["nps", "pension", "subscriber", "annuity", "withdrawal", "tier"]):
                    updates.append(RegUpdate(
                        regulator="PFRDA",
                        title=title[:200],
                        summary=title[:300],
                        url=href,
                        date=DATE_STR,
                        category="Pension/NPS",
                        relevance="MEDIUM",
                        source_type="official",
                    ))
        log.info(f"  PFRDA: found {len(updates)} items")
        return updates


# ---------------------------------------------------------------------------
# NEWS SCRAPER (backup sourcing via news sites)
# ---------------------------------------------------------------------------
class NewsScraper(BaseFetcher):
    """Scrapes financial news sites for regulatory updates."""

    FEEDS = {
        "Mint": "https://www.livemint.com/rss/money",
        "ET_MF": "https://economictimes.indiatimes.com/wealth/mutual-funds/rssfeeds/46267806.cms",
        "ET_Tax": "https://economictimes.indiatimes.com/wealth/tax/rssfeeds/46266529.cms",
        "ET_Invest": "https://economictimes.indiatimes.com/wealth/invest/rssfeeds/46267805.cms",
        "Moneycontrol": "https://www.moneycontrol.com/rss/MCtopnews.xml",
    }

    REGULATORY_KEYWORDS = [
        "SEBI", "RBI", "IRDAI", "PFRDA", "CBDT", "AMFI",
        "circular", "notification", "regulation", "mandate",
        "income tax", "capital gains", "mutual fund rule",
        "repo rate", "insurance regulation", "NPS rule",
        "tax slab", "TDS", "budget", "gazette",
    ]

    def scrape(self) -> list[RegUpdate]:
        log.info("Scraping news feeds...")
        updates = []

        for source_name, feed_url in self.FEEDS.items():
            items = self.parse_rss(feed_url)
            for item in items[:10]:
                title = item.get("title", "")
                desc = item.get("description", "")
                combined = f"{title} {desc}".lower()

                if any(kw.lower() in combined for kw in self.REGULATORY_KEYWORDS):
                    relevance = "HIGH" if any(kw.lower() in combined for kw in RELEVANCE_KEYWORDS["high"]) else "MEDIUM"
                    category = self._categorize(combined)

                    updates.append(RegUpdate(
                        regulator=self._extract_regulator(combined),
                        title=title[:200],
                        summary=desc[:300] if desc else title[:300],
                        url=item.get("link", ""),
                        date=item.get("date", DATE_STR),
                        category=category,
                        relevance=relevance,
                        source_type="news",
                    ))

        log.info(f"  News: found {len(updates)} regulatory items")
        return updates

    def _extract_regulator(self, text: str) -> str:
        for reg in ["SEBI", "RBI", "IRDAI", "IRDA", "PFRDA", "CBDT", "AMFI"]:
            if reg.lower() in text.lower():
                return reg.replace("IRDA", "IRDAI")
        return "MoF/Other"

    def _categorize(self, text: str) -> str:
        if any(w in text for w in ["mutual fund", "nfo", "expense", "sip"]):
            return "MF Regulation"
        if any(w in text for w in ["tax", "tds", "capital gain", "itr"]):
            return "Taxation"
        if any(w in text for w in ["insurance", "irdai", "term plan", "health"]):
            return "Insurance"
        if any(w in text for w in ["nps", "pension", "pfrda"]):
            return "Pension/NPS"
        if any(w in text for w in ["repo", "rate cut", "rate hike", "monetary"]):
            return "Monetary Policy"
        return "Regulatory Update"


# ---------------------------------------------------------------------------
# DEDUPLICATOR
# ---------------------------------------------------------------------------
def dedup(updates: list[RegUpdate]) -> list[RegUpdate]:
    """Remove duplicate items based on title similarity."""
    seen = set()
    unique = []
    for u in updates:
        key = re.sub(r'[^a-z0-9]', '', u.title.lower())[:60]
        if key not in seen and len(key) > 10:
            seen.add(key)
            unique.append(u)
    return unique


# ---------------------------------------------------------------------------
# MARKDOWN FORMATTER
# ---------------------------------------------------------------------------
def format_briefing_md(updates: list[RegUpdate]) -> str:
    lines = []
    lines.append(f"# Regulatory Intelligence Briefing — {DATE_STR}")
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M IST')}*")
    lines.append(f"*Source: Novelty Wealth Regulatory Monitor*\n")

    if not updates:
        lines.append("## No material regulatory updates in the last 24 hours relevant to personal finance.\n")
        return "\n".join(lines)

    # Summary counts
    high = [u for u in updates if u.relevance == "HIGH"]
    med = [u for u in updates if u.relevance == "MEDIUM"]
    low = [u for u in updates if u.relevance == "LOW"]

    lines.append("## Summary\n")
    lines.append(f"- 🔴 **High Priority**: {len(high)} items")
    lines.append(f"- 🟡 **Medium Priority**: {len(med)} items")
    lines.append(f"- 🟢 **Low Priority**: {len(low)} items\n")

    # Summary table
    lines.append("## Updates\n")
    lines.append("| # | Regulator | Update | Category | Relevance | Source |")
    lines.append("|---|-----------|--------|----------|-----------|--------|")

    for i, u in enumerate(updates, 1):
        icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(u.relevance, "⚪")
        src = "Official" if u.source_type == "official" else "News"
        title_short = u.title[:80] + ("..." if len(u.title) > 80 else "")
        lines.append(f"| {i} | {u.regulator} | {title_short} | {u.category} | {icon} {u.relevance} | {src} |")

    lines.append("")

    # Detailed briefs
    lines.append("## Detailed Briefs\n")
    for i, u in enumerate(updates, 1):
        icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(u.relevance, "⚪")
        lines.append(f"### {i}. [{u.regulator}] {u.title[:120]}\n")
        lines.append(f"**Relevance**: {icon} {u.relevance} | **Category**: {u.category}")
        if u.circular_ref:
            lines.append(f"**Circular Ref**: {u.circular_ref}")
        lines.append(f"\n{u.summary}\n")
        lines.append(f"**Source**: [{u.source_type}]({u.url})\n")
        lines.append("---\n")

    # Closing
    lines.append("## Regulatory Pulse\n")
    lines.append(f"- 🔴 High: {len(high)} | 🟡 Medium: {len(med)} | 🟢 Low: {len(low)}")
    if high:
        lines.append(f"- **Top action**: Review {high[0].regulator} update on {high[0].title[:60]}")
    lines.append(f"\n*Built by Novelty Wealth (SEBI RIA: INA000019415)*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MAIN ORCHESTRATOR
# ---------------------------------------------------------------------------
class RegulatoryMonitor:
    def __init__(self):
        self.all_updates: list[RegUpdate] = []

    def run(self):
        log.info("=" * 60)
        log.info(f"Regulatory Monitor — {DATE_STR}")
        log.info("=" * 60)

        # Scrape all sources
        scrapers = [
            SEBIScraper(),
            RBIScraper(),
            PFRDAScraper(),
            NewsScraper(),
        ]

        for scraper in scrapers:
            try:
                items = scraper.scrape()
                self.all_updates.extend(items)
            except Exception as e:
                log.error(f"Scraper failed: {scraper.__class__.__name__} — {e}")

        # Dedup
        self.all_updates = dedup(self.all_updates)

        # Sort: HIGH first, then MEDIUM, then LOW
        order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        self.all_updates.sort(key=lambda u: (order.get(u.relevance, 3), u.regulator))

        log.info(f"Total unique updates: {len(self.all_updates)}")

        # Write outputs
        self._write_outputs()

        log.info("=" * 60)
        high_count = sum(1 for u in self.all_updates if u.relevance == "HIGH")
        log.info(f"Done. {len(self.all_updates)} updates, {high_count} high-priority.")
        log.info("=" * 60)

    def _write_outputs(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        BRIEFINGS_DIR.mkdir(parents=True, exist_ok=True)

        # JSON
        output = {
            "date": DATE_STR,
            "generated": datetime.now().isoformat(),
            "total": len(self.all_updates),
            "high_priority": sum(1 for u in self.all_updates if u.relevance == "HIGH"),
            "updates": [asdict(u) for u in self.all_updates],
        }

        # Latest
        with open(LATEST_FILE, "w") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        # Daily snapshot
        daily_json = BRIEFINGS_DIR / f"{DATE_STR}.json"
        with open(daily_json, "w") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        # Markdown briefing
        md = format_briefing_md(self.all_updates)
        daily_md = BRIEFINGS_DIR / f"{DATE_STR}.md"
        with open(daily_md, "w") as f:
            f.write(md)

        log.info(f"  Written: {LATEST_FILE}")
        log.info(f"  Written: {daily_json}")
        log.info(f"  Written: {daily_md}")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    monitor = RegulatoryMonitor()
    monitor.run()
