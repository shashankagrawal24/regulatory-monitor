"""
Indian Financial Regulatory Monitor v2
=======================================
Daily scraper for personal finance regulatory updates.

HARD RULES:
  1. Only items published in last 24 hours
  2. Only personal finance / macro economy relevant
  3. No IPO filings, company-specific, administrative/procedural rules
  4. Clear actionable titles

Regulators: SEBI, RBI, IRDAI, PFRDA, CBDT, AMFI
News: Mint, ET Wealth, Moneycontrol RSS
Output: data/briefings/{date}.json + .md + data/latest.json
"""

import json
import re
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass, asdict
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

IST = timezone(timedelta(hours=5, minutes=30))
NOW = datetime.now(IST)
TODAY = NOW.date()
YESTERDAY = TODAY - timedelta(days=1)
CUTOFF = NOW - timedelta(hours=24)
DATE_STR = TODAY.isoformat()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# NOISE EXCLUSION — hard blocklist for irrelevant content
# ---------------------------------------------------------------------------
NOISE_PATTERNS = [
    # IPO / company filings (not personal finance)
    r"(?i)\b(limited|ltd|enterprises|industries|technologies|capital ltd)\b.*(?:drhp|prospectus|public.?issue)",
    r"(?i)^[A-Z\s]+(LIMITED|LTD)\s*$",  # bare company names like "RENTOMOJO LIMITED"
    r"(?i)\bfiling.*public.?issue",
    r"(?i)\bdraft.?red.?herring",
    r"(?i)\bIPO\b.*\b(filing|offer|document)\b",

    # Administrative / procedural rules (not actionable for investors)
    r"(?i)\b(salaries|allowances|conditions of service|chairman and members)\b",
    r"(?i)\b(appeal to central government|procedure rules|annual report rules)\b",
    r"(?i)\b(form of annual statement|company law board)\b",
    r"(?i)\b(holding inquiry and imposing penalties)\b",
    r"(?i)\b(appellate tribunal).*\b(procedure|salaries|rules)\b",
    r"(?i)\b(depositories act).*\b(appeal|procedure)\b",

    # NFO filings (low relevance unless it's a new category)
    r"(?i)^(invesco|hdfc|icici|sbi|axis|kotak|nippon|dsp|tata|aditya)\s.*\b(fund)\b$",

    # Company-specific orders
    r"(?i)\b(adjudication order|consent order|settlement order)\b.*(?:limited|ltd)",

    # Counterfeit notes, currency management (not personal finance)
    r"(?i)\bcounterfeit\b",
    r"(?i)\bcurrency distribution\b",
    r"(?i)\bcurrency chest\b",

    # Generic navigation links scraped by accident
    r"(?i)^(notifications|circulars|draft notifications|guidelines|circulars withdrawn)$",
    r"(?i)^(rules|regulations|acts|orders|press releases)$",
    r"(?i)^(master directions|master circulars)$",
]

# ---------------------------------------------------------------------------
# RELEVANCE — what Novelty Wealth's ICP cares about
# ---------------------------------------------------------------------------
PERSONAL_FINANCE_KEYWORDS = [
    # Tax
    "income tax", "capital gains", "LTCG", "STCG", "TDS", "ITR",
    "section 80", "tax slab", "surcharge", "rebate", "standard deduction",
    "new tax regime", "old tax regime", "tax saving", "ELSS", "form 15",
    "form 121", "advance tax", "tax audit", "HRA", "indexation",

    # Investments
    "mutual fund", "SIP", "NFO", "expense ratio", "exit load", "NAV",
    "stock market", "demat", "equity", "debt fund", "hybrid fund",
    "ETF", "index fund", "SGB", "sovereign gold", "PPF", "EPF",
    "NPS", "pension", "annuity", "small savings", "FD rate",
    "savings account", "SCSS", "KVP", "NSC", "sukanya",
    "REIT", "InvIT", "AIF", "PMS", "portfolio management",

    # Insurance
    "insurance", "term plan", "health insurance", "ULIP",
    "claim settlement", "surrender value", "IRDAI", "premium",

    # Banking / Macro
    "repo rate", "rate cut", "rate hike", "monetary policy",
    "inflation", "GDP", "fiscal deficit", "RBI policy",
    "lending rate", "MCLR", "base rate", "credit score",
    "CIBIL", "digital lending", "UPI", "KYC",
    "loan", "EMI", "moratorium", "interest rate",

    # Regulatory for retail
    "investor protection", "nominee", "KYC", "RIA",
    "investment advisor", "financial planning", "fintech",
    "disclosure", "AMFI", "MF distributor",
]

MACRO_KEYWORDS = [
    "economy", "GDP", "inflation", "fiscal", "budget", "tariff",
    "trade war", "global market", "recession", "liquidity",
    "FII", "FPI", "dollar", "rupee", "crude oil",
    "employment", "manufacturing", "services PMI", "CPI", "WPI",
]


def is_noise(title: str) -> bool:
    """Returns True if the item should be excluded."""
    for pattern in NOISE_PATTERNS:
        if re.search(pattern, title):
            return True
    return False


def is_relevant(title: str, description: str = "") -> bool:
    """Returns True if the item is relevant to personal finance / macro."""
    combined = f"{title} {description}".lower()
    for kw in PERSONAL_FINANCE_KEYWORDS + MACRO_KEYWORDS:
        if kw.lower() in combined:
            return True
    return False


def score_relevance(title: str, description: str = "") -> str:
    """Score as HIGH / MEDIUM / LOW."""
    combined = f"{title} {description}".lower()

    high_triggers = [
        "income tax", "capital gains", "TDS", "tax slab", "form 15", "form 121",
        "mutual fund", "SIP", "expense ratio", "exit load",
        "repo rate", "rate cut", "rate hike",
        "NPS", "pension", "PPF", "SCSS",
        "insurance", "term plan", "health insurance",
        "savings account", "FD rate", "lending rate",
        "new tax regime", "LTCG", "STCG", "indexation",
        "investor protection", "KYC", "nominee",
        "SGB", "sovereign gold", "small savings",
    ]

    for kw in high_triggers:
        if kw.lower() in combined:
            return "HIGH"

    medium_triggers = [
        "circular", "notification", "amendment", "regulation",
        "credit score", "CIBIL", "digital lending", "UPI",
        "NBFC", "inflation", "GDP", "fiscal", "budget",
        "liquidity", "monetary policy", "loan", "EMI",
    ]

    for kw in medium_triggers:
        if kw.lower() in combined:
            return "MEDIUM"

    return "LOW"


def categorize(title: str, description: str = "") -> str:
    """Assign a reader-friendly category."""
    t = f"{title} {description}".lower()
    if any(w in t for w in ["income tax", "tds", "capital gain", "itr", "tax slab", "form 15", "form 121", "80c", "80d"]):
        return "Taxation"
    if any(w in t for w in ["mutual fund", "nfo", "expense ratio", "sip", "nav", "amfi", "mf "]):
        return "Mutual Funds"
    if any(w in t for w in ["repo rate", "rate cut", "rate hike", "monetary", "mpc", "inflation"]):
        return "Rates & Monetary Policy"
    if any(w in t for w in ["insurance", "irdai", "term plan", "health insurance", "ulip", "claim"]):
        return "Insurance"
    if any(w in t for w in ["nps", "pension", "pfrda", "annuity", "tier"]):
        return "Pension & NPS"
    if any(w in t for w in ["saving", "fd ", "fixed deposit", "deposit rate", "savings account"]):
        return "Deposits & Savings"
    if any(w in t for w in ["credit", "cibil", "loan", "emi", "lending", "nbfc"]):
        return "Credit & Lending"
    if any(w in t for w in ["kyc", "nominee", "demat", "investor protection", "ria", "advisor"]):
        return "Investor Protection"
    if any(w in t for w in ["sgb", "ppf", "epf", "scss", "small saving", "nsc", "kvp"]):
        return "Govt Schemes"
    if any(w in t for w in ["gdp", "economy", "fiscal", "budget", "trade", "tariff", "rupee", "dollar"]):
        return "Macro & Economy"
    if any(w in t for w in ["stock", "equity", "market", "sebi", "trading"]):
        return "Capital Markets"
    return "Regulatory Update"


# ---------------------------------------------------------------------------
# DATA MODEL
# ---------------------------------------------------------------------------
@dataclass
class RegUpdate:
    regulator: str
    title: str
    summary: str
    url: str
    pub_date: str           # publication date from source
    category: str
    relevance: str          # HIGH / MEDIUM / LOW
    source_type: str        # official / news
    circular_ref: str = ""
    action_required: bool = False


# ---------------------------------------------------------------------------
# DATE PARSING
# ---------------------------------------------------------------------------
def parse_date(text: str) -> Optional[date]:
    """Try multiple date formats and return a date object."""
    text = text.strip()
    formats = [
        "%b %d, %Y", "%d %b %Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d",
        "%B %d, %Y", "%d %B %Y", "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z", "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ", "%d %b, %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    # Try partial match: extract "Apr 07, 2026" etc from longer text
    m = re.search(r'(\w{3,9}\s+\d{1,2},?\s+\d{4})', text)
    if m:
        for fmt in ["%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y"]:
            try:
                return datetime.strptime(m.group(1), fmt).date()
            except ValueError:
                continue

    return None


def is_within_24h(date_text: str) -> bool:
    """HARD GUARDRAIL: only items from last 24 hours."""
    d = parse_date(date_text)
    if d is None:
        return False  # if can't parse date, EXCLUDE (strict)
    return d >= YESTERDAY


# ---------------------------------------------------------------------------
# BASE FETCHER
# ---------------------------------------------------------------------------
class BaseFetcher:
    def get(self, url: str, timeout: int = 20) -> Optional[str]:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            log.warning(f"  Failed: {url} -- {e}")
            return None

    def soup(self, url: str) -> Optional[BeautifulSoup]:
        text = self.get(url)
        return BeautifulSoup(text, "html.parser") if text else None

    def parse_rss(self, url: str) -> list[dict]:
        text = self.get(url)
        if not text:
            return []
        items = []
        try:
            root = ET.fromstring(text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            for item in root.findall(".//item"):
                items.append({
                    "title": (item.findtext("title") or "").strip(),
                    "link": (item.findtext("link") or "").strip(),
                    "date": (item.findtext("pubDate") or "").strip(),
                    "description": (item.findtext("description") or "").strip(),
                })
            for entry in root.findall(".//atom:entry", ns):
                link_el = entry.find("atom:link", ns)
                items.append({
                    "title": (entry.findtext("atom:title", "", ns)).strip(),
                    "link": link_el.get("href", "") if link_el is not None else "",
                    "date": (entry.findtext("atom:updated", "", ns)).strip(),
                    "description": (entry.findtext("atom:summary", "", ns)).strip(),
                })
        except ET.ParseError as e:
            log.warning(f"  RSS parse error: {e}")
        return items


# ---------------------------------------------------------------------------
# SEBI SCRAPER — circulars only (not rules, not filings)
# ---------------------------------------------------------------------------
class SEBIScraper(BaseFetcher):
    # Only circulars (sid=1, ssid=1) — these are the actionable ones
    CIRCULARS_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=1&smid=0"
    PRESS_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=3&ssid=0&smid=0"
    BASE = "https://www.sebi.gov.in"

    def scrape(self) -> list[RegUpdate]:
        log.info("Scraping SEBI circulars...")
        updates = []

        for url, label in [(self.CIRCULARS_URL, "circular"), (self.PRESS_URL, "press")]:
            s = self.soup(url)
            if not s:
                continue

            rows = s.select("table tr, .listingTable tr")
            for row in rows[:30]:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                date_text = cells[0].get_text(strip=True)
                link_el = row.find("a")
                if not link_el:
                    continue

                title = link_el.get_text(strip=True)
                href = link_el.get("href", "")
                if href and not href.startswith("http"):
                    href = urljoin(self.BASE, href)

                # HARD FILTER 1: must be from last 24 hours
                if not is_within_24h(date_text):
                    continue

                # HARD FILTER 2: exclude noise
                if is_noise(title):
                    log.info(f"    Skipping noise: {title[:60]}")
                    continue

                # HARD FILTER 3: must be relevant to personal finance
                if not is_relevant(title):
                    log.info(f"    Skipping irrelevant: {title[:60]}")
                    continue

                circ_ref = ""
                ref_match = re.search(r'SEBI/HO/[\w/\-]+', title)
                if ref_match:
                    circ_ref = ref_match.group()

                relevance = score_relevance(title)
                updates.append(RegUpdate(
                    regulator="SEBI",
                    title=title[:200],
                    summary=title[:300],
                    url=href,
                    pub_date=date_text,
                    category=categorize(title),
                    relevance=relevance,
                    circular_ref=circ_ref,
                    source_type="official",
                    action_required=(relevance == "HIGH"),
                ))

        log.info(f"  SEBI: {len(updates)} relevant items (after filtering)")
        return updates


# ---------------------------------------------------------------------------
# RBI SCRAPER
# ---------------------------------------------------------------------------
class RBIScraper(BaseFetcher):
    RSS_URL = "https://www.rbi.org.in/pressreleases_rss.xml"
    NOTIF_URL = "https://www.rbi.org.in/Scripts/NotificationUser.aspx"
    BASE = "https://www.rbi.org.in"

    def scrape(self) -> list[RegUpdate]:
        log.info("Scraping RBI...")
        updates = []

        # RSS feed
        for item in self.parse_rss(self.RSS_URL)[:20]:
            title, desc, date_text = item["title"], item["description"], item["date"]
            if not is_within_24h(date_text):
                continue
            if is_noise(title):
                continue
            if not is_relevant(title, desc):
                continue

            updates.append(RegUpdate(
                regulator="RBI",
                title=title[:200],
                summary=self._clean_html(desc)[:300],
                url=item["link"],
                pub_date=date_text,
                category=categorize(title, desc),
                relevance=score_relevance(title, desc),
                source_type="official",
                action_required=(score_relevance(title, desc) == "HIGH"),
            ))

        # Notifications page
        s = self.soup(self.NOTIF_URL)
        if s:
            for row in s.select("table tr")[:30]:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue
                date_text = cells[0].get_text(strip=True)
                link_el = row.find("a")
                if not link_el:
                    continue

                title = link_el.get_text(strip=True)
                href = link_el.get("href", "")
                if href and not href.startswith("http"):
                    href = urljoin(self.BASE + "/Scripts/", href)

                if not is_within_24h(date_text):
                    continue
                if is_noise(title):
                    continue
                if not is_relevant(title):
                    continue

                updates.append(RegUpdate(
                    regulator="RBI",
                    title=title[:200],
                    summary=title[:300],
                    url=href,
                    pub_date=date_text,
                    category=categorize(title),
                    relevance=score_relevance(title),
                    source_type="official",
                    action_required=(score_relevance(title) == "HIGH"),
                ))

        log.info(f"  RBI: {len(updates)} relevant items")
        return updates

    def _clean_html(self, text: str) -> str:
        return re.sub(r'<[^>]+>', '', text).strip()


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
        if not s:
            return updates

        for row in s.select("table tr")[:20]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            date_text = cells[0].get_text(strip=True)
            link_el = row.find("a")
            if not link_el:
                continue

            title = link_el.get_text(strip=True)
            href = link_el.get("href", "")
            if href and not href.startswith("http"):
                href = urljoin(self.BASE, href)

            if not is_within_24h(date_text):
                continue
            if is_noise(title):
                continue
            if not is_relevant(title):
                continue

            updates.append(RegUpdate(
                regulator="PFRDA",
                title=title[:200],
                summary=title[:300],
                url=href,
                pub_date=date_text,
                category="Pension & NPS",
                relevance=score_relevance(title),
                source_type="official",
                action_required=(score_relevance(title) == "HIGH"),
            ))

        log.info(f"  PFRDA: {len(updates)} relevant items")
        return updates


# ---------------------------------------------------------------------------
# NEWS SCRAPER — personal finance RSS feeds
# ---------------------------------------------------------------------------
class NewsScraper(BaseFetcher):
    FEEDS = {
        "Mint_Money": "https://www.livemint.com/rss/money",
        "Mint_Economy": "https://www.livemint.com/rss/economy",
        "ET_MF": "https://economictimes.indiatimes.com/wealth/mutual-funds/rssfeeds/46267806.cms",
        "ET_Tax": "https://economictimes.indiatimes.com/wealth/tax/rssfeeds/46266529.cms",
        "ET_Invest": "https://economictimes.indiatimes.com/wealth/invest/rssfeeds/46267805.cms",
        "ET_Insurance": "https://economictimes.indiatimes.com/wealth/insure/rssfeeds/46267684.cms",
        "Moneycontrol": "https://www.moneycontrol.com/rss/MCtopnews.xml",
    }

    def scrape(self) -> list[RegUpdate]:
        log.info("Scraping news feeds...")
        updates = []

        for source_name, feed_url in self.FEEDS.items():
            items = self.parse_rss(feed_url)
            for item in items[:15]:
                title = item.get("title", "")
                desc = self._clean(item.get("description", ""))
                date_text = item.get("date", "")

                # HARD FILTER: 24 hours
                if date_text and not is_within_24h(date_text):
                    continue

                # Must be relevant
                if not is_relevant(title, desc):
                    continue

                # Must not be noise
                if is_noise(title):
                    continue

                regulator = self._detect_regulator(f"{title} {desc}")
                relevance = score_relevance(title, desc)

                updates.append(RegUpdate(
                    regulator=regulator,
                    title=title[:200],
                    summary=desc[:300] if desc else title[:300],
                    url=item.get("link", ""),
                    pub_date=date_text,
                    category=categorize(title, desc),
                    relevance=relevance,
                    source_type="news",
                    action_required=(relevance == "HIGH"),
                ))

        log.info(f"  News: {len(updates)} relevant items")
        return updates

    def _detect_regulator(self, text: str) -> str:
        t = text.upper()
        for reg in ["SEBI", "RBI", "IRDAI", "IRDA", "PFRDA", "CBDT", "AMFI"]:
            if reg in t:
                return reg.replace("IRDA", "IRDAI")
        return "MoF/Other"

    def _clean(self, text: str) -> str:
        return re.sub(r'<[^>]+>', '', text).strip()[:500]


# ---------------------------------------------------------------------------
# DEDUP + TITLE CLEANER
# ---------------------------------------------------------------------------
def dedup(updates: list[RegUpdate]) -> list[RegUpdate]:
    seen = set()
    unique = []
    for u in updates:
        key = re.sub(r'[^a-z0-9]', '', u.title.lower())[:50]
        if key not in seen and len(key) > 8:
            seen.add(key)
            unique.append(u)
    return unique


def clean_title(title: str) -> str:
    """Make titles more readable and actionable."""
    # Remove trailing dates and reference numbers
    title = re.sub(r'\s*\[Last amended on.*?\]', '', title)
    title = re.sub(r'\s*\(Last amended.*?\)', '', title)
    # Remove excess whitespace
    title = re.sub(r'\s+', ' ', title).strip()
    # Trim to reasonable length
    if len(title) > 120:
        title = title[:117] + "..."
    return title


# ---------------------------------------------------------------------------
# MARKDOWN BRIEFING
# ---------------------------------------------------------------------------
def format_md(updates: list[RegUpdate]) -> str:
    lines = []
    lines.append(f"# Daily Regulatory Brief -- {DATE_STR}")
    lines.append(f"*Generated: {NOW.strftime('%Y-%m-%d %H:%M IST')} | Novelty Wealth*\n")

    if not updates:
        lines.append("> No material regulatory updates in the last 24 hours relevant to personal finance.\n")
        lines.append("*Check back tomorrow. Markets are quiet today.*")
        return "\n".join(lines)

    high = [u for u in updates if u.relevance == "HIGH"]
    med = [u for u in updates if u.relevance == "MEDIUM"]

    # Pulse
    lines.append(f"**Today's Pulse:** {len(high)} high-priority | {len(med)} medium | {len(updates)} total\n")

    if high:
        lines.append(f"> **Top action:** {high[0].title[:80]}\n")

    # HIGH items first with full detail
    if high:
        lines.append("## High Priority -- Action Required\n")
        for i, u in enumerate(high, 1):
            lines.append(f"### {i}. {u.title}\n")
            lines.append(f"**Regulator:** {u.regulator} | **Category:** {u.category}")
            if u.circular_ref:
                lines.append(f"**Ref:** `{u.circular_ref}`")
            lines.append(f"\n{u.summary}\n")
            src_label = "Official" if u.source_type == "official" else "News"
            lines.append(f"[{src_label} Source]({u.url})\n")
            lines.append("---\n")

    # MEDIUM items
    if med:
        lines.append("## Medium Priority -- Monitor\n")
        lines.append("| # | Regulator | Update | Category | Source |")
        lines.append("|---|-----------|--------|----------|--------|")
        for i, u in enumerate(med, 1):
            src = f"[Link]({u.url})"
            lines.append(f"| {i} | {u.regulator} | {u.title[:80]} | {u.category} | {src} |")
        lines.append("")

    # Footer
    lines.append(f"\n---\n*Covers: SEBI, RBI, IRDAI, PFRDA, CBDT, AMFI | Last 24 hours only*")
    lines.append(f"*Novelty Wealth (SEBI RIA: INA000019415)*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# ORCHESTRATOR
# ---------------------------------------------------------------------------
class RegulatoryMonitor:
    def __init__(self):
        self.updates: list[RegUpdate] = []

    def run(self):
        log.info("=" * 60)
        log.info(f"Regulatory Monitor v2 -- {DATE_STR}")
        log.info(f"Cutoff: {CUTOFF.strftime('%Y-%m-%d %H:%M IST')}")
        log.info("=" * 60)

        scrapers = [SEBIScraper(), RBIScraper(), PFRDAScraper(), NewsScraper()]

        for scraper in scrapers:
            try:
                self.updates.extend(scraper.scrape())
            except Exception as e:
                log.error(f"  {scraper.__class__.__name__} failed: {e}")

        # Dedup
        before = len(self.updates)
        self.updates = dedup(self.updates)
        log.info(f"Dedup: {before} -> {len(self.updates)}")

        # Clean titles
        for u in self.updates:
            u.title = clean_title(u.title)

        # Sort: HIGH first, then MEDIUM, then alphabetical within
        order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        self.updates.sort(key=lambda u: (order.get(u.relevance, 3), u.regulator))

        # Drop LOW entirely from output (noise reduction)
        self.updates = [u for u in self.updates if u.relevance in ("HIGH", "MEDIUM")]

        self._write()

        high_n = sum(1 for u in self.updates if u.relevance == "HIGH")
        log.info("=" * 60)
        log.info(f"Done: {len(self.updates)} updates ({high_n} high-priority)")
        log.info("=" * 60)

    def _write(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        BRIEFINGS_DIR.mkdir(parents=True, exist_ok=True)

        output = {
            "date": DATE_STR,
            "generated": NOW.isoformat(),
            "cutoff": CUTOFF.isoformat(),
            "total": len(self.updates),
            "high_priority": sum(1 for u in self.updates if u.relevance == "HIGH"),
            "medium_priority": sum(1 for u in self.updates if u.relevance == "MEDIUM"),
            "updates": [asdict(u) for u in self.updates],
        }

        with open(LATEST_FILE, "w") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        with open(BRIEFINGS_DIR / f"{DATE_STR}.json", "w") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        md = format_md(self.updates)
        with open(BRIEFINGS_DIR / f"{DATE_STR}.md", "w") as f:
            f.write(md)

        log.info(f"  Written: {LATEST_FILE}")
        log.info(f"  Written: {BRIEFINGS_DIR / DATE_STR}.json + .md")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    RegulatoryMonitor().run()
