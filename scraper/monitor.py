"""
Indian Financial Regulatory & Content Intelligence Monitor v3
==============================================================
Comprehensive daily scraper for:
  1. Regulatory circulars (SEBI, RBI, IRDAI, PFRDA, CBDT, AMFI, PIB)
  2. Personal finance news (20+ RSS feeds)
  3. Content opportunity scoring for Novelty Wealth

HARD RULES:
  1. Only items published in last 24 hours (datetime precision w/ timezone)
  2. Relevant to personal finance / macro economy / retail investors
  3. No IPO filings, company-specific orders, admin/procedural rules
  4. Weighted multi-signal relevance scoring
  5. Semantic dedup + cross-source clustering
  6. Content ideation fields on every item

Improvements over v2:
  - Added CBDT, IRDAI, AMFI, PIB scrapers (was missing 4 of 7 claimed regulators)
  - Datetime-precision date filtering (not date-only)
  - Weighted multi-keyword scoring (not first-match)
  - Source-tier weighting (official > tier1 > tier2)
  - Similarity-based dedup clustering
  - Content ideation fields: user_impact, content_angle, affected_segments, engagement_score
  - Retry with backoff on all HTTP requests
  - Unparseable-date fallback list
  - Runtime globals (not import-time)
  - Health monitoring (zero-result alerts)
  - 20+ RSS feeds (was 7)
  - Engagement-potential scoring
  - Expanded keyword taxonomy with synonyms
  - Negative keyword filtering (noise inside relevant titles)

Output: data/briefings/{date}.json + .md + data/latest.json
"""

import json
import re
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from urllib.parse import urljoin
from difflib import SequenceMatcher
import time
import hashlib

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DATA_DIR = Path("data")
BRIEFINGS_DIR = DATA_DIR / "briefings"
LATEST_FILE = DATA_DIR / "latest.json"
HEALTH_FILE = DATA_DIR / "scraper_health.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-IN,en;q=0.9",
}

IST = timezone(timedelta(hours=5, minutes=30))

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
    # IPO / company filings
    r"(?i)\b(limited|ltd|enterprises|industries|technologies|capital ltd)\b.*(?:drhp|prospectus|public.?issue)",
    r"(?i)^[A-Z\s]+(LIMITED|LTD)\s*$",
    r"(?i)\bfiling.*public.?issue",
    r"(?i)\bdraft.?red.?herring",
    r"(?i)\bIPO\b.*\b(filing|offer|document)\b",

    # Administrative / procedural
    r"(?i)\b(salaries|allowances|conditions of service|chairman and members)\b",
    r"(?i)\b(appeal to central government|procedure rules|annual report rules)\b",
    r"(?i)\b(form of annual statement|company law board)\b",
    r"(?i)\b(holding inquiry and imposing penalties)\b",
    r"(?i)\b(appellate tribunal).*\b(procedure|salaries|rules)\b",
    r"(?i)\b(depositories act).*\b(appeal|procedure)\b",

    # NFO filings (bare fund names)
    r"(?i)^(invesco|hdfc|icici|sbi|axis|kotak|nippon|dsp|tata|aditya)\s.*\b(fund)\b$",

    # Company-specific enforcement
    r"(?i)\b(adjudication order|consent order|settlement order)\b.*(?:limited|ltd)",
    r"(?i)\b(show cause notice)\b.*(?:limited|ltd)",
    r"(?i)\b(penalty imposed on)\b.*(?:limited|ltd)",

    # Currency operations (not personal finance)
    r"(?i)\bcounterfeit\b",
    r"(?i)\bcurrency distribution\b",
    r"(?i)\bcurrency chest\b",

    # Navigation artefacts
    r"(?i)^(notifications|circulars|draft notifications|guidelines|circulars withdrawn)$",
    r"(?i)^(rules|regulations|acts|orders|press releases)$",
    r"(?i)^(master directions|master circulars)$",

    # Corporate governance (not retail)
    r"(?i)\b(board meeting|agm|egm|shareholder meeting)\b.*(?:limited|ltd)",
    r"(?i)\brelated party transaction\b",
    r"(?i)\bcorporate governance\b.*(?:limited|ltd)",
]

# Negative signals: if these co-occur WITH a relevant keyword, downweight
NEGATIVE_MODIFIERS = [
    r"(?i)\badjudication\b",
    r"(?i)\bpenalty\b.*(?:limited|ltd|company)",
    r"(?i)\bconsent order\b",
    r"(?i)\bwholesale\b",
    r"(?i)\binstitutional\b",
    r"(?i)\bforeign portfolio\b.*(?:registration|licence)",
]


# ---------------------------------------------------------------------------
# KEYWORD TAXONOMY — with synonyms and variants
# ---------------------------------------------------------------------------
# Each entry: (canonical_keyword, weight, [variants])
# Weight: 3 = HIGH trigger, 2 = MEDIUM trigger, 1 = LOW/contextual

KEYWORD_TAXONOMY = [
    # === TAXATION (weight 3) ===
    ("income tax", 3, ["income-tax", "IT act", "IT dept", "IT department"]),
    ("capital gains", 3, ["capital gain", "LTCG", "STCG", "long term capital gain", "short term capital gain"]),
    ("TDS", 3, ["tax deducted at source", "TDS rate", "TDS on"]),
    ("ITR", 3, ["income tax return", "ITR filing", "ITR form"]),
    ("tax slab", 3, ["tax bracket", "tax rate"]),
    ("section 80", 3, ["80C", "80D", "80E", "80G", "80CCD", "80TTA", "80TTB"]),
    ("new tax regime", 3, ["old tax regime", "tax regime"]),
    ("standard deduction", 3, []),
    ("surcharge", 3, ["tax surcharge"]),
    ("indexation", 3, ["indexation benefit", "cost inflation index", "CII"]),
    ("advance tax", 2, []),
    ("tax audit", 2, []),
    ("HRA", 2, ["house rent allowance"]),
    ("form 15", 2, ["form 15G", "form 15H"]),
    ("ELSS", 3, ["equity linked saving", "tax saving fund"]),
    ("rebate", 2, ["tax rebate", "87A"]),
    ("new income tax act", 3, ["income tax act 2025", "new IT act"]),
    ("GST on insurance", 3, ["GST on premium", "GST financial services"]),
    ("gift tax", 2, ["gift taxation"]),
    ("DTAA", 2, ["double tax avoidance", "tax treaty"]),

    # === MUTUAL FUNDS (weight 3) ===
    ("mutual fund", 3, ["MF", "mutual funds"]),
    ("SIP", 3, ["systematic investment plan", "SIP amount"]),
    ("expense ratio", 3, ["TER", "total expense ratio"]),
    ("exit load", 3, []),
    ("NAV", 2, ["net asset value"]),
    ("NFO", 2, ["new fund offer"]),
    ("AMFI", 2, []),
    ("MF distributor", 2, ["MFD", "ARN"]),
    ("fund of funds", 2, ["FoF"]),
    ("debt fund", 2, ["debt mutual fund"]),
    ("hybrid fund", 2, []),
    ("index fund", 2, ["passive fund"]),
    ("ETF", 2, ["exchange traded fund"]),
    ("ELSS", 3, []),
    ("fund categorization", 3, ["recategorization"]),

    # === RATES & MONETARY POLICY (weight 3) ===
    ("repo rate", 3, ["reverse repo", "policy rate"]),
    ("rate cut", 3, ["rate reduction", "rate decrease"]),
    ("rate hike", 3, ["rate increase"]),
    ("monetary policy", 3, ["MPC", "monetary policy committee"]),
    ("inflation", 2, ["CPI inflation", "WPI", "retail inflation"]),
    ("lending rate", 2, ["MCLR", "base rate", "EBLR"]),
    ("FD rate", 3, ["fixed deposit rate", "deposit rate", "FD interest"]),
    ("savings account", 3, ["savings rate", "savings interest"]),

    # === INSURANCE (weight 3) ===
    ("insurance", 2, []),
    ("term plan", 3, ["term insurance", "term life"]),
    ("health insurance", 3, ["mediclaim", "health cover"]),
    ("ULIP", 3, []),
    ("IRDAI", 2, ["IRDA"]),
    ("claim settlement", 3, ["claim ratio"]),
    ("surrender value", 3, ["surrender charge"]),
    ("premium", 2, ["insurance premium"]),
    ("annuity", 3, ["annuity rate"]),

    # === PENSION & NPS (weight 3) ===
    ("NPS", 3, ["national pension", "NPS tier", "NPS vatsalya"]),
    ("pension", 3, ["pension fund", "pension scheme"]),
    ("PFRDA", 2, []),
    ("retirement", 2, ["retirement planning", "retirement corpus"]),

    # === DEPOSITS & SAVINGS (weight 3) ===
    ("PPF", 3, ["public provident fund"]),
    ("EPF", 3, ["employee provident fund", "PF withdrawal", "EPFO"]),
    ("small savings", 3, ["small saving scheme"]),
    ("SCSS", 3, ["senior citizen saving"]),
    ("KVP", 2, ["kisan vikas patra"]),
    ("NSC", 2, ["national savings certificate"]),
    ("sukanya", 3, ["sukanya samriddhi"]),
    ("SGB", 3, ["sovereign gold bond", "gold bond"]),

    # === CREDIT & LENDING (weight 2) ===
    ("credit score", 2, ["CIBIL", "credit bureau"]),
    ("digital lending", 2, ["online lending"]),
    ("UPI", 2, ["unified payments"]),
    ("KYC", 2, ["know your customer", "e-KYC", "CKYC"]),
    ("loan", 2, ["home loan", "personal loan", "education loan"]),
    ("EMI", 2, []),
    ("NBFC", 2, []),
    ("credit card", 2, ["credit card charges", "credit card interest"]),

    # === INVESTOR PROTECTION (weight 3) ===
    ("investor protection", 3, []),
    ("nominee", 3, ["nomination"]),
    ("RIA", 2, ["registered investment advisor", "investment adviser"]),
    ("financial planning", 2, []),
    ("disclosure", 2, []),
    ("demat", 2, ["demat account"]),
    ("financial fraud", 3, ["investment fraud", "ponzi", "scam"]),

    # === GOVT SCHEMES (weight 2) ===
    ("Atal Pension", 2, ["APY"]),
    ("PM Vaya Vandana", 2, ["PMVVY"]),
    ("Ayushman", 2, []),

    # === CAPITAL MARKETS (weight 2) ===
    ("SEBI", 2, []),
    ("stock market", 2, ["equity market", "share market"]),
    ("trading", 2, ["intraday", "F&O", "futures", "options"]),
    ("REIT", 2, ["real estate investment trust"]),
    ("InvIT", 2, []),
    ("AIF", 2, ["alternative investment fund"]),
    ("PMS", 2, ["portfolio management service"]),
    ("margin", 2, ["margin trading", "margin requirement"]),

    # === MACRO & ECONOMY (weight 2) ===
    ("GDP", 2, ["economic growth"]),
    ("fiscal deficit", 2, ["fiscal policy"]),
    ("budget", 2, ["union budget", "finance bill"]),
    ("tariff", 2, ["trade war", "import duty", "customs duty"]),
    ("rupee", 2, ["INR", "dollar rupee", "USD INR"]),
    ("crude oil", 2, ["oil price", "petrol", "diesel"]),
    ("FII", 2, ["FPI", "foreign investor"]),
    ("global market", 2, ["US market", "Nasdaq", "S&P"]),
    ("recession", 2, ["slowdown"]),
    ("employment", 2, ["unemployment", "jobs data"]),

    # === MARKET VOLATILITY (weight 3 — high engagement) ===
    ("market crash", 3, ["market fall", "market correction", "bloodbath"]),
    ("VIX", 2, ["India VIX", "volatility"]),
    ("circuit breaker", 3, []),

    # === REAL ESTATE (weight 2) ===
    ("REIT", 2, []),
    ("home loan rate", 2, ["housing loan"]),
    ("stamp duty", 2, []),
    ("property tax", 2, []),

    # === FINTECH REGULATION (weight 2) ===
    ("fintech", 2, ["fintech regulation"]),
    ("digital lending", 2, ["lending app"]),
    ("payment aggregator", 2, []),
    ("RBI digital", 2, ["CBDC", "digital rupee"]),
]

# Build flat lookup for fast matching
_KEYWORD_LOOKUP: list[tuple[str, int]] = []
for canonical, weight, variants in KEYWORD_TAXONOMY:
    _KEYWORD_LOOKUP.append((canonical.lower(), weight))
    for v in variants:
        _KEYWORD_LOOKUP.append((v.lower(), weight))

# Category-to-segments mapping
SEGMENT_MAP = {
    "Taxation": ["taxpayers", "salaried", "equity investors", "HNIs"],
    "Mutual Funds": ["MF investors", "SIP holders"],
    "Rates & Monetary Policy": ["borrowers", "FD holders", "salaried"],
    "Insurance": ["insurance holders", "health insurance buyers"],
    "Pension & NPS": ["NPS subscribers", "retirees", "salaried"],
    "Deposits & Savings": ["conservative investors", "retirees", "salaried"],
    "Credit & Lending": ["borrowers", "credit card users"],
    "Investor Protection": ["all investors"],
    "Govt Schemes": ["small savers", "retirees", "salaried"],
    "Macro & Economy": ["all investors"],
    "Capital Markets": ["stock traders", "equity investors"],
    "Regulatory Update": ["all investors"],
}


# ---------------------------------------------------------------------------
# RELEVANCE ENGINE (weighted multi-signal)
# ---------------------------------------------------------------------------
def compute_relevance_score(title: str, description: str = "", source_type: str = "news") -> int:
    """
    Compute a numeric relevance score based on:
    - Keyword matches (weighted)
    - Source tier bonus
    - Negative modifier penalty
    - Multi-keyword bonus
    Returns an integer score (higher = more relevant).
    """
    combined = f"{title} {description}".lower()
    score = 0
    matched_keywords = []

    for kw, weight in _KEYWORD_LOOKUP:
        if kw in combined:
            score += weight
            matched_keywords.append(kw)

    # Multi-keyword bonus: more matches = more relevant
    unique_matches = len(set(matched_keywords))
    if unique_matches >= 4:
        score += 3
    elif unique_matches >= 2:
        score += 1

    # Source tier bonus
    if source_type == "official":
        score += 2
    elif source_type == "tier1_news":
        score += 1

    # Negative modifier penalty
    for pattern in NEGATIVE_MODIFIERS:
        if re.search(pattern, combined):
            score -= 2

    # Soft suppression penalty (v3.1)
    score += soft_suppression_penalty(title)

    return max(score, 0)


def score_to_level(score: int) -> str:
    """Convert numeric score to HIGH / MEDIUM / LOW."""
    if score >= 5:
        return "HIGH"
    elif score >= 2:
        return "MEDIUM"
    elif score >= 1:
        return "LOW"
    return "NONE"


def is_noise(title: str) -> bool:
    for pattern in NOISE_PATTERNS:
        if re.search(pattern, title):
            return True
    return False


# Soft suppression: items that match these get a score penalty but aren't hard-blocked
SOFT_SUPPRESS_PATTERNS = [
    r"(?i)\bIDCW\b",                    # routine dividend payouts
    r"(?i)\bdividend\b.*\b(record date|ex-date)\b",
    r"(?i)\bNFO\b.*\b(open|launch|subscribe)\b",  # routine NFOs (category shifts still pass via scoring)
    r"(?i)\bcorporate action\b",
    r"(?i)\bboard meeting\b",
    r"(?i)\bresult.*quarter\b",          # quarterly results
]


def soft_suppression_penalty(title: str) -> int:
    """Returns a negative score adjustment for soft-suppress items."""
    penalty = 0
    for pattern in SOFT_SUPPRESS_PATTERNS:
        if re.search(pattern, title):
            penalty -= 2
    return penalty


def is_relevant(title: str, description: str = "", source_type: str = "news") -> bool:
    """Returns True if item passes minimum relevance threshold."""
    return compute_relevance_score(title, description, source_type) >= 1


def categorize(title: str, description: str = "") -> str:
    t = f"{title} {description}".lower()
    categories = [
        ("Taxation", ["income tax", "tds", "capital gain", "itr", "tax slab", "form 15", "80c", "80d", "ltcg", "stcg", "indexation", "tax regime", "elss", "gst on insurance", "advance tax", "surcharge", "rebate", "dtaa"]),
        ("Mutual Funds", ["mutual fund", "nfo", "expense ratio", "sip", "nav", "amfi", "mf ", "fund categorization", "exit load", "etf", "index fund", "debt fund", "hybrid fund"]),
        ("Rates & Monetary Policy", ["repo rate", "rate cut", "rate hike", "monetary", "mpc", "inflation", "mclr", "lending rate"]),
        ("Insurance", ["insurance", "irdai", "irda", "term plan", "health insurance", "ulip", "claim", "premium", "surrender"]),
        ("Pension & NPS", ["nps", "pension", "pfrda", "annuity", "tier", "retirement", "atal pension", "apy"]),
        ("Deposits & Savings", ["saving", "fd ", "fixed deposit", "deposit rate", "savings account", "ppf", "epf", "scss", "kvp", "nsc", "sukanya", "sgb", "small saving"]),
        ("Credit & Lending", ["credit", "cibil", "loan", "emi", "lending", "nbfc", "credit card", "upi"]),
        ("Investor Protection", ["kyc", "nominee", "demat", "investor protection", "ria", "advisor", "fraud", "scam"]),
        ("Govt Schemes", ["atal pension", "pm vaya", "ayushman", "pmvvy"]),
        ("Macro & Economy", ["gdp", "economy", "fiscal", "budget", "trade", "tariff", "rupee", "dollar", "crude", "fii", "fpi", "employment", "recession"]),
        ("Capital Markets", ["stock", "equity", "market", "sebi", "trading", "reit", "invit", "aif", "pms", "margin", "f&o", "circuit"]),
    ]
    for cat_name, keywords in categories:
        if any(kw in t for kw in keywords):
            return cat_name
    return "Regulatory Update"


def compute_engagement_score(title: str, description: str, category: str, relevance_score: int) -> int:
    """
    Estimate content engagement potential (1-10).
    Based on: topic virality, user impact breadth, actionability, novelty.
    """
    combined = f"{title} {description}".lower()
    engagement = 0

    # High-virality topics (tax, rate changes, market events)
    viral_triggers = ["tax", "rate cut", "rate hike", "market crash", "budget", "slab", "ltcg", "stcg", "sip", "fd rate", "inflation", "scam", "fraud"]
    for vt in viral_triggers:
        if vt in combined:
            engagement += 2
            break

    # Broad user impact
    broad_impact = ["all investors", "taxpayers", "salaried"]
    segments = SEGMENT_MAP.get(category, ["all investors"])
    if any(s in broad_impact for s in segments):
        engagement += 2

    # Actionability signals
    action_words = ["must", "mandatory", "deadline", "effective from", "last date", "new rule", "changed", "revised", "increased", "decreased", "abolished", "introduced"]
    for aw in action_words:
        if aw in combined:
            engagement += 2
            break

    # Base from relevance
    engagement += min(relevance_score // 2, 3)

    return min(max(engagement, 1), 10)


def generate_user_impact(title: str, category: str) -> str:
    """Generate a one-line user impact summary."""
    t = title.lower()
    impact_templates = {
        "Taxation": "May affect your tax liability or filing process",
        "Mutual Funds": "May affect your mutual fund investments or SIPs",
        "Rates & Monetary Policy": "May impact your loan EMIs, FD returns, or savings rates",
        "Insurance": "May affect your insurance premiums, claims, or policy terms",
        "Pension & NPS": "May affect your NPS contributions, withdrawals, or pension planning",
        "Deposits & Savings": "May impact your FD, PPF, or small savings returns",
        "Credit & Lending": "May affect your loan eligibility, credit score, or EMIs",
        "Investor Protection": "Affects how your investments are protected and administered",
        "Govt Schemes": "May change benefits or eligibility for government savings schemes",
        "Macro & Economy": "Broader economic signal that may affect your portfolio",
        "Capital Markets": "May affect stock market trading rules or your equity investments",
    }
    return impact_templates.get(category, "Regulatory development relevant to your finances")


def generate_content_angle(title: str, category: str) -> str:
    """Suggest a Novelty Wealth content angle."""
    t = title.lower()
    if any(w in t for w in ["new rule", "circular", "notification", "amendment", "revised"]):
        return f"Explainer: What this {category.lower()} change means for you"
    if any(w in t for w in ["rate cut", "rate hike", "repo"]):
        return "Impact analysis: How this rate change affects your money"
    if any(w in t for w in ["deadline", "last date", "due date"]):
        return "Reminder + checklist content for users"
    if any(w in t for w in ["scam", "fraud", "warning"]):
        return "Trust-building: How to protect yourself"
    if any(w in t for w in ["market crash", "correction", "fall"]):
        return "Calm-down content: What to do (and not do) right now"
    return f"Educational explainer on this {category.lower()} development"


# ---------------------------------------------------------------------------
# DATA MODEL (v3 — with content ideation fields)
# ---------------------------------------------------------------------------
@dataclass
class RegUpdate:
    regulator: str
    title: str
    summary: str
    url: str
    pub_date: str
    category: str               # primary category (backward compat)
    relevance: str              # HIGH / MEDIUM / LOW
    relevance_score: int        # composite numeric score
    source_type: str            # official / tier1_news / tier2_news / blog
    source_name: str            # e.g., "SEBI", "Mint", "ET Wealth"
    circular_ref: str = ""
    action_required: bool = False

    # === 4-AXIS SCORING (v3.1 — from ChatGPT audit) ===
    regulatory_importance: int = 0   # 0-10: how significant is this regulatory change
    retail_user_impact: int = 0      # 0-10: how directly does this affect retail users
    actionability: int = 0           # 0-10: does user need to DO something
    engagement_potential: int = 0    # 0-10: will this drive content engagement

    # === MULTI-LABEL TAGS (v3.1) ===
    topic_tags: list = field(default_factory=list)        # ["tax", "MF", "SIP", "FD"]
    user_segment_tags: list = field(default_factory=list)  # ["salaried", "retirees", "HNI"]
    content_tags: list = field(default_factory=list)       # ["explainer", "alert", "reaction"]

    # === CONTENT IDEATION (v3) ===
    user_impact: str = ""
    content_angle: str = ""
    affected_segments: list = field(default_factory=list)
    engagement_score: int = 0       # kept for backward compat (= engagement_potential)
    urgency: str = "awareness"      # immediate / this_week / awareness
    possible_content_formats: list = field(default_factory=list)  # ["blog", "reel", "push", "carousel"]
    story_maturity: str = "confirmed"  # breaking / developing / confirmed / evergreen
    evergreen_or_breaking: str = "breaking"  # breaking / evergreen

    # === ACTION DETECTION (v3.1) ===
    action_type: str = ""           # file / switch / update / verify / claim / review / none
    action_deadline: str = ""       # extracted deadline if any

    # === CLUSTERING (v3) ===
    cluster_id: str = ""
    also_covered_by: list = field(default_factory=list)
    source_tier: str = "tier2_news"

    # === NOVELTY WEALTH ANGLE (v3.1) ===
    nw_angle: str = ""              # portfolio_review / tax_optimization / risk_education / family_finance / wealth_checkup

    # === METADATA ===
    date_parsed: bool = True
    matched_keywords: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# DATE PARSING (v3 — datetime precision with timezone)
# ---------------------------------------------------------------------------
def parse_datetime(text: str) -> Optional[datetime]:
    """Parse into a timezone-aware datetime. Returns None if unparseable."""
    text = text.strip()
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%d %b %Y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)
            return dt
        except ValueError:
            continue

    # Fall back to date-only formats (assume start of day IST)
    d = parse_date_only(text)
    if d:
        return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=IST)
    return None


def parse_date_only(text: str) -> Optional[date]:
    """Try multiple date-only formats."""
    text = text.strip()
    formats = [
        "%b %d, %Y", "%d %b %Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d",
        "%B %d, %Y", "%d %B %Y", "%d %b, %Y", "%d-%b-%Y", "%b %d %Y",
        "%B %d %Y", "%d %B, %Y", "%d.%m.%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    # Extract date-like substring
    m = re.search(r'(\d{1,2}[\s\-/\.]\w{3,9}[\s\-/\.,]+\d{4})', text)
    if m:
        return parse_date_only(m.group(1))

    m = re.search(r'(\w{3,9}\s+\d{1,2},?\s+\d{4})', text)
    if m:
        return parse_date_only(m.group(1))

    return None


def is_within_24h(date_text: str, cutoff: datetime) -> tuple[bool, bool]:
    """
    Returns (is_recent, date_was_parsed).
    If date can't be parsed, returns (False, False).
    """
    dt = parse_datetime(date_text)
    if dt is None:
        return False, False
    return dt >= cutoff, True


# ---------------------------------------------------------------------------
# BASE FETCHER (v3 — with retry + backoff)
# ---------------------------------------------------------------------------
class BaseFetcher:
    MAX_RETRIES = 2
    RETRY_DELAY = 3  # seconds

    def get(self, url: str, timeout: int = 20) -> Optional[str]:
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=timeout)
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                if attempt < self.MAX_RETRIES:
                    log.warning(f"  Retry {attempt+1}/{self.MAX_RETRIES} for {url}: {e}")
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                else:
                    log.warning(f"  Failed after {self.MAX_RETRIES+1} attempts: {url} -- {e}")
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
            log.warning(f"  RSS parse error for {url}: {e}")
        return items


# ---------------------------------------------------------------------------
# 4-AXIS SCORING (v3.1)
# ---------------------------------------------------------------------------
def compute_regulatory_importance(title: str, desc: str, source_type: str) -> int:
    """How significant is this as a regulatory change? (0-10)"""
    combined = f"{title} {desc}".lower()
    score = 0
    # Official sources inherently more regulatory-important
    if source_type == "official":
        score += 3
    # High-impact regulatory signals
    high_reg = ["new rule", "amendment", "revised", "notification", "circular", "effective from",
                "gazette", "act", "regulation", "mandate", "abolished", "introduced", "supersede"]
    for w in high_reg:
        if w in combined:
            score += 2
            break
    # Regulator mentions
    if any(r in combined for r in ["sebi", "rbi", "irdai", "pfrda", "cbdt", "amfi"]):
        score += 1
    # Draft/consultation = lower
    if any(w in combined for w in ["draft", "consultation", "proposed", "discussion paper"]):
        score -= 1
    return min(max(score, 0), 10)


def compute_retail_user_impact(title: str, desc: str, category: str) -> int:
    """How directly does this affect retail investors/users? (0-10)"""
    combined = f"{title} {desc}".lower()
    score = 0
    # Direct user-impact words
    direct = ["your", "investor", "taxpayer", "policyholder", "subscriber", "depositor",
              "retail", "individual", "salaried", "senior citizen", "nominee", "beneficiary"]
    for w in direct:
        if w in combined:
            score += 2
            break
    # Product mentions = user-facing
    products = ["sip", "mutual fund", "fd", "ppf", "nps", "insurance", "emi", "loan",
                "credit card", "demat", "tax", "itr", "elss", "savings account"]
    product_count = sum(1 for p in products if p in combined)
    score += min(product_count * 2, 4)
    # Broad categories are more impactful
    broad_cats = ["Taxation", "Rates & Monetary Policy", "Deposits & Savings"]
    if category in broad_cats:
        score += 2
    return min(max(score, 0), 10)


def compute_actionability(title: str, desc: str) -> tuple[int, str, str]:
    """
    Does the user need to DO something? (0-10)
    Also returns detected action_type and action_deadline.
    """
    combined = f"{title} {desc}".lower()
    score = 0
    action_type = "none"

    # Action verb detection
    action_verbs = {
        "file": ["file", "filing", "submit", "return"],
        "switch": ["switch", "migrate", "opt", "choose", "select"],
        "update": ["update", "revise", "amend", "modify", "change"],
        "verify": ["verify", "check", "confirm", "validate", "link", "kyc"],
        "claim": ["claim", "redeem", "withdraw", "encash"],
        "review": ["review", "assess", "evaluate", "reconsider", "rebalance"],
    }
    for atype, verbs in action_verbs.items():
        if any(v in combined for v in verbs):
            score += 3
            action_type = atype
            break

    # Deadline signals
    deadline = ""
    deadline_words = ["deadline", "last date", "due date", "before", "by", "effective from", "w.e.f."]
    for dw in deadline_words:
        if dw in combined:
            score += 3
            # Try to extract date near deadline word
            idx = combined.find(dw)
            nearby = combined[idx:idx+60]
            date_match = re.search(r'(\d{1,2}[\s/\-]\w{3,9}[\s/\-,]*\d{4})', nearby)
            if date_match:
                deadline = date_match.group(1).strip()
            break

    # Mandatory/compulsory
    if any(w in combined for w in ["mandatory", "compulsory", "must", "required"]):
        score += 2

    return min(max(score, 0), 10), action_type, deadline


# ---------------------------------------------------------------------------
# MULTI-LABEL TAGGING (v3.1)
# ---------------------------------------------------------------------------
TOPIC_TAG_MAP = [
    ("tax", ["income tax", "tds", "capital gain", "ltcg", "stcg", "itr", "tax slab", "80c", "80d", "elss", "gst", "advance tax", "surcharge", "dtaa", "indexation"]),
    ("mutual_funds", ["mutual fund", "sip", "nfo", "expense ratio", "exit load", "nav", "amfi", "etf", "index fund", "debt fund", "hybrid fund"]),
    ("rates", ["repo rate", "rate cut", "rate hike", "monetary policy", "mpc", "inflation", "mclr", "lending rate"]),
    ("insurance", ["insurance", "irdai", "term plan", "health insurance", "ulip", "claim settlement", "premium", "surrender"]),
    ("pension", ["nps", "pension", "pfrda", "annuity", "retirement", "atal pension"]),
    ("deposits", ["fd", "fixed deposit", "ppf", "epf", "scss", "sgb", "small saving", "savings account", "kvp", "nsc", "sukanya"]),
    ("credit", ["credit", "cibil", "loan", "emi", "lending", "nbfc", "credit card", "upi"]),
    ("protection", ["kyc", "nominee", "investor protection", "fraud", "scam", "demat"]),
    ("markets", ["stock market", "equity", "sebi", "trading", "reit", "aif", "pms", "f&o", "nifty", "sensex"]),
    ("macro", ["gdp", "economy", "budget", "tariff", "rupee", "crude oil", "fii", "fpi", "recession"]),
    ("real_estate", ["reit", "home loan", "stamp duty", "property tax", "housing"]),
    ("gold", ["gold", "sgb", "sovereign gold", "gold etf"]),
]

USER_SEGMENT_TAG_MAP = [
    ("salaried", ["salary", "salaried", "hra", "standard deduction", "form 16", "employer"]),
    ("retirees", ["senior citizen", "pension", "scss", "annuity", "retirement", "nps"]),
    ("hni", ["hni", "aif", "pms", "surcharge", "dtaa", "gift tax", "family trust"]),
    ("mf_investors", ["mutual fund", "sip", "nfo", "expense ratio", "etf", "index fund"]),
    ("stock_traders", ["stock", "trading", "f&o", "margin", "intraday", "demat"]),
    ("taxpayers", ["tax", "tds", "itr", "capital gain", "ltcg", "stcg", "80c"]),
    ("borrowers", ["loan", "emi", "home loan", "lending rate", "mclr", "credit"]),
    ("insurance_holders", ["insurance", "premium", "claim", "health insurance", "term plan"]),
    ("first_time", ["beginner", "start investing", "first investment", "new investor"]),
    ("families", ["nominee", "sukanya", "family", "inheritance", "senior citizen"]),
]

CONTENT_TAG_MAP = [
    ("alert", ["mandatory", "deadline", "effective from", "last date", "compulsory", "must"]),
    ("explainer", ["what is", "how to", "understand", "guide", "explained", "meaning"]),
    ("reaction", ["market crash", "rate cut", "rate hike", "budget", "correction", "fall"]),
    ("myth_busting", ["myth", "misconception", "actually", "truth"]),
    ("comparison", ["vs", "versus", "compared", "better", "which"]),
    ("checklist", ["checklist", "steps", "things to do", "before you"]),
]


def generate_topic_tags(combined: str) -> list[str]:
    tags = []
    for tag, keywords in TOPIC_TAG_MAP:
        if any(kw in combined for kw in keywords):
            tags.append(tag)
    return tags


def generate_user_segment_tags(combined: str) -> list[str]:
    tags = []
    for tag, keywords in USER_SEGMENT_TAG_MAP:
        if any(kw in combined for kw in keywords):
            tags.append(tag)
    return tags or ["all_investors"]


def generate_content_tags(combined: str) -> list[str]:
    tags = []
    for tag, keywords in CONTENT_TAG_MAP:
        if any(kw in combined for kw in keywords):
            tags.append(tag)
    return tags or ["informational"]


# ---------------------------------------------------------------------------
# CONTENT FORMAT + STORY MATURITY + NW ANGLE (v3.1)
# ---------------------------------------------------------------------------
def suggest_content_formats(engagement: int, actionability: int, category: str) -> list[str]:
    """Suggest best content formats based on item characteristics."""
    formats = []
    if engagement >= 7:
        formats.extend(["reel", "carousel"])
    if actionability >= 5:
        formats.append("push_notification")
    if engagement >= 4:
        formats.append("blog")
    if actionability >= 7:
        formats.append("in_app_widget")
    if category in ("Taxation", "Mutual Funds", "Rates & Monetary Policy"):
        if "blog" not in formats:
            formats.append("blog")
    if not formats:
        formats.append("blog")
    return formats


def detect_story_maturity(title: str, desc: str) -> tuple[str, str]:
    """Returns (story_maturity, evergreen_or_breaking)."""
    combined = f"{title} {desc}".lower()
    if any(w in combined for w in ["breaking", "just in", "flash", "developing"]):
        return "breaking", "breaking"
    if any(w in combined for w in ["draft", "proposed", "consultation", "discussion paper", "expected"]):
        return "developing", "breaking"
    if any(w in combined for w in ["guide", "how to", "what is", "explained", "everything you need"]):
        return "evergreen", "evergreen"
    return "confirmed", "breaking"


def detect_nw_angle(category: str, combined: str) -> str:
    """Detect best Novelty Wealth editorial angle."""
    if any(w in combined for w in ["tax", "itr", "tds", "capital gain", "ltcg", "stcg", "80c"]):
        return "tax_optimization"
    if any(w in combined for w in ["crash", "correction", "volatility", "risk", "rebalance", "allocation"]):
        return "risk_education"
    if any(w in combined for w in ["senior citizen", "nominee", "inheritance", "family", "sukanya"]):
        return "family_finance"
    if any(w in combined for w in ["portfolio", "fund", "sip", "investment", "returns"]):
        return "portfolio_review"
    return "wealth_checkup"


# ---------------------------------------------------------------------------
# EXCLUSION LOG (v3.1 — track why items were dropped)
# ---------------------------------------------------------------------------
_exclusion_log: list[dict] = []


def log_exclusion(title: str, url: str, reason: str, source: str):
    """Track excluded items for filter tuning."""
    _exclusion_log.append({
        "title": title[:120],
        "url": url,
        "reason": reason,
        "source": source,
    })


def get_exclusion_log() -> list[dict]:
    return _exclusion_log


def passes_filters(title: str, description: str, url: str, source_name: str,
                   source_type: str = "news", lenient: bool = False) -> bool:
    """
    Unified filter gate with exclusion logging.
    Returns True if item should be included.
    Set lenient=True for domain-specific sources (CBDT, IRDAI) where
    keyword matching can be relaxed.
    """
    if is_noise(title):
        log_exclusion(title, url, "noise_pattern", source_name)
        return False

    if is_relevant(title, description, source_type):
        return True

    # Lenient mode: check domain-specific fallback keywords
    if lenient:
        return True  # caller handles domain-specific checks after this

    log_exclusion(title, url, "not_relevant", source_name)
    return False


# ---------------------------------------------------------------------------
# HELPER: Build a RegUpdate with all v3.1 fields populated
# ---------------------------------------------------------------------------
def build_update(
    regulator: str,
    title: str,
    description: str,
    url: str,
    pub_date: str,
    source_type: str,
    source_name: str,
    circular_ref: str = "",
    date_parsed: bool = True,
) -> RegUpdate:
    """Centralized builder that computes all derived fields."""
    category = categorize(title, description)
    combined = f"{title} {description}".lower()

    # Core relevance
    rel_score = compute_relevance_score(title, description, source_type)
    level = score_to_level(rel_score)

    # 4-axis scoring
    reg_importance = compute_regulatory_importance(title, description, source_type)
    retail_impact = compute_retail_user_impact(title, description, category)
    actionability_score, action_type, action_deadline = compute_actionability(title, description)
    engagement = compute_engagement_score(title, description, category, rel_score)

    # Multi-label tags
    topic_tags = generate_topic_tags(combined)
    user_segment_tags = generate_user_segment_tags(combined)
    content_tags = generate_content_tags(combined)

    # Content ideation
    user_impact = generate_user_impact(title, category)
    content_angle = generate_content_angle(title, category)
    segments = SEGMENT_MAP.get(category, ["all investors"])
    content_formats = suggest_content_formats(engagement, actionability_score, category)
    story_mat, eg_or_br = detect_story_maturity(title, description)
    nw_angle = detect_nw_angle(category, combined)

    # Urgency
    if any(w in combined for w in ["effective from", "deadline", "last date", "mandatory", "immediately"]):
        urgency = "immediate"
    elif any(w in combined for w in ["proposed", "draft", "consultation", "upcoming"]):
        urgency = "awareness"
    elif level == "HIGH":
        urgency = "this_week"
    else:
        urgency = "awareness"

    # Source tier
    if source_type == "official":
        source_tier = "official"
    elif source_name in ("Mint_Money", "Mint_Economy", "Mint_Market", "ET_MF", "ET_Tax", "ET_Invest",
                          "ET_Insurance", "ET_Save", "BusinessStandard_Economy", "BusinessStandard_Markets",
                          "BusinessStandard_PF", "CNBCTV18"):
        source_tier = "tier1_news"
    else:
        source_tier = "tier2_news"

    # Matched keywords
    matched = []
    for kw, weight in _KEYWORD_LOOKUP:
        if kw in combined:
            matched.append(kw)

    # Action required: now based on actionability score, not just relevance level
    action_req = actionability_score >= 5 or (level == "HIGH" and action_type != "none")

    return RegUpdate(
        regulator=regulator,
        title=title[:200],
        summary=description[:500] if description else title[:300],
        url=url,
        pub_date=pub_date,
        category=category,
        relevance=level,
        relevance_score=rel_score,
        source_type=source_type,
        source_name=source_name,
        circular_ref=circular_ref,
        action_required=action_req,
        regulatory_importance=reg_importance,
        retail_user_impact=retail_impact,
        actionability=actionability_score,
        engagement_potential=engagement,
        topic_tags=topic_tags,
        user_segment_tags=user_segment_tags,
        content_tags=content_tags,
        user_impact=user_impact,
        content_angle=content_angle,
        affected_segments=segments,
        engagement_score=engagement,
        urgency=urgency,
        possible_content_formats=content_formats,
        story_maturity=story_mat,
        evergreen_or_breaking=eg_or_br,
        action_type=action_type,
        action_deadline=action_deadline,
        cluster_id="",
        also_covered_by=[],
        source_tier=source_tier,
        nw_angle=nw_angle,
        date_parsed=date_parsed,
        matched_keywords=list(set(matched))[:10],
    )


# ---------------------------------------------------------------------------
# SEBI SCRAPER
# ---------------------------------------------------------------------------
class SEBIScraper(BaseFetcher):
    CIRCULARS_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=1&smid=0"
    PRESS_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=3&ssid=0&smid=0"
    BASE = "https://www.sebi.gov.in"

    def scrape(self, cutoff: datetime) -> list[RegUpdate]:
        log.info("Scraping SEBI circulars + press releases...")
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

                recent, parsed = is_within_24h(date_text, cutoff)
                if not recent and parsed:
                    continue

                if not passes_filters(title, "", href, "SEBI", "official"):
                    continue

                circ_ref = ""
                ref_match = re.search(r'SEBI/HO/[\w/\-]+', title)
                if ref_match:
                    circ_ref = ref_match.group()

                updates.append(build_update(
                    regulator="SEBI",
                    title=title,
                    description=title,
                    url=href,
                    pub_date=date_text,
                    source_type="official",
                    source_name="SEBI",
                    circular_ref=circ_ref,
                    date_parsed=parsed,
                ))

        log.info(f"  SEBI: {len(updates)} relevant items")
        return updates


# ---------------------------------------------------------------------------
# RBI SCRAPER
# ---------------------------------------------------------------------------
class RBIScraper(BaseFetcher):
    RSS_URL = "https://www.rbi.org.in/pressreleases_rss.xml"
    NOTIF_URL = "https://www.rbi.org.in/Scripts/NotificationUser.aspx"
    BASE = "https://www.rbi.org.in"

    def scrape(self, cutoff: datetime) -> list[RegUpdate]:
        log.info("Scraping RBI...")
        updates = []

        for item in self.parse_rss(self.RSS_URL)[:20]:
            title, desc, date_text = item["title"], item["description"], item["date"]
            recent, parsed = is_within_24h(date_text, cutoff)
            if not recent and parsed:
                continue
            if not passes_filters(title, desc, "", "RBI", "official"):
                continue

            clean_desc = re.sub(r'<[^>]+>', '', desc).strip()
            updates.append(build_update(
                regulator="RBI", title=title, description=clean_desc,
                url=item["link"], pub_date=date_text,
                source_type="official", source_name="RBI",
                date_parsed=parsed,
            ))

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

                recent, parsed = is_within_24h(date_text, cutoff)
                if not recent and parsed:
                    continue
                if not passes_filters(title, "", href, "RBI", "official"):
                    continue

                updates.append(build_update(
                    regulator="RBI", title=title, description=title,
                    url=href, pub_date=date_text,
                    source_type="official", source_name="RBI",
                    date_parsed=parsed,
                ))

        log.info(f"  RBI: {len(updates)} relevant items")
        return updates


# ---------------------------------------------------------------------------
# PFRDA SCRAPER
# ---------------------------------------------------------------------------
class PFRDAScraper(BaseFetcher):
    URL = "https://www.pfrda.org.in/index1.cshtml?lsid=1063"
    BASE = "https://www.pfrda.org.in"

    def scrape(self, cutoff: datetime) -> list[RegUpdate]:
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

            recent, parsed = is_within_24h(date_text, cutoff)
            if not recent and parsed:
                continue
            if not passes_filters(title, "", href, "PFRDA", "official"):
                continue

            updates.append(build_update(
                regulator="PFRDA", title=title, description=title,
                url=href, pub_date=date_text,
                source_type="official", source_name="PFRDA",
                date_parsed=parsed,
            ))

        log.info(f"  PFRDA: {len(updates)} relevant items")
        return updates


# ---------------------------------------------------------------------------
# CBDT SCRAPER (NEW in v3)
# ---------------------------------------------------------------------------
class CBDTScraper(BaseFetcher):
    """Scrapes incometaxindia.gov.in for notifications and circulars."""
    CIRCULARS_URL = "https://incometaxindia.gov.in/Pages/communications/circulars.aspx"
    NOTIF_URL = "https://incometaxindia.gov.in/Pages/communications/notifications.aspx"
    PRESS_URL = "https://incometaxindia.gov.in/Pages/communications/press-releases.aspx"
    BASE = "https://incometaxindia.gov.in"

    def scrape(self, cutoff: datetime) -> list[RegUpdate]:
        log.info("Scraping CBDT (Income Tax)...")
        updates = []

        for url, label in [
            (self.CIRCULARS_URL, "circular"),
            (self.NOTIF_URL, "notification"),
            (self.PRESS_URL, "press"),
        ]:
            s = self.soup(url)
            if not s:
                continue

            rows = s.select("table tr, .result-list li, .list-group-item")
            for row in rows[:25]:
                cells = row.find_all("td")
                link_el = row.find("a")

                if cells and len(cells) >= 2:
                    date_text = cells[0].get_text(strip=True)
                    if not link_el:
                        link_el = cells[-1].find("a") or cells[1].find("a")
                elif link_el:
                    # Try to find date in row text
                    row_text = row.get_text(strip=True)
                    date_match = re.search(r'(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4})', row_text)
                    date_text = date_match.group(1) if date_match else ""
                else:
                    continue

                if not link_el:
                    continue

                title = link_el.get_text(strip=True)
                href = link_el.get("href", "")
                if href and not href.startswith("http"):
                    href = urljoin(self.BASE, href)

                if not date_text:
                    continue

                recent, parsed = is_within_24h(date_text, cutoff)
                if not recent and parsed:
                    continue
                if is_noise(title):
                    log_exclusion(title, href, "noise_pattern", "CBDT")
                    continue

                # CBDT items are almost always tax-relevant, but still filter
                if not is_relevant(title, "", "official"):
                    # For CBDT, lower the bar — it's a tax source
                    if not any(w in title.lower() for w in ["tax", "income", "tds", "itr", "notification", "circular", "section", "cbdt"]):
                        log_exclusion(title, href, "not_relevant_even_lenient", "CBDT")
                        continue

                updates.append(build_update(
                    regulator="CBDT", title=title, description=title,
                    url=href, pub_date=date_text,
                    source_type="official", source_name="CBDT",
                    date_parsed=parsed,
                ))

        log.info(f"  CBDT: {len(updates)} relevant items")
        return updates


# ---------------------------------------------------------------------------
# IRDAI SCRAPER (NEW in v3)
# ---------------------------------------------------------------------------
class IRDAIScraper(BaseFetcher):
    """Scrapes irdai.gov.in for circulars and press releases."""
    CIRCULARS_URL = "https://irdai.gov.in/circulars"
    PRESS_URL = "https://irdai.gov.in/press-releases"
    BASE = "https://irdai.gov.in"

    def scrape(self, cutoff: datetime) -> list[RegUpdate]:
        log.info("Scraping IRDAI...")
        updates = []

        for url, label in [(self.CIRCULARS_URL, "circular"), (self.PRESS_URL, "press")]:
            s = self.soup(url)
            if not s:
                continue

            # IRDAI uses various listing formats
            rows = s.select("table tr, .journal-content-article tr, .list-group-item, .portlet-body tr")
            for row in rows[:25]:
                cells = row.find_all("td")
                link_el = row.find("a")

                if cells and len(cells) >= 2:
                    date_text = cells[0].get_text(strip=True)
                    if not link_el:
                        for cell in cells:
                            link_el = cell.find("a")
                            if link_el:
                                break
                elif link_el:
                    row_text = row.get_text(strip=True)
                    date_match = re.search(r'(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4})', row_text)
                    date_text = date_match.group(1) if date_match else ""
                else:
                    continue

                if not link_el:
                    continue

                title = link_el.get_text(strip=True)
                href = link_el.get("href", "")
                if href and not href.startswith("http"):
                    href = urljoin(self.BASE, href)

                if not date_text:
                    continue

                recent, parsed = is_within_24h(date_text, cutoff)
                if not recent and parsed:
                    continue
                if is_noise(title):
                    log_exclusion(title, href, "noise_pattern", "IRDAI")
                    continue
                if not is_relevant(title, "", "official"):
                    # For IRDAI, lower bar — insurance source
                    if not any(w in title.lower() for w in ["insurance", "irdai", "irda", "premium", "claim", "policy", "circular"]):
                        log_exclusion(title, href, "not_relevant_even_lenient", "IRDAI")
                        continue

                updates.append(build_update(
                    regulator="IRDAI", title=title, description=title,
                    url=href, pub_date=date_text,
                    source_type="official", source_name="IRDAI",
                    date_parsed=parsed,
                ))

        log.info(f"  IRDAI: {len(updates)} relevant items")
        return updates


# ---------------------------------------------------------------------------
# AMFI SCRAPER (NEW in v3)
# ---------------------------------------------------------------------------
class AMFIScraper(BaseFetcher):
    """Scrapes amfiindia.com for circulars."""
    CIRCULARS_URL = "https://www.amfiindia.com/Themes/Theme1/downloads/AMFI_Circulars.aspx"
    BASE = "https://www.amfiindia.com"

    def scrape(self, cutoff: datetime) -> list[RegUpdate]:
        log.info("Scraping AMFI...")
        updates = []
        s = self.soup(self.CIRCULARS_URL)
        if not s:
            return updates

        rows = s.select("table tr, .gridView tr")
        for row in rows[:20]:
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

            recent, parsed = is_within_24h(date_text, cutoff)
            if not recent and parsed:
                continue
            if is_noise(title):
                log_exclusion(title, href, "noise_pattern", "AMFI")
                continue

            updates.append(build_update(
                regulator="AMFI", title=title, description=title,
                url=href, pub_date=date_text,
                source_type="official", source_name="AMFI",
                date_parsed=parsed,
            ))

        log.info(f"  AMFI: {len(updates)} relevant items")
        return updates


# ---------------------------------------------------------------------------
# PIB SCRAPER (NEW in v3)
# ---------------------------------------------------------------------------
class PIBScraper(BaseFetcher):
    """Scrapes PIB RSS for finance ministry press releases."""
    RSS_URL = "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3"  # Finance Ministry
    BASE = "https://pib.gov.in"

    def scrape(self, cutoff: datetime) -> list[RegUpdate]:
        log.info("Scraping PIB (Finance Ministry)...")
        updates = []

        for item in self.parse_rss(self.RSS_URL)[:20]:
            title = item["title"]
            desc = re.sub(r'<[^>]+>', '', item.get("description", "")).strip()
            date_text = item.get("date", "")

            recent, parsed = is_within_24h(date_text, cutoff)
            if not recent and parsed:
                continue
            if not passes_filters(title, desc, "", "PIB", "official"):
                continue

            updates.append(build_update(
                regulator="MoF/PIB", title=title, description=desc,
                url=item["link"], pub_date=date_text,
                source_type="official", source_name="PIB",
                date_parsed=parsed,
            ))

        log.info(f"  PIB: {len(updates)} relevant items")
        return updates


# ---------------------------------------------------------------------------
# NEWS SCRAPER (v3 — expanded to 20+ feeds with source tiers)
# ---------------------------------------------------------------------------
class NewsScraper(BaseFetcher):
    FEEDS = {
        # Tier 1 — high-quality personal finance sources
        "Mint_Money": ("https://www.livemint.com/rss/money", "tier1_news"),
        "Mint_Economy": ("https://www.livemint.com/rss/economy", "tier1_news"),
        "Mint_Market": ("https://www.livemint.com/rss/markets", "tier1_news"),
        "ET_MF": ("https://economictimes.indiatimes.com/wealth/mutual-funds/rssfeeds/46267806.cms", "tier1_news"),
        "ET_Tax": ("https://economictimes.indiatimes.com/wealth/tax/rssfeeds/46266529.cms", "tier1_news"),
        "ET_Invest": ("https://economictimes.indiatimes.com/wealth/invest/rssfeeds/46267805.cms", "tier1_news"),
        "ET_Insurance": ("https://economictimes.indiatimes.com/wealth/insure/rssfeeds/46267684.cms", "tier1_news"),
        "ET_Save": ("https://economictimes.indiatimes.com/wealth/save/rssfeeds/46267453.cms", "tier1_news"),
        "ET_RealEstate": ("https://economictimes.indiatimes.com/wealth/real-estate/rssfeeds/46268020.cms", "tier1_news"),
        "BusinessStandard_Economy": ("https://www.business-standard.com/rss/economy-102.rss", "tier1_news"),
        "BusinessStandard_Markets": ("https://www.business-standard.com/rss/markets-106.rss", "tier1_news"),
        "BusinessStandard_PF": ("https://www.business-standard.com/rss/pf-702.rss", "tier1_news"),
        "CNBCTV18": ("https://www.cnbctv18.com/commonfeeds/v1/cne/rss/economy-gcppn.xml", "tier1_news"),

        # Tier 2 — broader coverage
        "Moneycontrol": ("https://www.moneycontrol.com/rss/MCtopnews.xml", "tier2_news"),
        "Moneycontrol_MF": ("https://www.moneycontrol.com/rss/mutualfunds.xml", "tier2_news"),
        "Moneycontrol_Tax": ("https://www.moneycontrol.com/rss/incometax.xml", "tier2_news"),
        "NDTV_Business": ("https://feeds.feedburner.com/ndtvprofit-latest", "tier2_news"),
        "FE_PF": ("https://www.financialexpress.com/money/feed/", "tier2_news"),
        "FE_Economy": ("https://www.financialexpress.com/economy/feed/", "tier2_news"),
        "VROnline": ("https://www.valueresearchonline.com/rss/", "tier2_news"),
    }

    def scrape(self, cutoff: datetime) -> list[RegUpdate]:
        log.info("Scraping news feeds (20+ sources)...")
        updates = []

        for source_name, (feed_url, source_tier) in self.FEEDS.items():
            items = self.parse_rss(feed_url)
            for item in items[:15]:
                title = item.get("title", "")
                desc = re.sub(r'<[^>]+>', '', item.get("description", "")).strip()[:500]
                date_text = item.get("date", "")

                recent, parsed = is_within_24h(date_text, cutoff)
                if not recent and parsed:
                    continue

                # For news, also allow items where date couldn't be parsed
                # (RSS feeds usually have dates, so missing = probably old — skip)
                if not parsed and date_text:
                    continue

                if not is_relevant(title, desc, source_tier):
                    log_exclusion(title, item.get("link", ""), "not_relevant", source_name)
                    continue
                if is_noise(title):
                    log_exclusion(title, item.get("link", ""), "noise_pattern", source_name)
                    continue

                regulator = self._detect_regulator(f"{title} {desc}")

                updates.append(build_update(
                    regulator=regulator, title=title, description=desc,
                    url=item.get("link", ""), pub_date=date_text,
                    source_type=source_tier, source_name=source_name,
                    date_parsed=parsed,
                ))

        log.info(f"  News: {len(updates)} relevant items")
        return updates

    def _detect_regulator(self, text: str) -> str:
        t = text.upper()
        for reg in ["SEBI", "RBI", "IRDAI", "IRDA", "PFRDA", "CBDT", "AMFI", "EPFO", "PIB"]:
            if reg in t:
                return reg.replace("IRDA", "IRDAI")
        return "MoF/Other"


# ---------------------------------------------------------------------------
# DEDUP + CLUSTERING (v3 — similarity-based)
# ---------------------------------------------------------------------------
def title_similarity(a: str, b: str) -> float:
    """Compute similarity between two titles (0-1)."""
    a_clean = re.sub(r'[^a-z0-9\s]', '', a.lower())
    b_clean = re.sub(r'[^a-z0-9\s]', '', b.lower())
    return SequenceMatcher(None, a_clean, b_clean).ratio()


def cluster_and_dedup(updates: list[RegUpdate], similarity_threshold: float = 0.55) -> list[RegUpdate]:
    """
    Cluster similar items together. Keep the best item per cluster
    (prefer official > tier1 > tier2, then highest relevance_score).
    """
    if not updates:
        return []

    clusters: list[list[int]] = []
    assigned = set()

    for i in range(len(updates)):
        if i in assigned:
            continue
        cluster = [i]
        assigned.add(i)
        for j in range(i + 1, len(updates)):
            if j in assigned:
                continue
            if title_similarity(updates[i].title, updates[j].title) >= similarity_threshold:
                cluster.append(j)
                assigned.add(j)
        clusters.append(cluster)

    # Pick best per cluster
    tier_order = {"official": 0, "tier1_news": 1, "tier2_news": 2, "blog": 3}
    deduped = []

    for cluster in clusters:
        items = [updates[idx] for idx in cluster]
        # Sort: official first, then highest score
        items.sort(key=lambda u: (tier_order.get(u.source_type, 9), -u.relevance_score))
        primary = items[0]

        # Generate cluster ID
        primary.cluster_id = hashlib.md5(primary.title[:50].lower().encode()).hexdigest()[:8]

        # Track other sources
        if len(items) > 1:
            primary.also_covered_by = [
                f"{it.source_name}" for it in items[1:]
            ]

        deduped.append(primary)

    return deduped


def clean_title(title: str) -> str:
    title = re.sub(r'\s*\[Last amended on.*?\]', '', title)
    title = re.sub(r'\s*\(Last amended.*?\)', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    if len(title) > 150:
        title = title[:147] + "..."
    return title


# ---------------------------------------------------------------------------
# MARKDOWN BRIEFING (v3 — with content ideation)
# ---------------------------------------------------------------------------
def format_md(updates: list[RegUpdate], now: datetime, date_str: str, uncertain_date_items: list[RegUpdate]) -> str:
    lines = []
    lines.append(f"# Daily Regulatory & Content Intelligence Brief — {date_str}")
    lines.append(f"*Generated: {now.strftime('%Y-%m-%d %H:%M IST')} | Novelty Wealth*\n")

    if not updates and not uncertain_date_items:
        lines.append("> No material regulatory updates in the last 24 hours relevant to personal finance.\n")
        lines.append("*Check back tomorrow. Markets are quiet today.*")
        return "\n".join(lines)

    high = [u for u in updates if u.relevance == "HIGH"]
    med = [u for u in updates if u.relevance == "MEDIUM"]

    # Pulse
    lines.append(f"**Today's Pulse:** {len(high)} high-priority | {len(med)} medium | {len(updates)} total\n")

    if high:
        lines.append(f"> **Top action:** {high[0].title[:100]}\n")

    # === TOP 3 CONTENT OPPORTUNITIES ===
    by_engagement = sorted(updates, key=lambda u: -u.engagement_potential)[:3]
    if by_engagement:
        lines.append("## 📢 Top Content Opportunities\n")
        for i, u in enumerate(by_engagement, 1):
            lines.append(f"**{i}. {u.title[:80]}**")
            lines.append(f"   - Engagement: {u.engagement_potential}/10 | Retail Impact: {u.retail_user_impact}/10 | NW Angle: {u.nw_angle}")
            lines.append(f"   - Angle: *{u.content_angle}*")
            lines.append(f"   - Formats: {', '.join(u.possible_content_formats)} | Audience: {', '.join(u.user_segment_tags)}")
            lines.append("")

    # === HIGH PRIORITY ===
    if high:
        lines.append("## 🔴 High Priority — Action Required\n")
        for i, u in enumerate(high, 1):
            lines.append(f"### {i}. {u.title}\n")
            lines.append(f"**Regulator:** {u.regulator} | **Category:** {u.category} | **Urgency:** {u.urgency}")
            if u.circular_ref:
                lines.append(f"**Ref:** `{u.circular_ref}`")
            lines.append(f"\n{u.summary}\n")
            lines.append(f"**📊 Scores:** Regulatory {u.regulatory_importance}/10 | Retail Impact {u.retail_user_impact}/10 | Actionability {u.actionability}/10 | Engagement {u.engagement_potential}/10")
            lines.append(f"**👤 Who's affected:** {', '.join(u.affected_segments)}")
            lines.append(f"**🏷️ Tags:** {', '.join(u.topic_tags)} | Segments: {', '.join(u.user_segment_tags)}")
            lines.append(f"**💡 User impact:** {u.user_impact}")
            lines.append(f"**📝 Content angle:** {u.content_angle} ({u.nw_angle})")
            lines.append(f"**📦 Formats:** {', '.join(u.possible_content_formats)} | Maturity: {u.story_maturity}")
            if u.action_type != "none":
                deadline_str = f" (deadline: {u.action_deadline})" if u.action_deadline else ""
                lines.append(f"**⚡ Action:** {u.action_type}{deadline_str}")
            if u.also_covered_by:
                lines.append(f"**Also covered by:** {', '.join(u.also_covered_by)}")
            src_label = u.source_tier.replace("_", " ").title()
            lines.append(f"\n[{src_label} Source]({u.url})\n")
            lines.append("---\n")

    # === MEDIUM PRIORITY ===
    if med:
        lines.append("## 🟡 Medium Priority — Monitor\n")
        lines.append("| # | Regulator | Update | Category | Reg | Impact | Action | Engage | Source |")
        lines.append("|---|-----------|--------|----------|-----|--------|--------|--------|--------|")
        for i, u in enumerate(med, 1):
            src = f"[Link]({u.url})"
            lines.append(f"| {i} | {u.regulator} | {u.title[:70]} | {u.category} | {u.regulatory_importance} | {u.retail_user_impact} | {u.actionability} | {u.engagement_potential} | {src} |")
        lines.append("")

    # === UNCERTAIN DATE ITEMS ===
    if uncertain_date_items:
        lines.append("## ⚠️ Date Unverified — Manual Review Needed\n")
        lines.append("*These items could not have their dates parsed. They may be relevant.*\n")
        for u in uncertain_date_items[:5]:
            lines.append(f"- **[{u.regulator}]** {u.title[:80]} — [Source]({u.url})")
        lines.append("")

    # Footer
    lines.append(f"\n---\n*Covers: SEBI, RBI, IRDAI, PFRDA, CBDT, AMFI, PIB/MoF | Last 24 hours*")
    lines.append(f"*Sources: 7 regulators + 20 news feeds | Clustered & deduplicated*")
    lines.append(f"*Novelty Wealth (SEBI RIA: INA000019415)*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SCRAPER HEALTH MONITORING (v3)
# ---------------------------------------------------------------------------
def update_health(scraper_results: dict[str, int], health_file: Path):
    """Track per-scraper result counts to detect structural failures."""
    history = {}
    if health_file.exists():
        try:
            history = json.loads(health_file.read_text())
        except Exception:
            history = {}

    today_key = date.today().isoformat()
    if today_key not in history:
        history[today_key] = {}
    history[today_key].update(scraper_results)

    # Keep last 30 days
    cutoff_key = (date.today() - timedelta(days=30)).isoformat()
    history = {k: v for k, v in history.items() if k >= cutoff_key}

    health_file.write_text(json.dumps(history, indent=2))

    # Alert on consecutive zeros
    alerts = []
    for scraper_name in scraper_results:
        consecutive_zeros = 0
        for day_key in sorted(history.keys(), reverse=True):
            if history[day_key].get(scraper_name, 0) == 0:
                consecutive_zeros += 1
            else:
                break
        if consecutive_zeros >= 3:
            alerts.append(f"⚠️  {scraper_name} has returned 0 results for {consecutive_zeros} consecutive days — possible page structure change")

    for alert in alerts:
        log.warning(alert)

    return alerts


# ---------------------------------------------------------------------------
# ORCHESTRATOR (v3)
# ---------------------------------------------------------------------------
class RegulatoryMonitor:
    def __init__(self):
        # Runtime computation (not import-time)
        self.now = datetime.now(IST)
        self.today = self.now.date()
        self.cutoff = self.now - timedelta(hours=24)
        self.date_str = self.today.isoformat()
        self.updates: list[RegUpdate] = []
        self.uncertain_date_items: list[RegUpdate] = []

    def run(self):
        log.info("=" * 60)
        log.info(f"Regulatory & Content Intelligence Monitor v3 — {self.date_str}")
        log.info(f"Cutoff: {self.cutoff.strftime('%Y-%m-%d %H:%M IST')}")
        log.info("=" * 60)

        scrapers = [
            ("SEBI", SEBIScraper()),
            ("RBI", RBIScraper()),
            ("PFRDA", PFRDAScraper()),
            ("CBDT", CBDTScraper()),
            ("IRDAI", IRDAIScraper()),
            ("AMFI", AMFIScraper()),
            ("PIB", PIBScraper()),
            ("News", NewsScraper()),
        ]

        scraper_counts = {}
        all_items = []

        for name, scraper in scrapers:
            try:
                items = scraper.scrape(self.cutoff)
                scraper_counts[name] = len(items)
                all_items.extend(items)
            except Exception as e:
                log.error(f"  {name} scraper failed: {e}")
                scraper_counts[name] = -1  # -1 = error

        # Separate uncertain-date items
        self.uncertain_date_items = [u for u in all_items if not u.date_parsed]
        dated_items = [u for u in all_items if u.date_parsed]

        # Cluster and dedup
        before = len(dated_items)
        self.updates = cluster_and_dedup(dated_items)
        log.info(f"Cluster + dedup: {before} -> {len(self.updates)}")

        # Clean titles
        for u in self.updates:
            u.title = clean_title(u.title)

        # Sort: HIGH first, then by engagement score, then by source tier
        tier_order = {"official": 0, "tier1_news": 1, "tier2_news": 2, "blog": 3}
        level_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        self.updates.sort(key=lambda u: (
            level_order.get(u.relevance, 3),
            -u.engagement_score,
            tier_order.get(u.source_tier, 9),
        ))

        # Store LOW items separately (v3.1 — keep for trend formation, suppress from brief)
        self.low_items = [u for u in self.updates if u.relevance == "LOW"]

        # Drop LOW from primary output
        self.updates = [u for u in self.updates if u.relevance in ("HIGH", "MEDIUM")]

        # Write outputs
        self._write()

        # Update trend memory (v3.1)
        self._update_trend_memory()

        # Health monitoring
        health_alerts = update_health(scraper_counts, HEALTH_FILE)

        high_n = sum(1 for u in self.updates if u.relevance == "HIGH")
        exclusions = get_exclusion_log()
        log.info("=" * 60)
        log.info(f"Done: {len(self.updates)} updates ({high_n} high-priority)")
        if self.low_items:
            log.info(f"  + {len(self.low_items)} LOW items stored for trend tracking")
        if self.uncertain_date_items:
            log.info(f"  + {len(self.uncertain_date_items)} items with unparseable dates (flagged for review)")
        if exclusions:
            log.info(f"  + {len(exclusions)} items excluded (logged for filter tuning)")
        if health_alerts:
            for alert in health_alerts:
                log.info(f"  {alert}")
        log.info("=" * 60)

    def _update_trend_memory(self):
        """Maintain rolling 7-day + 30-day topic tag counts for trend detection."""
        trend_file = DATA_DIR / "trend_memory.json"
        memory = {}
        if trend_file.exists():
            try:
                memory = json.loads(trend_file.read_text())
            except Exception:
                memory = {}

        # Add today's topic tags
        today_tags = {}
        for u in self.updates + self.low_items:
            for tag in u.topic_tags:
                today_tags[tag] = today_tags.get(tag, 0) + 1

        memory[self.date_str] = today_tags

        # Keep 30 days
        cutoff_key = (self.today - timedelta(days=30)).isoformat()
        memory = {k: v for k, v in memory.items() if k >= cutoff_key}

        trend_file.write_text(json.dumps(memory, indent=2))

        # Detect rising trends (appeared 3+ of last 7 days)
        last_7_keys = sorted(memory.keys(), reverse=True)[:7]
        tag_day_count = {}
        for day_key in last_7_keys:
            for tag in memory[day_key]:
                tag_day_count[tag] = tag_day_count.get(tag, 0) + 1

        rising = {tag: count for tag, count in tag_day_count.items() if count >= 3}
        if rising:
            log.info(f"  📈 Rising trends (3+ days in last 7): {rising}")
        self.rising_trends = rising

    def _write(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        BRIEFINGS_DIR.mkdir(parents=True, exist_ok=True)

        # 4-axis ranking views (v3.1)
        by_retail_impact = sorted(self.updates, key=lambda u: -u.retail_user_impact)[:5]
        by_engagement = sorted(self.updates, key=lambda u: -u.engagement_potential)[:5]
        by_actionability = sorted(self.updates, key=lambda u: -u.actionability)[:5]
        by_regulatory = sorted(self.updates, key=lambda u: -u.regulatory_importance)[:5]

        output = {
            "version": "v3.1",
            "date": self.date_str,
            "generated": self.now.isoformat(),
            "cutoff": self.cutoff.isoformat(),
            "total": len(self.updates),
            "high_priority": sum(1 for u in self.updates if u.relevance == "HIGH"),
            "medium_priority": sum(1 for u in self.updates if u.relevance == "MEDIUM"),

            # === RANKING VIEWS (v3.1) ===
            "views": {
                "retail_users_most_affected": [
                    {"title": u.title, "retail_user_impact": u.retail_user_impact, "user_impact": u.user_impact}
                    for u in by_retail_impact
                ],
                "best_content_opportunities": [
                    {"title": u.title, "engagement_potential": u.engagement_potential,
                     "content_angle": u.content_angle, "possible_formats": u.possible_content_formats}
                    for u in by_engagement
                ],
                "action_required_items": [
                    {"title": u.title, "actionability": u.actionability,
                     "action_type": u.action_type, "action_deadline": u.action_deadline}
                    for u in by_actionability if u.actionability >= 3
                ],
                "most_significant_regulatory": [
                    {"title": u.title, "regulatory_importance": u.regulatory_importance, "regulator": u.regulator}
                    for u in by_regulatory
                ],
            },

            "updates": [asdict(u) for u in self.updates],
            "low_items": [asdict(u) for u in self.low_items[:20]],
            "uncertain_date_items": [asdict(u) for u in self.uncertain_date_items[:10]],
            "exclusion_log": get_exclusion_log()[:50],
            "scraper_meta": {
                "scrapers_run": 8,
                "sources": "SEBI, RBI, PFRDA, CBDT, IRDAI, AMFI, PIB, 20+ news feeds",
            },
        }

        with open(LATEST_FILE, "w") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        with open(BRIEFINGS_DIR / f"{self.date_str}.json", "w") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        md = format_md(self.updates, self.now, self.date_str, self.uncertain_date_items)
        with open(BRIEFINGS_DIR / f"{self.date_str}.md", "w") as f:
            f.write(md)

        log.info(f"  Written: {LATEST_FILE}")
        log.info(f"  Written: {BRIEFINGS_DIR / self.date_str}.json + .md")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    RegulatoryMonitor().run()
