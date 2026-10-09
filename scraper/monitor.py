"""
Indian Financial Regulatory & Content Intelligence Monitor v4
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

v4 changes:
  - Sources moved to scraper/sources.yaml (add a site with one entry, no code)
  - One generic scraper for RSS feeds and HTML listing pages
  - Date-only sources (most regulators) no longer dropped by the 24h filter
  - Seen store: no repeats across days, and dateless sources now work
  - Full article text used for scoring and summaries (never stored)
  - Per-source health in data/feed_health.json (dead feeds raise an alert)
  - CLI: --check-sources, --dry-run, --no-fulltext, --only NAME

Output: data/briefings/{date}.json + .md + data/latest.json
"""

import argparse
import html as html_lib
import json
import re
import logging
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from urllib.parse import urljoin, urlparse
from difflib import SequenceMatcher
import time
import hashlib

import requests
import yaml
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DATA_DIR = Path("data")
BRIEFINGS_DIR = DATA_DIR / "briefings"
LATEST_FILE = DATA_DIR / "latest.json"
HEALTH_FILE = DATA_DIR / "scraper_health.json"        # per-group counts (kept for history)
FEED_HEALTH_FILE = DATA_DIR / "feed_health.json"      # per-source stats (v4)
SEEN_FILE = DATA_DIR / "seen_items.json"              # what was already reported (v4)
SOURCES_FILE = Path(__file__).resolve().parent / "sources.yaml"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/rss+xml;q=0.9,*/*;q=0.8",
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

    # Regulator housekeeping (v4 — surfaced once official listings were fixed)
    r"(?i)\b(recruitment|request for proposal|\brfp\b|tender|pre-?bid|corrigendum|expression of interest|empanel)",
    r"(?i)\b(in the matter of|appeal no\.?|recovery certificate|summons|notice of demand)\b",
    r"(?i)\b(awareness program|felicitation|opens? a (local|regional) office|hindi (pakhwada|diwas))\b",
    r"(?i)\b(auction of (state )?government securities|treasury bills?:|money market operations|result of the .*auction)\b",

    r"(?i)^(attachment|annexure|enclosure)\b",
    r"(?i)\b(variable rate (reverse )?repo|vrrr?\b|turnover data|weekly statistical supplement|reserve money for the week|sectoral deployment of bank credit)",
    r"(?i)\b(imposes monetary penalty|directions under section 35|cancels the licen[cs]e|certificate of registration)\b",
    r"(?i)\b(surveillance measure|\b(st-|lt-)?asm\b|\bgsm\b|mwpl|client limits|mock trading|availability of .* on nse)\b",
    r"(?i)\b(surrender of trading member|trade for trade|listing of (equity shares|units|securities)|suspension of trading|change in name of)\b",

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

# Build flat lookup for fast matching (one entry per keyword, highest weight wins)
_kw_weights: dict[str, int] = {}
for canonical, weight, variants in KEYWORD_TAXONOMY:
    for kw in [canonical, *variants]:
        _kw_weights[kw.lower()] = max(weight, _kw_weights.get(kw.lower(), 0))
_KEYWORD_LOOKUP: list[tuple[str, int]] = list(_kw_weights.items())

# v4: match whole words only. Plain substring matching scored "premium" as EMI,
# "criteria" as RIA, "arbitration" as ITR and "international" as NAV.
_KEYWORD_PATTERNS: list[tuple[str, int, "re.Pattern"]] = [
    (kw, weight, re.compile(rf"(?<![a-z0-9]){re.escape(kw)}s?(?![a-z0-9])"))
    for kw, weight in _KEYWORD_LOOKUP
]


def match_keywords(text: str) -> list[tuple[str, int]]:
    """Whole-word keyword matches in `text` as (keyword, weight)."""
    lowered = text.lower()
    hits = [(kw, weight) for kw, weight, pattern in _KEYWORD_PATTERNS if pattern.search(lowered)]
    names = {kw for kw, _ in hits}
    # "mutual fund" already matches "mutual funds"; do not count the plural twice
    return [(kw, w) for kw, w in hits if not (kw.endswith("s") and kw[:-1] in names)]

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

    for kw, weight in match_keywords(combined):
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


def keyword_score(title: str, description: str = "") -> int:
    """
    Relevance from keywords alone, with no source-tier bonus.
    v4: used as the entry gate. The tier bonus used to let every item from an
    official or tier-1 source through, even with zero keyword matches.
    """
    return compute_relevance_score(title, description, "news")


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

    # === FULL TEXT (v4) — derived signals only, the article body is never stored ===
    fulltext_status: str = ""       # ok / feed / paywalled / blocked / not_html / error / "" (not attempted)
    word_count: int = 0
    fulltext_keywords: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# DATE PARSING (v3 — datetime precision with timezone)
# ---------------------------------------------------------------------------
def parse_datetime_precise(text: str) -> tuple[Optional[datetime], bool]:
    """
    Parse into a timezone-aware datetime.
    Returns (datetime, has_time). has_time is False when the source only gave a date.
    Naive timestamps are treated as IST.
    """
    text = (text or "").strip()
    if not text:
        return None, False

    def aware(dt: datetime) -> datetime:
        return dt if dt.tzinfo else dt.replace(tzinfo=IST)

    def timed(dt: datetime) -> tuple[datetime, bool]:
        dt = aware(dt)
        # Exactly midnight means the source only knows the day (NSE, some CMS feeds)
        return dt, not (dt.hour == 0 and dt.minute == 0 and dt.second == 0)

    # RFC 822 (most RSS feeds). Handles GMT / +0530 / missing zone correctly.
    if re.search(r"\d{1,2}:\d{2}", text):
        try:
            return timed(parsedate_to_datetime(text))
        except (TypeError, ValueError, IndexError):
            pass
        try:
            return timed(datetime.fromisoformat(text.replace("Z", "+00:00")))
        except ValueError:
            pass
        for fmt in ("%d %b %Y %H:%M:%S", "%d-%b-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%d %b %Y %H:%M", "%B %d, %Y %H:%M", "%b %d, %Y %H:%M", "%d-%m-%Y %H:%M"):
            try:
                return timed(datetime.strptime(text, fmt))
            except ValueError:
                continue

    d = parse_date_only(text)
    if d:
        return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=IST), False
    return None, False


def parse_datetime(text: str) -> Optional[datetime]:
    """Parse into a timezone-aware datetime. Returns None if unparseable."""
    return parse_datetime_precise(text)[0]


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
            d = datetime.strptime(text, fmt).date()
        except ValueError:
            continue
        return d if 2000 <= d.year <= 2100 else None

    # Extract date-like substring
    # (v4: the "!= text" guards stop an endless loop on text that looks like
    # a date but is not one, e.g. "12 Regular 2026")
    m = re.search(r'(\d{1,2}[\s\-/\.]\w{3,9}[\s\-/\.,]+\d{4})', text)
    if m and m.group(1) != text:
        found = parse_date_only(m.group(1))
        if found:
            return found

    m = re.search(r'(\w{3,9}\s+\d{1,2},?\s+\d{4})', text)
    if m and m.group(1) != text:
        return parse_date_only(m.group(1))

    return None


def is_within_24h(date_text: str, cutoff: datetime) -> tuple[bool, bool]:
    """
    Returns (is_recent, date_was_parsed).
    If date can't be parsed, returns (False, False).

    v4 fix: when a source gives a date with no time (most regulator pages),
    compare calendar days. Before this, "8 Oct" was read as 8 Oct 00:00 and
    always fell just outside a 24h window, so official circulars never showed up.
    Repeats across days are prevented by the seen store.
    """
    dt, has_time = parse_datetime_precise(date_text)
    if dt is None:
        return False, False
    now = datetime.now(IST)
    if dt > now + timedelta(days=2):
        return False, True      # future date = an effective date, not a publish date
    if has_time:
        return dt >= cutoff, True
    return dt.date() >= cutoff.astimezone(IST).date(), True


# ---------------------------------------------------------------------------
# BASE FETCHER (v3 — with retry + backoff)
# ---------------------------------------------------------------------------
_host_locks: dict[str, threading.Lock] = {}
_host_last: dict[str, float] = {}
_host_guard = threading.Lock()
HOST_DELAY = 1.0   # seconds between two requests to the same site


def _polite_wait(url: str):
    """Serialize and space out requests per host, so parallel runs stay polite."""
    host = urlparse(url).netloc
    with _host_guard:
        lock = _host_locks.setdefault(host, threading.Lock())
    with lock:
        wait = HOST_DELAY - (time.time() - _host_last.get(host, 0))
        if wait > 0:
            time.sleep(wait)
        _host_last[host] = time.time()


class BaseFetcher:
    MAX_RETRIES = 2
    RETRY_DELAY = 3  # seconds

    def fetch(self, url: str, timeout: int = 20) -> tuple[int, Optional[bytes]]:
        """GET with retry. Returns (http_status, body bytes or None). Status 0 = no response."""
        status = 0
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                _polite_wait(url)
                resp = requests.get(url, headers=HEADERS, timeout=timeout)
                status = resp.status_code
                if status in (401, 403, 404, 410, 451):
                    return status, None     # retrying will not help
                if status in (418, 429):    # rate limiter (RBI answers 418 when hit too fast)
                    time.sleep(8 * (attempt + 1))
                resp.raise_for_status()
                return status, resp.content
            except Exception as e:
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
                else:
                    log.debug(f"  Failed after {self.MAX_RETRIES+1} attempts: {url} -- {e}")
        return status, None

    def get(self, url: str, timeout: int = 20) -> Optional[str]:
        _, content = self.fetch(url, timeout)
        return content.decode("utf-8", errors="replace") if content is not None else None

    def soup(self, url: str) -> Optional[BeautifulSoup]:
        _, content = self.fetch(url)
        return BeautifulSoup(content, "html.parser") if content else None


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower() if isinstance(tag, str) else ""


def parse_feed(content: bytes) -> list[dict]:
    """
    Parse RSS 2.0 / Atom / RDF into dicts: title, link, date, description, content.
    Works on raw bytes (so encodings and BOMs are handled) and falls back to a
    forgiving parser when the XML is malformed.
    """
    items = []
    try:
        root = ET.fromstring(content.lstrip())
    except ET.ParseError:
        soup = BeautifulSoup(content, "xml")
        for node in soup.find_all(["item", "entry"]):
            def text_of(*names):
                for n in names:
                    el = node.find(n)
                    if el is not None and el.get_text(strip=True):
                        return el.get_text(strip=True)
                return ""
            link_el = node.find("link")
            link = (link_el.get("href") or link_el.get_text(strip=True)) if link_el is not None else ""
            items.append({
                "title": text_of("title"), "link": link,
                "date": text_of("pubDate", "published", "updated", "date"),
                "description": text_of("description", "summary"),
                "content": text_of("encoded", "content"),
            })
        return items

    for node in root.iter():
        if _local(node.tag) not in ("item", "entry"):
            continue
        rec = {"title": "", "link": "", "date": "", "description": "", "content": ""}
        for child in node:
            name, text = _local(child.tag), (child.text or "").strip()
            if name == "title" and not rec["title"]:
                rec["title"] = text
            elif name == "link":
                href = child.get("href")
                if href and child.get("rel", "alternate") == "alternate":
                    rec["link"] = href.strip()
                elif text and not rec["link"]:
                    rec["link"] = text
            elif name in ("pubdate", "published", "date") and text:
                rec["date"] = text
            elif name == "updated" and text and not rec["date"]:
                rec["date"] = text
            elif name in ("description", "summary") and not rec["description"]:
                rec["description"] = text
            elif name in ("encoded", "content") and text:
                rec["content"] = text
        if not rec["link"]:
            guid = next((c.text for c in node if _local(c.tag) in ("guid", "id") and c.text), "")
            if guid and guid.strip().startswith("http"):
                rec["link"] = guid.strip()
        items.append(rec)
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

    # Source tier (v4: comes straight from sources.yaml)
    source_tier = source_type if source_type in ("official", "tier1_news", "tier2_news", "blog") else "tier2_news"

    # Matched keywords
    matched = [kw for kw, _ in match_keywords(combined)]

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
# SOURCE CONFIG (v4 — sources live in scraper/sources.yaml, not in code)
# ---------------------------------------------------------------------------
VALID_TIERS = ("official", "tier1_news", "tier2_news", "blog")


@dataclass
class Source:
    name: str
    url: str
    type: str = "rss"               # rss | listing
    tier: str = "tier2_news"        # official | tier1_news | tier2_news | blog
    group: str = "News"             # health roll-up bucket (SEBI, RBI, ..., News)
    regulator: str = ""             # fixed regulator label; empty = detect from text
    enabled: bool = True
    max_items: int = 40
    lenient: object = False         # True = keep every non-noise item; list = fallback keywords
    min_score: int = 1              # keyword score an item needs to get in (raise for noisy feeds)
    max_keep: int = 10              # most items one source may contribute per run (best first)
    fulltext: bool = True           # allow article body fetch for this source
    strip_title_suffix: bool = False  # drop trailing " - Publisher" (aggregator feeds)
    # listing-only options
    row: str = ""                   # CSS selector for one row/card per item
    link: str = ""                  # CSS selector for the item link (default: first <a href>)
    title: str = ""                 # CSS selector for the title (default: best text in row)
    date: str = ""                  # CSS selector for the date (default: first date found in row)
    base: str = ""                  # base URL for relative links (default: source url)
    note: str = ""


def load_sources(path: Path = SOURCES_FILE) -> tuple[list[Source], dict]:
    """Read sources.yaml. Returns (enabled sources, settings dict)."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    settings = raw.get("settings", {}) or {}
    known = set(Source.__dataclass_fields__)
    sources, names = [], set()
    for entry in raw.get("sources", []) or []:
        unknown = set(entry) - known
        if unknown:
            log.warning(f"  sources.yaml: {entry.get('name', '?')} has unknown keys {sorted(unknown)} (ignored)")
        src = Source(**{k: v for k, v in entry.items() if k in known})
        if src.name in names:
            raise ValueError(f"sources.yaml: duplicate source name '{src.name}'")
        if src.tier not in VALID_TIERS:
            raise ValueError(f"sources.yaml: {src.name} has invalid tier '{src.tier}'")
        if src.type not in ("rss", "listing"):
            raise ValueError(f"sources.yaml: {src.name} has invalid type '{src.type}'")
        names.add(src.name)
        if src.enabled:
            sources.append(src)
    return sources, settings


# ---------------------------------------------------------------------------
# SEEN STORE (v4 — remembers what was already reported)
# ---------------------------------------------------------------------------
class SeenStore:
    """
    Remembers the first day each item was reported, per source.

    Why this exists:
      1. Many regulator pages give a date but no time. Those items are accepted
         for "yesterday or today", so without memory they would repeat for two days.
      2. Some sources give no date at all (PIB feed, some listing pages). For those,
         "new" means "not seen on a previous run".
    """
    KEEP_DAYS = 90

    def __init__(self, path: Path, today: date):
        self.path = path
        self.today = today.isoformat()
        self._lock = threading.Lock()
        self.data: dict[str, dict[str, str]] = {}
        if path.exists():
            try:
                self.data = json.loads(path.read_text())
            except Exception:
                log.warning(f"  Could not read {path}; starting with an empty seen store")
        self._known_sources = set(self.data)

    @staticmethod
    def key(title: str, url: str) -> str:
        return hashlib.md5(f"{url}|{title.strip().lower()[:120]}".encode()).hexdigest()[:16]

    def is_bootstrap(self, source: str) -> bool:
        """True the first time a source is ever scraped."""
        return source not in self._known_sources

    def reported_before_today(self, source: str, key: str) -> bool:
        first = self.data.get(source, {}).get(key)
        return first is not None and first < self.today

    def mark(self, source: str, key: str):
        with self._lock:
            self.data.setdefault(source, {}).setdefault(key, self.today)

    def save(self):
        cutoff = (date.fromisoformat(self.today) - timedelta(days=self.KEEP_DAYS)).isoformat()
        pruned = {
            src: {k: d for k, d in items.items() if d >= cutoff}
            for src, items in self.data.items()
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(pruned, indent=1, sort_keys=True))


# ---------------------------------------------------------------------------
# GENERIC SOURCE SCRAPER (v4 — one scraper for every RSS feed and listing page)
# ---------------------------------------------------------------------------
OFFICIAL_GRACE_DAYS = 2

_DATE_IN_TEXT = re.compile(
    r"(\d{4}-\d{2}-\d{2}"
    r"|\d{1,2}(?:st|nd|rd|th)?[\s\-/.](?:\d{1,2}|[A-Za-z]{3,9})[\s\-/.,]+\d{4}"
    r"|[A-Za-z]{3,9}\.?\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})"
)
_GENERIC_LINK_TEXT = re.compile(r"(?i)^(click here|download|view|read more|more|pdf|details|open|link)\b")


def find_date_in_text(text: str) -> str:
    """Return the first substring of `text` that parses as a date, else ''."""
    for m in _DATE_IN_TEXT.finditer(text):
        candidate = re.sub(r"(?<=\d)(st|nd|rd|th)\b", "", m.group(1))
        if parse_date_only(candidate):
            return candidate
    return ""


def detect_regulator(text: str) -> str:
    t = text.upper()
    for needle, label in [
        ("SEBI", "SEBI"), ("RBI", "RBI"), ("RESERVE BANK", "RBI"), ("IRDAI", "IRDAI"), ("IRDA", "IRDAI"),
        ("PFRDA", "PFRDA"), ("CBDT", "CBDT"), ("INCOME TAX DEPARTMENT", "CBDT"),
        ("AMFI", "AMFI"), ("EPFO", "EPFO"), ("GST COUNCIL", "GST Council"), ("PIB", "PIB"),
    ]:
        if re.search(rf"\b{re.escape(needle)}\b", t):
            return label
    return "MoF/Other"


@dataclass
class SourceResult:
    """Per-source run stats, written to data/feed_health.json."""
    name: str
    group: str
    status: str = "ok"          # ok | empty | stale | http_error | parse_error | error
    http: int = 0
    fetched: int = 0            # raw items found on the feed/page
    recent: int = 0             # of those, inside the time window
    relevant: int = 0           # of those, passed relevance filters
    detail: str = ""
    updates: list = field(default_factory=list)


class SourceScraper(BaseFetcher):
    """Scrapes one Source (RSS feed or HTML listing page) into RegUpdate items."""

    def __init__(self, seen: Optional[SeenStore] = None):
        self.seen = seen
        # url -> article text that came inside the feed itself (content:encoded)
        self.feed_bodies: dict[str, str] = {}

    # -- public -------------------------------------------------------------
    def scrape(self, src: Source, cutoff: datetime) -> SourceResult:
        res = SourceResult(name=src.name, group=src.group)
        status, content = self.fetch(src.url)
        res.http = status
        if content is None:
            res.status = "http_error"
            res.detail = f"HTTP {status}" if status else "no response"
            return res

        try:
            raw_items = self._rss_items(content) if src.type == "rss" else self._listing_items(content, src)
        except Exception as e:  # a broken page must never take the run down
            res.status = "parse_error"
            res.detail = str(e)[:150]
            return res

        res.fetched = len(raw_items)
        if not raw_items:
            res.status = "empty"
            res.detail = "page loaded but no items found (layout or feed URL may have changed)"
            return res

        # A feed that still loads but stopped publishing is as dead as a 404
        if src.tier != "official":
            dates = [parse_datetime(i.get("date", "")) for i in raw_items]
            dates = [d for d in dates if d]
            limit = STALE_AFTER_DAYS_BLOG if src.tier == "blog" else STALE_AFTER_DAYS
            if dates and max(dates) < cutoff - timedelta(days=limit):
                res.status = "stale"
                res.detail = f"newest item is from {max(dates).date().isoformat()}"

        bootstrap = self.seen.is_bootstrap(src.name) if self.seen else False
        for item in raw_items[: src.max_items]:
            upd = self._to_update(item, src, cutoff, bootstrap, res)
            if upd:
                res.updates.append(upd)
        res.relevant = len(res.updates)
        if len(res.updates) > src.max_keep:
            res.updates.sort(key=lambda u: -u.relevance_score)
            for dropped in res.updates[src.max_keep:]:
                log_exclusion(dropped.title, dropped.url, "source_cap", src.name)
            res.updates = res.updates[: src.max_keep]
        return res

    # -- item extraction ----------------------------------------------------
    def _rss_items(self, content: bytes) -> list[dict]:
        return parse_feed(content)

    def _listing_items(self, content: bytes, src: Source) -> list[dict]:
        try:
            soup = BeautifulSoup(content, "html.parser")
        except RecursionError:      # very deeply nested pages
            soup = BeautifulSoup(content, "lxml")
        base = src.base or src.url
        items, used = [], set()

        if src.row:
            rows = soup.select(src.row)
            pairs = []
            for row in rows:
                if row.name == "a" and row.get("href"):
                    link_el = row
                elif src.link:
                    link_el = row.select_one(src.link)
                else:
                    link_el = row.find("a", href=True)
                pairs.append((row, link_el))
        else:
            # No row selector: start from the links and climb to the nearest
            # ancestor that carries a date (works for card and div layouts).
            pairs = []
            for link_el in soup.select(src.link or "a[href]"):
                row = link_el
                for _ in range(5):
                    if row.parent is None or row.parent.name in ("body", "html", "[document]"):
                        break
                    row = row.parent
                    if find_date_in_text(row.get_text(" ", strip=True)[:600]):
                        break
                else:
                    row = link_el
                pairs.append((row, link_el))

        for row, link_el in pairs:
            row_text = row.get_text(" ", strip=True)
            if src.date:
                date_el = row.select_one(src.date)
                date_text = find_date_in_text(date_el.get_text(" ", strip=True)) if date_el else ""
            else:
                date_text = find_date_in_text(row_text[:800])

            href = link_el.get("href", "").strip() if link_el is not None else ""
            if href.lower().startswith(("javascript:", "#", "mailto:")):
                href = ""
            url = urljoin(base, href) if href else ""

            if src.title:
                t_el = row.select_one(src.title)
                title = t_el.get_text(" ", strip=True) if t_el else ""
            else:
                title = self._best_title(row, link_el, date_text)
            title = re.sub(r"\s+", " ", title).strip()
            if len(title) < 12:
                continue

            dedup_key = (title.lower(), url)
            if dedup_key in used:
                continue
            used.add(dedup_key)
            items.append({
                "title": title, "link": url or src.url, "date": date_text,
                "description": "", "content": "",
            })
        return items

    @staticmethod
    def _best_title(row, link_el, date_text: str) -> str:
        link_text = link_el.get_text(" ", strip=True) if link_el is not None else ""
        if len(link_text) >= 20 and not _GENERIC_LINK_TEXT.match(link_text):
            return link_text
        pieces = [s.strip() for s in row.stripped_strings]
        pieces = [
            p for p in pieces
            if len(p) >= 12 and p != date_text and not _GENERIC_LINK_TEXT.match(p)
            and not re.fullmatch(r"[\d\s\-/.,:]+", p)
        ]
        return max(pieces, key=len)[:300] if pieces else link_text

    # -- filtering + build --------------------------------------------------
    def _to_update(self, item: dict, src: Source, cutoff: datetime,
                   bootstrap: bool, res: SourceResult) -> Optional[RegUpdate]:
        title = html_lib.unescape(item.get("title", "")).strip()
        if src.strip_title_suffix:
            title = re.sub(r"\s+[-|–]\s+[^-|–]{2,40}$", "", title)
        url = item.get("link", "").strip()
        date_text = item.get("date", "").strip()
        desc = clean_html_text(item.get("description", ""))[:500]
        if not title:
            return None

        key = SeenStore.key(title, url)
        recent, parsed = is_within_24h(date_text, cutoff)
        if parsed and not recent and src.tier == "official" and self.seen is not None:
            # Regulators often upload a circular a day or two after the date printed
            # on it. Look back a little further; the seen store prevents repeats.
            recent, _ = is_within_24h(date_text, cutoff - timedelta(days=OFFICIAL_GRACE_DAYS))

        if parsed:
            if not recent:
                return None
            res.recent += 1
            if self.seen and self.seen.reported_before_today(src.name, key):
                return None
        else:
            # No usable date. "New" = not seen on an earlier run.
            if self.seen is None:
                return None
            if self.seen.reported_before_today(src.name, key):
                return None
            if bootstrap:
                # First ever run for this source: we cannot tell old from new.
                # Remember everything, report nothing (avoids a flood of stale items).
                self.seen.mark(src.name, key)
                return None
            res.recent += 1

        # Relevance gate
        is_official = src.tier == "official"
        if is_noise(title):
            log_exclusion(title, url, "noise_pattern", src.name)
            return None
        if keyword_score(title, desc) < max(int(src.min_score), 1):
            keep = False
            if src.lenient is True:
                keep = True
            elif isinstance(src.lenient, list):
                keep = any(str(w).lower() in title.lower() for w in src.lenient)
            if not keep:
                log_exclusion(title, url, "not_relevant", src.name)
                return None

        regulator = src.regulator or detect_regulator(f"{title} {desc}")
        circ_ref = extract_circular_ref(f"{title} {desc}") if is_official else ""
        pub_date = date_text if parsed else f"first seen {self.seen.today}"

        body = clean_html_text(item.get("content", ""))
        if len(body) >= 600 and url:
            self.feed_bodies[url] = body

        if self.seen:
            self.seen.mark(src.name, key)

        upd = build_update(
            regulator=regulator, title=title, description=desc or title,
            url=url, pub_date=pub_date, source_type=src.tier, source_name=src.name,
            circular_ref=circ_ref, date_parsed=True,
        )
        if is_official and upd.relevance in ("LOW", "NONE"):
            # A regulator's own notice that passed the gate is never background noise.
            upd.relevance = "MEDIUM"
        return upd


_CIRCULAR_REF_PATTERNS = [
    r"SEBI/HO/[\w/\-()]+\d",
    r"RBI/\d{4}-\d{2,4}/\d+",
    r"IRDAI?/[A-Z&]+/[\w/\-]+\d",
    r"PFRDA[/\-][\w/\-]+\d",
    r"(?i)\b(?:circular|notification)\s+no\.?\s*[\w\-/]+/\d{4}(?:-\d{2})?",
    r"\b\d{2,3}[A-Z]?/\s?(?:BP|MEM-COR)/\s?[\w ]+/\s?\d{4}-\d{2}",
]


def extract_circular_ref(text: str) -> str:
    for pattern in _CIRCULAR_REF_PATTERNS:
        m = re.search(pattern, text)
        if m:
            return re.sub(r"\s+", " ", m.group()).strip(" .,")[:80]
    return ""


def clean_html_text(text: str) -> str:
    if not text:
        return ""
    if "<" in text:
        text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", html_lib.unescape(text)).strip()


# ---------------------------------------------------------------------------
# FULL ARTICLE TEXT (v4 — used for scoring and summaries, never stored)
# ---------------------------------------------------------------------------
_PAYWALL_MARKERS = [
    "subscribe to read", "subscribe to continue", "this story is for subscribers",
    "premium article", "already a subscriber", "login to read", "sign in to read",
    "to continue reading", "exclusive to subscribers", "become a member to read",
]
_BLOCK_MARKERS = ["just a moment...", "attention required", "access denied", "are you a robot", "captcha"]


class ArticleFetcher(BaseFetcher):
    """
    Downloads an article page and extracts the body text.

    The text is held in memory for scoring and for a short summary only.
    It is never written to data/ (publisher copyright).
    If a site blocks the request or shows a paywall, we fall back to the
    headline and feed summary. We do not try to get around either.
    """
    MAX_RETRIES = 0

    def fetch_article(self, url: str) -> tuple[str, str]:
        """Returns (status, text). status: ok | paywalled | blocked | not_html | error."""
        if re.search(r"(?i)\.(pdf|docx?|xlsx?|zip)(\?|$)", url) or "news.google.com" in url:
            return "not_html", ""
        status, content = self.fetch(url, timeout=15)
        if content is None:
            return ("blocked" if status in (401, 403, 429, 451) else "error"), ""
        head = content[:3000].decode("utf-8", errors="ignore").lower()
        if content[:5] == b"%PDF-":
            return "not_html", ""
        if any(m in head for m in _BLOCK_MARKERS):
            return "blocked", ""

        soup = BeautifulSoup(content, "html.parser")
        text, free = self._from_json_ld(soup)
        if len(text) < 400:
            text = self._from_dom(soup) or text
        text = re.sub(r"\s+", " ", text).strip()

        page_text = soup.get_text(" ", strip=True).lower()
        paywalled = free is False or (
            len(text) < 1200 and any(m in page_text for m in _PAYWALL_MARKERS)
        )
        if len(text) < 200:
            return ("paywalled" if paywalled else "error"), text
        return ("paywalled" if paywalled else "ok"), text

    @staticmethod
    def _from_json_ld(soup) -> tuple[str, Optional[bool]]:
        best, free = "", None

        def walk(node):
            nonlocal best, free
            if isinstance(node, dict):
                body = node.get("articleBody")
                if isinstance(body, str) and len(body) > len(best):
                    best = body
                if "isAccessibleForFree" in node:
                    val = str(node["isAccessibleForFree"]).lower()
                    if val in ("false", "0"):
                        free = False
                    elif free is None:
                        free = True
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                walk(json.loads(script.string or ""))
            except Exception:
                continue
        return clean_html_text(best), free

    @staticmethod
    def _from_dom(soup) -> str:
        for tag in soup(["script", "style", "nav", "header", "footer", "aside", "form", "noscript", "figure"]):
            tag.decompose()
        candidates = soup.select(
            "[itemprop='articleBody'], article, .article-body, .story-content, .storyDetails, "
            ".entry-content, .post-content, .content_wrapper, .artText, main"
        )

        def para_text(el) -> str:
            return " ".join(
                p.get_text(" ", strip=True) for p in el.find_all("p")
                if len(p.get_text(strip=True)) > 40
            )

        best = max((para_text(c) for c in candidates), key=len, default="")
        if len(best) < 400:
            best = max(best, para_text(soup), key=len)
        return best


def first_sentences(text: str, max_chars: int = 320) -> str:
    """A short lead (1 to 2 sentences) for the summary field."""
    out = ""
    for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z₹\"'(])", text):
        if len(out) + len(sentence) > max_chars:
            break
        out = f"{out} {sentence}".strip()
        if len(out) > 140:
            break
    return out or text[:max_chars].rsplit(" ", 1)[0] + "..."


def enrich_with_fulltext(u: RegUpdate, body: str, status: str):
    """
    Use the article body to sharpen an item. Only derived fields are kept:
    scores, tags, regulator, circular reference, deadline, and a short lead.
    """
    u.fulltext_status = status
    if not body:
        return
    u.word_count = len(body.split())
    lead = body[:1500]
    lowered = body.lower()
    context = f"{u.title} {u.summary} {lead}"

    # Summary: only replace when the feed gave us nothing beyond the headline
    if len(u.summary) < 60 or u.summary.strip().lower() == u.title.strip().lower():
        u.summary = first_sentences(body)

    # Regulator and circular reference often sit in the body, not the headline
    if u.regulator == "MoF/Other":
        u.regulator = detect_regulator(context)
    if not u.circular_ref:
        u.circular_ref = extract_circular_ref(body[:6000])

    # Bounded relevance boost: strong keywords the article keeps coming back to
    already = set(u.matched_keywords)
    strong = []
    for kw, weight in _KEYWORD_LOOKUP:
        if weight >= 3 and kw not in already and kw not in strong and len(kw) > 3:
            if len(re.findall(rf"\b{re.escape(kw)}\b", lowered)) >= 3:
                strong.append(kw)
    u.fulltext_keywords = strong[:8]
    if strong:
        u.relevance_score += min(len(strong), 2)
        u.relevance = score_to_level(u.relevance_score)

    # Actionability and deadline: take the stronger reading
    act, act_type, deadline = compute_actionability(u.title, f"{u.summary} {lead}")
    if act > u.actionability:
        u.actionability = act
        if u.action_type in ("", "none"):
            u.action_type = act_type
    if deadline and not u.action_deadline:
        u.action_deadline = deadline
    u.action_required = u.actionability >= 5 or (u.relevance == "HIGH" and u.action_type != "none")

    # Tags: union with what the body supports
    combined = context.lower()
    u.topic_tags = sorted(set(u.topic_tags) | set(generate_topic_tags(combined)))
    seg = set(u.user_segment_tags) | set(generate_user_segment_tags(combined))
    if len(seg) > 1:
        seg.discard("all_investors")
    u.user_segment_tags = sorted(seg)
    maturity, eg = detect_story_maturity(u.title, f"{u.summary} {lead[:600]}")
    if maturity != "confirmed":
        u.story_maturity, u.evergreen_or_breaking = maturity, eg


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
def format_md(updates: list[RegUpdate], now: datetime, date_str: str, uncertain_date_items: list[RegUpdate],
              results: Optional[list] = None, source_alerts: Optional[list] = None,
              more_count: int = 0, hours: int = 24) -> str:
    results = results or []
    source_alerts = source_alerts or []
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
    official = [u for u in updates if u.source_tier == "official"]
    lines.append(f"**Today's Pulse:** {len(high)} high-priority | {len(med)} medium | {len(updates)} total "
                 f"| {len(official)} from official regulator sources\n")

    if high:
        lines.append(f"> **Top action:** {high[0].title[:100]}\n")

    # === OFFICIAL REGULATOR UPDATES (v4) ===
    if official:
        lines.append("## 🏛️ Official Regulator Updates\n")
        lines.append("| Regulator | Update | Date | Ref | Priority | Source |")
        lines.append("|-----------|--------|------|-----|----------|--------|")
        for u in official:
            title = u.title[:110].replace("|", "/")
            lines.append(f"| {u.regulator} | {title} | {u.pub_date[:22]} | {u.circular_ref or '-'} "
                         f"| {u.relevance} | [{u.source_name}]({u.url}) |")
        lines.append("")

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

    if more_count:
        lines.append(f"*{more_count} more news items ranked below the briefing cap. "
                     f"They are in the JSON under `more_items`.*\n")

    # === UNCERTAIN DATE ITEMS ===
    if uncertain_date_items:
        lines.append("## ⚠️ Date Unverified — Manual Review Needed\n")
        lines.append("*These items could not have their dates parsed. They may be relevant.*\n")
        for u in uncertain_date_items[:5]:
            lines.append(f"- **[{u.regulator}]** {u.title[:80]} — [Source]({u.url})")
        lines.append("")

    # === SOURCE HEALTH (v4) ===
    if source_alerts:
        dead = [a for a in source_alerts if a.get("dead")]
        lines.append("## 🩺 Source Health\n")
        lines.append(f"*{len(source_alerts)} of {len(results)} sources returned nothing today"
                     f"{f', {len(dead)} for 3+ runs in a row' if dead else ''}. "
                     f"Details in `data/feed_health.json`.*\n")
        for a in sorted(source_alerts, key=lambda a: -a["days_failing"])[:15]:
            mark = "DEAD" if a.get("dead") else "check"
            lines.append(f"- **{a['source']}** ({a['group']}): {a['status']}, HTTP {a['http']}, "
                         f"{a['days_failing']} run(s) [{mark}]")
        lines.append("")

    # Footer
    groups = list(dict.fromkeys(r.group for r in results if r.group != "News"))
    n_news = sum(1 for r in results if r.group == "News")
    n_ok = sum(1 for r in results if r.status == "ok")
    lines.append(f"\n---\n*Covers: {', '.join(groups) or 'configured regulators'} | Last {hours} hours*")
    lines.append(f"*Sources: {len(results)} configured ({len(results) - n_news} official pages and feeds, "
                 f"{n_news} news and research feeds), {n_ok} responding | Clustered & deduplicated*")
    lines.append(f"*Novelty Wealth (SEBI RIA: INA000019415)*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SCRAPER HEALTH MONITORING (v3)
# ---------------------------------------------------------------------------
def _load_history(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def _trim_history(history: dict, today: date, days: int = 30) -> dict:
    cutoff_key = (today - timedelta(days=days)).isoformat()
    return {k: v for k, v in history.items() if k >= cutoff_key}


def update_health(scraper_results: dict[str, int], health_file: Path, today: Optional[date] = None):
    """Per-group relevant-item counts (kept so the existing history stays comparable)."""
    today = today or datetime.now(IST).date()
    history = _load_history(health_file)
    history.setdefault(today.isoformat(), {}).update(scraper_results)
    history = _trim_history(history, today)
    health_file.write_text(json.dumps(history, indent=2))


DEAD_AFTER_DAYS = 3
STALE_AFTER_DAYS = 30        # news feed whose newest item is older than this = frozen feed
STALE_AFTER_DAYS_BLOG = 180  # blogs post less often


def update_feed_health(results: list["SourceResult"], health_file: Path, today: date) -> list[dict]:
    """
    Per-source health (v4). Records what every single feed or page returned and
    flags the ones that look broken.

    A source is "dead" when it has returned zero raw items (or an error) for
    DEAD_AFTER_DAYS runs in a row. Zero *relevant* items is normal for a quiet
    regulator and is not an alert; zero *fetched* items means the URL or the
    page layout changed.
    """
    history = _load_history(health_file)
    history[today.isoformat()] = {
        r.name: {"status": r.status, "http": r.http, "fetched": r.fetched,
                 "recent": r.recent, "relevant": r.relevant, **({"detail": r.detail} if r.detail else {})}
        for r in results
    }
    history = _trim_history(history, today)
    health_file.write_text(json.dumps(history, indent=1))

    alerts = []
    days = sorted(history.keys(), reverse=True)
    for r in results:
        streak = 0
        for day_key in days:
            entry = history[day_key].get(r.name)
            if entry is None:
                break
            if entry.get("fetched", 0) == 0 or entry.get("status") == "stale":
                streak += 1
            else:
                break
        if r.status != "ok":
            alerts.append({
                "source": r.name, "group": r.group, "status": r.status, "http": r.http,
                "days_failing": streak, "dead": streak >= DEAD_AFTER_DAYS, "detail": r.detail,
            })
    for a in alerts:
        if a["dead"]:
            log.warning(f"⚠️  DEAD SOURCE: {a['source']} has returned nothing for {a['days_failing']} runs "
                        f"({a['status']}, HTTP {a['http']}). Fix or disable it in sources.yaml.")
        elif a["status"] == "stale":
            log.warning(f"⚠️  STALE SOURCE: {a['source']} still loads but stopped publishing ({a['detail']}).")
    return alerts


# ---------------------------------------------------------------------------
# ORCHESTRATOR (v3)
# ---------------------------------------------------------------------------
class RegulatoryMonitor:
    def __init__(self, dry_run: bool = False, fulltext: bool = True, only: Optional[list[str]] = None,
                 hours: int = 24):
        # Runtime computation (not import-time)
        self.now = datetime.now(IST)
        self.today = self.now.date()
        self.hours = hours
        self.cutoff = self.now - timedelta(hours=hours)
        self.date_str = self.today.isoformat()
        self.updates: list[RegUpdate] = []
        self.low_items: list[RegUpdate] = []
        self.more_items: list[RegUpdate] = []       # news that ranked below the briefing cap
        self.uncertain_date_items: list[RegUpdate] = []
        self.dry_run = dry_run
        self.use_fulltext = fulltext
        self.sources, self.settings = load_sources()
        if only:
            wanted = {o.lower() for o in only}
            self.sources = [s for s in self.sources if s.name.lower() in wanted or s.group.lower() in wanted]
        self.source_by_name = {s.name: s for s in self.sources}
        self.results: list[SourceResult] = []
        self.source_alerts: list[dict] = []
        self.fulltext_stats: dict[str, int] = {}

    # -- scraping -----------------------------------------------------------
    def scrape_all(self, seen: Optional[SeenStore]) -> SourceScraper:
        scraper = SourceScraper(seen)
        workers = int(self.settings.get("workers", 8))

        def one(src: Source) -> SourceResult:
            try:
                return scraper.scrape(src, self.cutoff)
            except Exception as e:
                return SourceResult(name=src.name, group=src.group, status="error", detail=str(e)[:150])

        with ThreadPoolExecutor(max_workers=workers) as pool:
            self.results = list(pool.map(one, self.sources))

        for r in self.results:
            flag = "" if r.status == "ok" else f"  <-- {r.status} {r.detail}"
            log.info(f"  {r.name:<28} fetched={r.fetched:<4} recent={r.recent:<4} relevant={r.relevant:<4}{flag}")
        return scraper

    def fetch_fulltext(self, scraper: SourceScraper):
        """Pull article bodies for shortlisted news items and refine their scores."""
        cfg = self.settings.get("fulltext", {}) or {}
        if not self.use_fulltext or not cfg.get("enabled", True):
            return
        max_articles = int(cfg.get("max_articles", 80))
        fetcher = ArticleFetcher()

        targets = []
        for u in self.updates:
            src = self.source_by_name.get(u.source_name)
            if u.source_tier == "official" or src is None or not src.fulltext or not u.url:
                continue
            targets.append(u)

        # Highest-relevance items get the fetch budget first
        need_fetch = [u for u in targets if u.url not in scraper.feed_bodies]
        need_fetch.sort(key=lambda u: -u.relevance_score)
        budget = {id(u) for u in need_fetch[:max_articles]}

        def one(u: RegUpdate):
            body = scraper.feed_bodies.get(u.url, "")
            if body:
                return u, "feed", body          # the feed already carried the article
            if id(u) in budget:
                status, text = fetcher.fetch_article(u.url)
                return u, status, text
            return u, "", ""

        with ThreadPoolExecutor(max_workers=int(cfg.get("workers", 6))) as pool:
            for u, status, body in pool.map(one, targets):
                if status:
                    enrich_with_fulltext(u, body, status)
                    self.fulltext_stats[status] = self.fulltext_stats.get(status, 0) + 1
        log.info(f"Full text: {self.fulltext_stats or 'nothing fetched'}")

    # -- main ---------------------------------------------------------------
    def run(self):
        log.info("=" * 60)
        log.info(f"Regulatory & Content Intelligence Monitor v4 — {self.date_str}")
        log.info(f"Cutoff: {self.cutoff.strftime('%Y-%m-%d %H:%M IST')} | Sources: {len(self.sources)}"
                 f"{' | DRY RUN' if self.dry_run else ''}")
        log.info("=" * 60)

        seen = SeenStore(SEEN_FILE, self.today)
        scraper = self.scrape_all(seen)
        all_items = [u for r in self.results for u in r.updates]

        # Per-group counts (-1 = every source in the group failed)
        scraper_counts: dict[str, int] = {}
        for group in dict.fromkeys(r.group for r in self.results):
            group_results = [r for r in self.results if r.group == group]
            if all(r.status in ("http_error", "error", "parse_error") for r in group_results):
                scraper_counts[group] = -1
            else:
                scraper_counts[group] = sum(r.relevant for r in group_results)

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

        # Full article text for shortlisted items (may move items between levels)
        self.updates = [u for u in self.updates if u.relevance != "NONE"]
        self.fetch_fulltext(scraper)

        # Sort: HIGH first, then official before news, then by engagement score
        tier_order = {"official": 0, "tier1_news": 1, "tier2_news": 2, "blog": 3}
        level_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        self.updates.sort(key=lambda u: (
            level_order.get(u.relevance, 3),
            0 if u.source_tier == "official" else 1,
            -u.engagement_score,
            -u.relevance_score,
            tier_order.get(u.source_tier, 9),
        ))

        # Store LOW items separately (v3.1 — keep for trend formation, suppress from brief)
        self.low_items = [u for u in self.updates if u.relevance == "LOW"]

        # Drop LOW from primary output
        self.updates = [u for u in self.updates if u.relevance in ("HIGH", "MEDIUM")]

        # Briefing cap (v4): more sources must not mean an unreadable brief.
        # Official items always stay. News beyond the cap is kept in the JSON
        # under "more_items", ranked, so nothing is lost.
        max_news = int((self.settings.get("briefing", {}) or {}).get("max_news_items", 60))
        kept, news_seen = [], 0
        for u in self.updates:
            if u.source_tier == "official":
                kept.append(u)
            elif news_seen < max_news:
                kept.append(u)
                news_seen += 1
            else:
                self.more_items.append(u)
        self.updates = kept

        high_n = sum(1 for u in self.updates if u.relevance == "HIGH")
        official_n = sum(1 for u in self.updates if u.source_tier == "official")

        if self.dry_run:
            log.info("=" * 60)
            log.info(f"DRY RUN: {len(self.updates)} updates ({high_n} high-priority, {official_n} official). Nothing written.")
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        # Health first, so the briefing itself can show which sources are down
        update_health(scraper_counts, HEALTH_FILE, self.today)
        self.source_alerts = update_feed_health(self.results, FEED_HEALTH_FILE, self.today)

        # Write outputs
        self._write()
        seen.save()

        # Update trend memory (v3.1)
        self._update_trend_memory()

        exclusions = get_exclusion_log()
        log.info("=" * 60)
        log.info(f"Done: {len(self.updates)} updates ({high_n} high-priority, {official_n} from official sources)")
        if self.low_items:
            log.info(f"  + {len(self.low_items)} LOW items stored for trend tracking")
        if self.uncertain_date_items:
            log.info(f"  + {len(self.uncertain_date_items)} items with unparseable dates (flagged for review)")
        if exclusions:
            log.info(f"  + {len(exclusions)} items excluded (logged for filter tuning)")
        if self.source_alerts:
            dead = [a["source"] for a in self.source_alerts if a["dead"]]
            log.info(f"  + {len(self.source_alerts)} sources returned nothing today"
                     f"{' | DEAD: ' + ', '.join(dead) if dead else ''}")
        log.info("=" * 60)

    def check_sources(self):
        """Probe every source and print a table. Writes nothing."""
        log.info(f"Checking {len(self.sources)} sources (no files are written)...")
        self.scrape_all(SeenStore(SEEN_FILE, self.today))
        bad = [r for r in self.results if r.status != "ok"]
        print(f"\n{'SOURCE':<30}{'GROUP':<12}{'STATUS':<13}{'HTTP':<6}{'FETCHED':<9}{'RECENT':<8}{'RELEVANT'}")
        for r in sorted(self.results, key=lambda r: (r.status == "ok", r.group, r.name)):
            print(f"{r.name:<30}{r.group:<12}{r.status:<13}{r.http:<6}{r.fetched:<9}{r.recent:<8}{r.relevant}")
        print(f"\n{len(self.results) - len(bad)} of {len(self.results)} sources OK."
              + (f" Not OK: {', '.join(r.name for r in bad)}" if bad else ""))
        return 0

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
            "version": "v4.0",
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
            "more_items": [asdict(u) for u in self.more_items[:150]],
            "low_items": [asdict(u) for u in self.low_items[:20]],
            "uncertain_date_items": [asdict(u) for u in self.uncertain_date_items[:10]],
            "exclusion_log": get_exclusion_log()[:50],
            "official_items": sum(1 for u in self.updates if u.source_tier == "official"),
            "scraper_meta": {
                "sources_configured": len(self.results),
                "sources_ok": sum(1 for r in self.results if r.status == "ok"),
                "official_sources": sum(1 for s in self.sources if s.tier == "official"),
                "news_sources": sum(1 for s in self.sources if s.tier != "official"),
                "fulltext": self.fulltext_stats,
                "source_alerts": self.source_alerts,
                "per_source": {r.name: {"fetched": r.fetched, "recent": r.recent, "relevant": r.relevant,
                                        "status": r.status} for r in self.results},
            },
        }

        with open(LATEST_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        with open(BRIEFINGS_DIR / f"{self.date_str}.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        md = format_md(self.updates, self.now, self.date_str, self.uncertain_date_items,
                       results=self.results, source_alerts=self.source_alerts,
                       more_count=len(self.more_items), hours=self.hours)
        with open(BRIEFINGS_DIR / f"{self.date_str}.md", "w", encoding="utf-8") as f:
            f.write(md)

        log.info(f"  Written: {LATEST_FILE}")
        log.info(f"  Written: {BRIEFINGS_DIR / self.date_str}.json + .md")


# ---------------------------------------------------------------------------
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Indian financial regulatory monitor")
    parser.add_argument("--check-sources", action="store_true",
                        help="probe every source in sources.yaml and print a health table (writes nothing)")
    parser.add_argument("--dry-run", action="store_true", help="run the full pipeline but write no files")
    parser.add_argument("--no-fulltext", action="store_true", help="skip article body fetching")
    parser.add_argument("--only", nargs="+", metavar="NAME",
                        help="limit the run to these source names or groups (e.g. SEBI VROnline)")
    parser.add_argument("--hours", type=int, default=24,
                        help="look-back window in hours (default 24). Use 72 to catch up after a missed run")
    args = parser.parse_args(argv)

    monitor = RegulatoryMonitor(dry_run=args.dry_run, fulltext=not args.no_fulltext, only=args.only,
                                hours=args.hours)
    if args.check_sources:
        return monitor.check_sources()
    monitor.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
