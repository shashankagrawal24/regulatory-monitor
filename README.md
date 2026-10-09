# Indian Financial Regulatory Monitor

> Daily automated intelligence briefing on circulars, notifications, and policy changes from Indian financial regulators relevant to personal finance.

Built by [Novelty Wealth](https://noveltywealth.com) (SEBI RIA: INA000019415)

---

## What It Does

Every day at 4:00 AM IST, this scraper checks official regulator websites and financial news feeds for updates, then generates a structured briefing with relevance scoring.

### Sources Monitored

All sources live in [`scraper/sources.yaml`](scraper/sources.yaml). Currently 77: 17 official pages and feeds, 60 news and research feeds.

| Group | Focus | Where it is read from |
|-------|-------|-----------------------|
| SEBI | MFs, capital markets, RIA rules, investor protection | Circulars, master circulars, press releases, consultation papers (sebi.gov.in) |
| RBI | Interest rates, banking, lending, payments, forex | Press release, notification and speech feeds (rbi.org.in) |
| IRDAI | Insurance products, claim norms, new categories | Circulars, press releases (irdai.gov.in) |
| PFRDA | NPS, pension regulations, subscriber guidelines | Circulars, press releases (pfrda.org.in), NPS Trust circulars |
| CBDT | Income tax, TDS, capital gains, ITR notifications | e-filing portal news (incometax.gov.in). incometaxindia.gov.in blocks scripts |
| AMFI | MF industry circulars, best practice guidelines | amfiindia.com/circulars |
| PIB | Government press releases | PIB English feed |
| Exchanges | NSE circulars, BSE notices | Exchange feeds |
| News | Mint, ET Wealth, Business Standard, BusinessLine, CNBC-TV18, Financial Express, Moneycontrol and others | RSS feeds and section pages |
| Research and blogs | Value Research, Morningstar India, Cafemutual, Freefincal, PrimeInvestor, Zerodha, ET Money | RSS feeds and listing pages |
| Tax | TaxGuru, Taxscan | RSS feeds |
| Insurance and cards | Asia Insurance Post, CardExpert, CardInsider, Live From A Lounge | RSS feeds |

### Relevance Scoring

- 🔴 **HIGH** — Directly changes product behavior, taxation, or compliance for retail investors
- 🟡 **MEDIUM** — Relevant context, may affect users indirectly
- 🟢 **LOW** — Background regulatory housekeeping

## Repo Structure

```
├── .github/workflows/monitor.yml   # Daily cron (4 AM IST)
├── scraper/
│   ├── monitor.py                  # Scraper, scoring, briefing writer
│   ├── sources.yaml                # Every source. Add a website here
│   ├── slack_notify.py             # Posts the briefing to Slack
│   └── requirements.txt
├── tests/test_monitor.py           # Offline tests (no network)
├── data/
│   ├── latest.json                 # Most recent briefing (JSON)
│   ├── feed_health.json            # Per-source health, last 30 days
│   ├── scraper_health.json         # Per-group counts, last 30 days
│   ├── seen_items.json             # What was already reported (stops repeats)
│   └── briefings/
│       ├── 2026-04-07.json         # Daily snapshot
│       └── 2026-04-07.md           # Readable markdown briefing
└── README.md
```

## Quick Start

Run everything from the repo root, so output lands in `data/`.

```bash
# Install
pip install -r scraper/requirements.txt

# Run the daily job
python scraper/monitor.py

# Check which sources are alive (writes nothing)
python scraper/monitor.py --check-sources

# Full pipeline without writing files
python scraper/monitor.py --dry-run

# Catch up after a missed run
python scraper/monitor.py --hours 72

# Only some sources or groups
python scraper/monitor.py --check-sources --only SEBI VROnline

# Tests
python -m unittest discover -s tests -v
```

## How It Decides What Is New

- **Sources with a timestamp** (most news feeds): item must be inside the last 24 hours.
- **Sources with a date but no time** (most regulator pages): item must be dated yesterday or today. Official sources get 2 extra days, because circulars are often uploaded after the date printed on them.
- **Sources with no date** (PIB feed, some listing pages): item is new IF it was not on the page during an earlier run. The first run for such a source reports nothing.
- `data/seen_items.json` remembers what was reported, so nothing repeats on the next day.

## Full Article Text

For shortlisted news items the scraper opens the article and uses the body to improve scoring, tags, regulator detection, circular reference and deadline. IF the feed summary is empty, THEN a short lead is taken from the article.

- Article bodies are never written to `data/`. Only derived fields are stored (`fulltext_status`, `word_count`, `fulltext_keywords`).
- IF a site shows a paywall or a bot check, THEN the item keeps its headline and feed summary. The scraper does not try to get around either. Value Research article pages are behind such a check, so only its RSS feed is used.
- Limits are in `sources.yaml` under `settings.fulltext`.

## Source Health

Every run writes one line per source to `data/feed_health.json`: status, HTTP code, items fetched, items recent, items relevant.

| Status | Meaning |
|--------|---------|
| `ok` | Source returned items |
| `empty` | Page loaded but no items found. Layout or URL probably changed |
| `stale` | Feed loads but its newest item is over 30 days old (180 for blogs) |
| `http_error` | Blocked, moved or down |
| `parse_error` | Page could not be read |

A source that returns nothing for 3 runs in a row is logged as DEAD. The daily markdown brief ends with a Source Health section listing every source that failed that day.

## Outputs

### JSON (`data/latest.json`)
```json
{
  "date": "2026-04-07",
  "total": 12,
  "high_priority": 3,
  "official_items": 4,
  "updates": [
    {
      "regulator": "SEBI",
      "title": "...",
      "relevance": "HIGH",
      "category": "MF Regulation",
      "url": "https://sebi.gov.in/...",
      "source_type": "official",
      "source_name": "SEBI_Circulars"
    }
  ],
  "more_items": [],
  "scraper_meta": {"sources_configured": 77, "sources_ok": 75, "source_alerts": []}
}
```

The brief holds every official item plus the top 60 news items. Lower-ranked news stays in `more_items`. Change the cap in `sources.yaml` under `settings.briefing`.

### Markdown Briefing (`data/briefings/{date}.md`)
Human-readable daily brief with summary table, detailed briefs per item, and a regulatory pulse closing.

## GitHub Actions

The workflow runs daily at 4:00 AM IST. It:
- Scrapes all regulator sources
- Generates JSON + Markdown briefing
- Auto-commits to `data/` with a commit message like: `brief: 2026-04-07 regulatory update (12 updates, 3 high-priority)`

Manual trigger: Actions tab → "Daily Regulatory Monitor" → Run workflow.

## Adding a New Website

No code needed. Add one entry to `scraper/sources.yaml`.

RSS feed:

```yaml
- {name: MySite, url: "https://example.com/feed/", tier: tier2_news}
```

Page that lists items (no feed):

```yaml
- name: NewReg_Circulars
  url: "https://newreg.gov.in/circulars"
  type: listing
  row: "table tr"        # one row or card per item
  tier: official
  group: NewReg
  regulator: NewReg
  lenient: true          # keep every item that is not noise
```

Then test it:

```bash
python scraper/monitor.py --check-sources --only NewReg_Circulars
```

All options are explained at the top of `sources.yaml`. For a noisy general feed, set `min_score: 3` so only clearly on-topic stories pass.

## Use Cases for Novelty Wealth

1. **Morning briefing** — Team checks `data/briefings/{today}.md` every morning
2. **NovaAI knowledge base** — High-priority items feed into NovaWiki entries
3. **Content pipeline** — HIGH items trigger LinkedIn posts and blog drafts
4. **Compliance** — Track RIA-relevant SEBI circulars automatically
5. **Client alerts** — Push notifications for tax or product changes

## Disclaimer

This tool scrapes publicly available information from official government and regulator websites. It is for internal use and educational purposes only. Always verify against official sources before taking action or publishing content. Not financial advice.

## License

MIT
