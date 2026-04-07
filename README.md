# Indian Financial Regulatory Monitor

> Daily automated intelligence briefing on circulars, notifications, and policy changes from Indian financial regulators relevant to personal finance.

Built by [Novelty Wealth](https://noveltywealth.com) (SEBI RIA: INA000019415)

---

## What It Does

Every day at 4:00 AM IST, this scraper checks official regulator websites and financial news feeds for updates, then generates a structured briefing with relevance scoring.

### Regulators Monitored

| Regulator | Focus | Source |
|-----------|-------|--------|
| SEBI | MFs, capital markets, RIA rules, investor protection | sebi.gov.in |
| RBI | Interest rates, banking, lending, payments, forex | rbi.org.in |
| IRDAI | Insurance products, claim norms, new categories | irdai.gov.in |
| PFRDA | NPS, pension regulations, subscriber guidelines | pfrda.org.in |
| CBDT | Income tax, TDS, capital gains, ITR notifications | incometaxindia.gov.in |
| AMFI | MF industry circulars, NFOs, expense ratios, KYC | amfiindia.com |
| News | Mint, ET Wealth, Moneycontrol (backup sourcing) | RSS feeds |

### Relevance Scoring

- 🔴 **HIGH** — Directly changes product behavior, taxation, or compliance for retail investors
- 🟡 **MEDIUM** — Relevant context, may affect users indirectly
- 🟢 **LOW** — Background regulatory housekeeping

## Repo Structure

```
├── .github/workflows/monitor.yml   # Daily cron (4 AM IST)
├── scraper/
│   ├── monitor.py                  # Main scraper
│   └── requirements.txt
├── data/
│   ├── latest.json                 # Most recent briefing (JSON)
│   └── briefings/
│       ├── 2026-04-07.json         # Daily snapshot
│       └── 2026-04-07.md           # Readable markdown briefing
└── README.md
```

## Quick Start

```bash
# Install
pip install -r scraper/requirements.txt

# Run
cd scraper && python monitor.py

# Check output
cat data/latest.json | python -m json.tool
cat data/briefings/$(date +%Y-%m-%d).md
```

## Outputs

### JSON (`data/latest.json`)
```json
{
  "date": "2026-04-07",
  "total": 12,
  "high_priority": 3,
  "updates": [
    {
      "regulator": "SEBI",
      "title": "...",
      "relevance": "HIGH",
      "category": "MF Regulation",
      "url": "https://sebi.gov.in/...",
      "source_type": "official"
    }
  ]
}
```

### Markdown Briefing (`data/briefings/{date}.md`)
Human-readable daily brief with summary table, detailed briefs per item, and a regulatory pulse closing.

## GitHub Actions

The workflow runs daily at 4:00 AM IST. It:
- Scrapes all regulator sources
- Generates JSON + Markdown briefing
- Auto-commits to `data/` with a commit message like: `brief: 2026-04-07 regulatory update (12 updates, 3 high-priority)`

Manual trigger: Actions tab → "Daily Regulatory Monitor" → Run workflow.

## Adding a New Regulator

Create a scraper class in `monitor.py`:

```python
class NewRegScraper(BaseFetcher):
    URL = "https://newreg.gov.in/circulars"

    def scrape(self) -> list[RegUpdate]:
        # Parse the page, return list of RegUpdate objects
        ...
```

Add it to the scrapers list in `RegulatoryMonitor.run()`.

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
