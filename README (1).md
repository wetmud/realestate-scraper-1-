# RE:PULSE — Real Estate Market Intelligence Scraper

A production-grade, Playwright-powered real estate scraper with trend analysis and interactive dashboard.

---

## Project Structure

```
real-estate-scraper/
├── scraper/
│   ├── scraper.py      # Core Playwright scraper
│   └── analysis.py     # Trend analysis engine
├── dashboard/
│   └── index.html      # Interactive market dashboard (standalone)
├── data/
│   ├── listings.json   # Latest raw listings (JSON array)
│   └── analysis.json   # Computed market analytics
├── scheduler.py        # Pipeline runner (scrape → analyze → save)
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Run in demo mode (no network needed)

```bash
python3 scraper/scraper.py demo
# → writes data/listings.json
```

### 3. Run analysis

```bash
python3 scraper/analysis.py
# → writes data/analysis.json
```

### 4. Run full pipeline

```bash
python3 scheduler.py --site demo
# or for a real target:
python3 scheduler.py --site realtor_ca
```

### 5. View dashboard

Open `dashboard/index.html` in any browser. It contains embedded demo data and runs fully offline.

---

## Running Against a Real Site

Edit `SITE_CONFIGS` in `scraper/scraper.py` to add or adjust a site config:

```python
"my_site": {
    "base_url": "https://example-realty.com",
    "search_url": "https://example-realty.com/listings?city=toronto",
    "selectors": {
        "listing_cards":  ".listing-card",
        "price":          ".listing-price",
        "address":        ".listing-address",
        "beds":           "[data-beds]",
        "baths":          "[data-baths]",
        "sqft":           "[data-sqft]",
        "type":           ".property-type",
        "image":          "img.primary-photo",
        "link":           "a.listing-link",
    },
}
```

Then run:
```bash
python3 scheduler.py --site my_site
```

**Always check `robots.txt` and the site's terms of service before scraping.**

---

## Scheduling (Cron)

### Linux/macOS cron — run every day at 7am

```bash
crontab -e
# Add:
0 7 * * * cd /path/to/real-estate-scraper && python3 scheduler.py --site demo >> logs/cron.log 2>&1
```

### GitHub Actions — daily at 8am UTC

```yaml
# .github/workflows/scrape.yml
name: Daily RE Scrape
on:
  schedule:
    - cron: '0 8 * * *'
  workflow_dispatch:
jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r requirements.txt && playwright install chromium
      - run: python3 scheduler.py --site demo
      - uses: actions/upload-artifact@v4
        with:
          name: scrape-data
          path: data/
```

---

## Output: Raw Listings JSON

```json
[
  {
    "listing_id": "24680123",
    "property_address": "42 Elm St",
    "city": "Toronto",
    "state_or_province": "ON",
    "postal_code": "M5G 1H5",
    "listing_price": 1125000,
    "property_type": "House",
    "bedrooms": 4,
    "bathrooms": 3.0,
    "square_feet": 1950,
    "price_per_sqft": 576,
    "listing_date": "2026-02-22",
    "days_on_market": 5,
    "listing_status": "Active",
    "listing_url": "https://www.realtor.ca/real-estate/24680123/listing",
    "image_url": "https://cdn.realtor.ca/listing/24680123/photo1.jpg",
    "agent_name": "Sarah Chen",
    "brokerage_name": "Royal LePage"
  }
]
```

---

## Output: Analysis JSON

```json
{
  "pricing": {
    "avg_list_price": 1746466,
    "median_list_price": 1480000,
    "avg_price_per_sqft": 968
  },
  "velocity": {
    "avg_days_on_market": 10.1,
    "listings_under_7_days": 7
  },
  "inventory": {
    "supply_demand_ratio": 4.0,
    "market_signal": "Buyer's Market"
  },
  "by_type": {
    "House": { "count": 6, "avg_price": 2603333 },
    "Condo": { "count": 7, "avg_price": 1086000 }
  }
}
```

---

## Adapting to Layout Changes

When a site redesigns, selectors will break. The scraper logs all selector failures:

```
WARNING: Selector '.listingCardPrice' not found. Page structure may have changed.
```

**Steps to fix:**
1. Open the target URL in Chrome DevTools
2. Use Inspect Element to find new CSS selectors for price, address, beds, etc.
3. Update the `selectors` dict in `SITE_CONFIGS` for the relevant site key
4. Re-run the scraper to verify

**Defensive patterns already built in:**
- All field extractions wrapped in try/except — single selector failure won't abort run
- Missing fields return `null`, not an error
- Retry with exponential backoff on page load failure
- Deduplication via listing_id and URL

---

## Extending the System

| Feature | How |
|---|---|
| Multiple cities | Add entries to `SITE_CONFIGS` with different `search_url` per city |
| Historical storage | Append to SQLite: `data/history.db` using `listings_YYYYMMDD.json` files |
| AI trend analysis | POST `analysis.json` to Claude API for natural language market summary |
| Dashboard integration | Serve `dashboard/index.html` with nginx; update `DATA` var via templating or JS fetch |
| Alerts | Compare current vs previous `analysis.json`; email if avg DOM drops > 20% |

---

## Ethics & Compliance

- Checks `robots.txt` before scraping (auto-aborts if disallowed)
- Adds random human-like delays between requests (1.5–3.5s)
- User-agent identifies as a standard browser (not a bot)
- Only collects publicly visible, non-login-gated data
- Never stores personally identifiable information beyond publicly listed agent names
- Respects `max_listings=25` hard cap per run
