# North Georgia Home Market-Timing Dashboard

A self-contained tool to help judge **whether now is a good time to buy** a home in
North Georgia — **Canton/Cherokee, Kennesaw/Cobb, Cumming/Forsyth** — and whether
rates are trending better and prices softening in those areas.

It answers three questions at a glance:

1. **Buyer's or seller's market?** — a big color-coded verdict banner + a 0–100 Buyer Leverage Score.
2. **Are rates trending better?** — 30-yr fixed, 10-yr Treasury, inflation, and a plain "falling/flat/rising" read.
3. **Are prices softening?** — median price, YoY/MoM, and the *leading* inventory signals that move first.

> ⚠️ **Not financial, lending, or real-estate advice.** Estimates only — verify with a licensed
> professional and current data before acting.

---

## What's in here

| File | Purpose |
|------|---------|
| `dashboard.html` | The dashboard. **Double-click to open** — no server, no build step. |
| `fetch_data.py` | Deterministic ETL: pulls FRED rates + Redfin county data → writes `data/market.json`. |
| `data/market.json` | The slim data file the dashboard reads (committed; refreshed weekly). |
| `.github/workflows/refresh-market-data.yml` | Weekly GitHub Action that re-runs the fetch and commits the data. |
| `requirements.txt` | Python deps (`requests`, `pandas`). |

---

## Quick start

### 1. Open the dashboard
Just double-click **`dashboard.html`**. If `data/market.json` is missing or you're offline,
it falls back to built-in seed values so it always renders. Charts need internet (Chart.js CDN);
everything else works offline.

### 2. Get live data locally (optional)
```bash
# Windows (PowerShell) — Python is available via the `py` launcher:
py -m pip install -r requirements.txt
$env:FRED_API_KEY = "your_key_here"      # get one at the link below
py fetch_data.py

# macOS / Linux:
python3 -m pip install -r requirements.txt
export FRED_API_KEY="your_key_here"
python3 fetch_data.py
```
This rewrites `data/market.json`; reload the dashboard to see live numbers.

**Get a free FRED API key:** https://fred.stlouisfed.org/docs/api/api_key.html

---

## How the weekly refresh works

The GitHub Action `refresh-market-data.yml`:

- Runs **every Monday ~7–8am ET** (Redfin publishes weekly) and on-demand via the Actions tab
  (`workflow_dispatch`).
- Installs deps, runs `fetch_data.py` with your `FRED_API_KEY`, and **commits `data/market.json`
  only if it changed** — so you get a version-controlled history of the market.

### Required one-time setup — add the FRED secret
The Action needs your FRED key as a repository secret:

1. Get a key: https://fred.stlouisfed.org/docs/api/api_key.html
2. In this repo on GitHub: **Settings → Secrets and variables → Actions → New repository secret**
3. Name it exactly **`FRED_API_KEY`**.

Until that secret exists, the Action still runs and refreshes Redfin housing data — it just keeps
the last-good rate values.

---

## Data sources & terms

- **Rates & inflation:** [FRED](https://fred.stlouisfed.org/) — `MORTGAGE30US` (30-yr fixed),
  `DGS10` (10-yr Treasury), `CPIAUCSL` (CPI → converted to YoY %).
- **Housing:** [Redfin Data Center](https://www.redfin.com/news/data-center/) — the *published*
  gzipped county TSV (`county_market_tracker`), filtered to Cherokee, Cobb, and Forsyth counties.
  This is a legitimate published-file download, **not** website scraping. Redfin refreshes weekly,
  which is why weekly scheduling is sufficient. **Respect Redfin's terms: personal use, no
  redistribution of the raw data.** (This repo commits only the slim derived `market.json`.)

---

## Extending it

- **Retarget areas:** edit `TARGET_COUNTIES` in `fetch_data.py` and the matching `areas` keys /
  `SEED` block + `<option>`s in `dashboard.html`.
- **Change scoring:** the Buyer Leverage Score and Buy Signal live in `dashboard.html`
  (`leverageScore()` / `buySignal()`), with each sub-score exposed so it's not a black box.
- **Add a metric:** map its Redfin column in `REDFIN_COLUMNS` + `fetch_redfin_counties()`,
  then add a `metric({...})` tile in `renderMetrics()`.

The pipeline is intentionally **not** an LLM/agent — it's a deterministic extract→filter→write
job so the weekly automation stays boring and reliable.
