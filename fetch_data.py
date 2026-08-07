#!/usr/bin/env python3
"""
fetch_data.py — North Georgia home market-timing data pipeline
===============================================================

A DETERMINISTIC extract -> filter -> write job. There is intentionally NO LLM
and NO agent here: this is a boring, reliable ETL step so the weekly GitHub
Action can run unattended and produce version-controlled data.

It produces `data/market.json`, which `dashboard.html` reads. If a source is
unreachable, we keep the last-good values already in `data/market.json` and log
a warning instead of crashing — a stale-but-rendering dashboard beats a broken one.

Sources
-------
1. FRED (Federal Reserve Economic Data) API — mortgage rate, 10-yr Treasury, CPI.
   * Get a FREE api key: https://fred.stlouisfed.org/docs/api/api_key.html
   * Provide it via the FRED_API_KEY environment variable (never hard-code it).
2. Redfin Data Center published files — county-level housing metrics.
   * These are legitimately PUBLISHED gzipped TSV files on a public S3 bucket
     (see https://www.redfin.com/news/data-center/). This is a file download,
     NOT website scraping.
   * Redfin refreshes these weekly, so a weekly schedule is sufficient.
   * Respect Redfin's terms: personal use, no redistribution of the raw data.

Dependencies: standard library + requests + pandas only (see requirements.txt).
Run locally:  set FRED_API_KEY, then `py fetch_data.py` (Windows) / `python3 fetch_data.py`.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# --------------------------------------------------------------------------- #
# EDITABLE CONSTANTS — tweak these to retarget the tool.                       #
# --------------------------------------------------------------------------- #

# Where the dashboard reads its data from (relative to this file).
OUTPUT_PATH = Path(__file__).resolve().parent / "data" / "market.json"

# Be a polite HTTP citizen: identify ourselves and cap how long we wait.
USER_AGENT = "ga-home-market-dashboard/1.0 (personal home-buying research; contact via GitHub)"
HTTP_TIMEOUT = 60  # seconds

# --- FRED series we pull. Keys are the FRED series IDs. --------------------- #
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_SERIES = {
    "MORTGAGE30US": "mortgage30",  # 30-yr fixed mortgage average (weekly, %)
    "DGS10": "treasury10",         # 10-yr Treasury constant maturity (daily, %)
    "CPIAUCSL": "cpi_index",       # CPI-U all items (monthly, index) -> we compute YoY %
}
# How many trailing MONTHLY points to keep for the rate trend charts.
RATE_HISTORY_MONTHS = 12

# --- Redfin published county file ------------------------------------------ #
# Public S3 bucket that backs the Redfin Data Center downloads.
REDFIN_COUNTY_URL = (
    "https://redfin-public-data.s3-us-west-2.amazonaws.com/"
    "redfin_market_tracker/county_market_tracker.tsv000.gz"
)
# Only aggregate rows (all home types) — not per-property-type breakdowns.
REDFIN_PROPERTY_TYPE = "All Residential"
# How many trailing periods (months) to keep for the housing trend charts.
HOUSING_HISTORY_MONTHS = 12

# Map each dashboard "area" -> the Redfin county `region` string (state-suffixed).
# `region` in the county file looks like "Cherokee County, GA".
TARGET_COUNTIES = {
    "canton_cherokee": {
        "label": "Canton / Cherokee",
        "county": "Cherokee County, GA",
        "median_household_income": 95000,   # ~ Census ACS; used by affordability index
    },
    "kennesaw_cobb": {
        "label": "Kennesaw / Cobb",
        "county": "Cobb County, GA",
        "median_household_income": 88000,
    },
    "cumming_forsyth": {
        "label": "Cumming / Forsyth",
        "county": "Forsyth County, GA",
        "median_household_income": 135000,
    },
}

# --- Redfin column -> dashboard metric mapping ----------------------------- #
# The county TSV is tab-separated with one row per (period_begin, region,
# property_type). These are the columns we consume; everything else is dropped.
#   period_begin          -> history label (YYYY-MM)
#   region                -> county name we filter on
#   property_type         -> filtered to REDFIN_PROPERTY_TYPE
#   median_sale_price     -> median_sale_price
#   median_sale_price_yoy -> price_yoy (fraction, e.g. -0.034)
#   median_sale_price_mom -> price_mom (fraction)
#   median_ppsf           -> median_ppsf (price per square foot)
#   months_of_supply      -> months_of_supply  (LEADING indicator; <4 seller, 4-6 balanced, >6 buyer)
#   median_dom            -> days_on_market
#   avg_sale_to_list      -> sale_to_list (fraction, e.g. 0.973)
#   price_drops           -> price_drops_pct (fraction of active listings with a price cut)
#   inventory             -> inventory (active listings)
#   inventory_yoy         -> inventory_yoy (fraction)
#   new_listings          -> new_listings
REDFIN_COLUMNS = [
    "period_begin", "region", "property_type",
    "median_sale_price", "median_sale_price_yoy", "median_sale_price_mom",
    "median_ppsf",
    "months_of_supply", "median_dom", "avg_sale_to_list", "price_drops",
    "inventory", "inventory_yoy", "new_listings",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("fetch_data")


# --------------------------------------------------------------------------- #
# Small helpers                                                                #
# --------------------------------------------------------------------------- #

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def load_last_good() -> dict[str, Any]:
    """Read the existing market.json so we can preserve values if a source fails."""
    try:
        with OUTPUT_PATH.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        log.warning("No usable existing market.json (%s); starting from empty last-good.", exc)
        return {}


def _round(value: Any, ndigits: int) -> Any:
    """Round if numeric, else return as-is (keeps None/NaN handling simple)."""
    try:
        if value is None or pd.isna(value):
            return None
        return round(float(value), ndigits)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# FRED (rates + inflation)                                                     #
# --------------------------------------------------------------------------- #

def fetch_fred_series(session: requests.Session, series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch one FRED series as a tidy DataFrame [date, value] (NaNs dropped)."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        # Only pull what we need for a 12-month view plus a YoY base year.
        "observation_start": "2023-01-01",
    }
    resp = session.get(FRED_BASE, params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    obs = resp.json().get("observations", [])
    df = pd.DataFrame(obs)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    # FRED encodes missing values as ".".
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["value"])[["date", "value"]].sort_values("date")


def _monthly_tail(df: pd.DataFrame, months: int) -> list[dict[str, Any]]:
    """Resample a series to month-end, take the last `months` points."""
    monthly = (
        df.set_index("date")["value"]
        .resample("ME").last()
        .dropna()
        .tail(months)
    )
    return [{"date": d.strftime("%Y-%m"), "value": _round(v, 2)} for d, v in monthly.items()]


def build_rates_block(session: requests.Session, api_key: str, last_good: dict) -> dict:
    """
    Assemble the shared `rates` block: current value + monthly history for each
    series, plus a plain-English rate-direction read.

    Mortgage rates track the 10-yr Treasury + inflation expectations + Fed tone,
    with a lag — so we blend the mortgage trend with the Treasury trend.
    """
    prior = last_good.get("rates", {})
    if not api_key:
        log.warning("FRED_API_KEY not set — keeping last-good rates block.")
        return prior or {}

    rates: dict[str, Any] = {}
    frames: dict[str, pd.DataFrame] = {}
    try:
        for series_id, key in FRED_SERIES.items():
            frames[key] = fetch_fred_series(session, series_id, api_key)
            log.info("FRED %s: %d observations.", series_id, len(frames[key]))
    except requests.RequestException as exc:
        log.warning("FRED fetch failed (%s) — keeping last-good rates block.", exc)
        return prior or {}

    # 30-yr fixed and 10-yr Treasury: straight monthly tails.
    for key in ("mortgage30", "treasury10"):
        df = frames.get(key)
        if df is None or df.empty:
            rates[key] = prior.get(key, {})
            continue
        history = _monthly_tail(df, RATE_HISTORY_MONTHS)
        rates[key] = {"value": history[-1]["value"] if history else None, "history": history}

    # CPI: convert the index level into a trailing 12-month YoY % change.
    cpi = frames.get("cpi_index")
    if cpi is not None and not cpi.empty:
        monthly = cpi.set_index("date")["value"].resample("ME").last().dropna()
        yoy = (monthly.pct_change(12) * 100).dropna().tail(RATE_HISTORY_MONTHS)
        history = [{"date": d.strftime("%Y-%m"), "value": _round(v, 1)} for d, v in yoy.items()]
        rates["cpi_yoy"] = {"value": history[-1]["value"] if history else None, "history": history}
    else:
        rates["cpi_yoy"] = prior.get("cpi_yoy", {})

    # Rate-direction read: compare latest mortgage rate vs its 3-month average,
    # corroborated by the 10-yr Treasury's own 3-month move.
    direction, reason = _rate_direction(rates)
    rates["direction"] = direction
    rates["direction_reason"] = reason
    return rates


def _rate_direction(rates: dict) -> tuple[str, str]:
    """Return ('falling'|'flat'|'rising', reason-string)."""
    m_hist = [p["value"] for p in rates.get("mortgage30", {}).get("history", []) if p["value"] is not None]
    t_hist = [p["value"] for p in rates.get("treasury10", {}).get("history", []) if p["value"] is not None]
    if len(m_hist) < 4:
        return "flat", "Not enough recent rate history to call a direction."

    latest = m_hist[-1]
    avg3 = sum(m_hist[-4:-1]) / 3  # average of the prior 3 months
    delta = latest - avg3
    t_delta = (t_hist[-1] - (sum(t_hist[-4:-1]) / 3)) if len(t_hist) >= 4 else 0.0

    if delta <= -0.05:
        direction = "falling"
    elif delta >= 0.05:
        direction = "rising"
    else:
        direction = "flat"

    treas = "easing" if t_delta < -0.03 else "climbing" if t_delta > 0.03 else "steady"
    reason = (
        f"30-yr fixed is {latest:.2f}%, {abs(delta):.2f} pts "
        f"{'below' if delta < 0 else 'above' if delta > 0 else 'in line with'} its 3-month average, "
        f"and the 10-yr Treasury is {treas}. Mortgage rates track the 10-yr + inflation + Fed tone, with a lag."
    )
    return direction, reason


# --------------------------------------------------------------------------- #
# Redfin (housing metrics)                                                     #
# --------------------------------------------------------------------------- #

def fetch_redfin_counties(session: requests.Session, last_good: dict) -> dict:
    """
    Stream the gzipped county TSV, filter to our target counties + All Residential,
    and return {area_key: {current metrics + history arrays}}.

    The full file is large (hundreds of MB uncompressed), so we read it in chunks
    and discard everything outside our county set as we go — we never hold the
    whole thing in memory.
    """
    wanted_regions = {cfg["county"] for cfg in TARGET_COUNTIES.values()}
    try:
        log.info("Downloading Redfin county file (this is a published file, not scraping)...")
        resp = session.get(REDFIN_COUNTY_URL, timeout=HTTP_TIMEOUT, stream=True)
        resp.raise_for_status()
        raw = gzip.GzipFile(fileobj=io.BytesIO(resp.content))

        collected: list[pd.DataFrame] = []
        reader = pd.read_csv(
            raw, sep="\t", usecols=REDFIN_COLUMNS, chunksize=100_000,
            dtype={"region": "string", "property_type": "string"},
        )
        for chunk in reader:
            mask = (
                chunk["region"].isin(wanted_regions)
                & (chunk["property_type"] == REDFIN_PROPERTY_TYPE)
            )
            hit = chunk.loc[mask]
            if not hit.empty:
                collected.append(hit)
        if not collected:
            log.warning("No matching Redfin rows found — keeping last-good housing values.")
            return last_good.get("areas", {})
        df = pd.concat(collected, ignore_index=True)
    except (requests.RequestException, ValueError, OSError) as exc:
        log.warning("Redfin fetch/parse failed (%s) — keeping last-good housing values.", exc)
        return last_good.get("areas", {})

    df["period_begin"] = pd.to_datetime(df["period_begin"])
    areas: dict[str, Any] = {}

    for area_key, cfg in TARGET_COUNTIES.items():
        county_df = df[df["region"] == cfg["county"]].sort_values("period_begin")
        if county_df.empty:
            log.warning("No Redfin rows for %s — keeping last-good.", cfg["county"])
            areas[area_key] = last_good.get("areas", {}).get(area_key, {})
            continue

        latest = county_df.iloc[-1]
        window = county_df.tail(HOUSING_HISTORY_MONTHS)

        areas[area_key] = {
            "label": cfg["label"],
            "county": cfg["county"],
            "median_household_income": cfg["median_household_income"],
            # Fractions in the file are converted to human-friendly percentages here.
            "median_sale_price": _round(latest["median_sale_price"], 0),
            "median_ppsf": _round(latest["median_ppsf"], 0),
            "price_yoy": _round(latest["median_sale_price_yoy"] * 100, 1),
            "price_mom": _round(latest["median_sale_price_mom"] * 100, 1),
            "months_of_supply": _round(latest["months_of_supply"], 1),
            "days_on_market": _round(latest["median_dom"], 0),
            "price_drops_pct": _round(latest["price_drops"] * 100, 0),
            "sale_to_list": _round(latest["avg_sale_to_list"] * 100, 1),
            "inventory": _round(latest["inventory"], 0),
            "inventory_yoy": _round(latest["inventory_yoy"] * 100, 1),
            "new_listings": _round(latest["new_listings"], 0),
            "history": {
                "labels": [d.strftime("%Y-%m") for d in window["period_begin"]],
                "median_sale_price": [_round(v, 0) for v in window["median_sale_price"]],
                "months_of_supply": [_round(v, 1) for v in window["months_of_supply"]],
                "days_on_market": [_round(v, 0) for v in window["median_dom"]],
                "price_drops_pct": [_round(v * 100, 0) for v in window["price_drops"]],
            },
        }
        log.info("Redfin %s: latest period %s.", cfg["county"], latest["period_begin"].strftime("%Y-%m"))

    return areas


# --------------------------------------------------------------------------- #
# Assemble + write                                                             #
# --------------------------------------------------------------------------- #

def build_market_json(rates: dict, areas: dict, last_good: dict) -> dict:
    """Assemble the slim JSON the dashboard consumes."""
    # Derive human-readable "as of" markers from whatever data we actually have.
    redfin_period = None
    for area in areas.values():
        labels = area.get("history", {}).get("labels")
        if labels:
            redfin_period = labels[-1]
            break
    fred_asof = None
    m_hist = rates.get("mortgage30", {}).get("history")
    if m_hist:
        fred_asof = m_hist[-1]["date"]

    return {
        "_comment": "Auto-generated by fetch_data.py. Do not hand-edit; re-run the script instead.",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_sources": {
            "redfin": "Redfin Data Center county_market_tracker (published TSV)",
            "fred": "FRED (MORTGAGE30US, DGS10, CPIAUCSL)",
            "redfin_period": redfin_period or last_good.get("data_sources", {}).get("redfin_period"),
            "fred_asof": fred_asof or last_good.get("data_sources", {}).get("fred_asof"),
        },
        "rates": rates or last_good.get("rates", {}),
        "areas": areas or last_good.get("areas", {}),
    }


def main() -> int:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    last_good = load_last_good()
    session = _session()

    rates = build_rates_block(session, api_key, last_good)
    areas = fetch_redfin_counties(session, last_good)

    payload = build_market_json(rates, areas, last_good)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")
    log.info("Wrote %s", OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
