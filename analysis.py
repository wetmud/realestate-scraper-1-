"""
RE:PULSE Trend Analysis Engine
Computes market statistics from raw listings.
Handles multi-site, multi-city data with per-source breakdowns.
"""

import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional


def load_listings(path: str = "data/listings.json") -> list[dict]:
    with open(path) as f:
        return json.load(f)


def analyze(listings: list[dict]) -> dict:
    """Compute aggregate market metrics from a list of listing dicts."""
    if not listings:
        return {}

    active  = [l for l in listings if l.get("listing_status") == "Active"]
    pending = [l for l in listings if l.get("listing_status") == "Pending"]
    sold    = [l for l in listings if l.get("listing_status") == "Sold"]

    prices       = [l["listing_price"] for l in listings if l.get("listing_price")]
    active_prices= [l["listing_price"] for l in active   if l.get("listing_price")]
    doms         = [l["days_on_market"] for l in listings if l.get("days_on_market") is not None]
    ppsf_vals    = [l["price_per_sqft"] for l in listings if l.get("price_per_sqft")]
    sqft_vals    = [l["square_feet"]    for l in listings if l.get("square_feet")]

    # ── By property type ───────────────────────────────────────────────────────
    by_type: dict[str, list[int]] = defaultdict(list)
    for l in listings:
        if l.get("listing_price"):
            by_type[l.get("property_type", "Unknown")].append(l["listing_price"])

    type_stats = {
        ptype: {
            "count":        len(prices_),
            "avg_price":    _safe_mean(prices_),
            "median_price": _safe_median(prices_),
        }
        for ptype, prices_ in by_type.items()
    }

    # ── By city ────────────────────────────────────────────────────────────────
    by_city: dict[str, list[int]] = defaultdict(list)
    for l in listings:
        if l.get("listing_price") and l.get("city"):
            key = f"{l['city']}, {l.get('state_or_province','')}"
            by_city[key].append(l["listing_price"])

    city_stats = {
        city: {
            "count":        len(prices_),
            "avg_price":    _safe_mean(prices_),
            "median_price": _safe_median(prices_),
        }
        for city, prices_ in by_city.items()
    }

    # ── By source site ─────────────────────────────────────────────────────────
    by_source: dict[str, list[int]] = defaultdict(list)
    for l in listings:
        if l.get("listing_price") and l.get("source_site"):
            by_source[l["source_site"]].append(l["listing_price"])

    source_stats = {
        source: {
            "count":        len(prices_),
            "avg_price":    _safe_mean(prices_),
            "median_price": _safe_median(prices_),
        }
        for source, prices_ in by_source.items()
    }

    # ── Supply/demand signal ───────────────────────────────────────────────────
    supply_demand = round(len(active) / max(len(pending) + len(sold), 1), 2)

    return {
        "generated_at":    datetime.utcnow().isoformat(),
        "total_listings":  len(listings),
        "by_status": {
            "active":  len(active),
            "pending": len(pending),
            "sold":    len(sold),
        },
        "pricing": {
            "avg_list_price":       _safe_mean(prices),
            "median_list_price":    _safe_median(prices),
            "min_price":            min(prices) if prices else None,
            "max_price":            max(prices) if prices else None,
            "avg_active_price":     _safe_mean(active_prices),
            "avg_price_per_sqft":   _safe_mean(ppsf_vals),
            "median_price_per_sqft":_safe_median(ppsf_vals),
        },
        "velocity": {
            "avg_days_on_market":    round(statistics.mean(doms), 1) if doms else None,
            "median_days_on_market": _safe_median(doms),
            "listings_under_7_days": len([d for d in doms if d <= 7]),
            "listings_over_30_days": len([d for d in doms if d > 30]),
        },
        "inventory": {
            "avg_sqft":             _safe_mean(sqft_vals),
            "supply_demand_ratio":  supply_demand,
            "market_signal": (
                "Seller's Market" if supply_demand < 1.5 else
                "Balanced Market" if supply_demand < 3.0 else
                "Buyer's Market"
            ),
        },
        "by_type":   type_stats,
        "by_city":   city_stats,
        "by_source": source_stats,
        "listings":  listings,
    }


def save_analysis(analysis: dict, path: str = "data/analysis.json"):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(analysis, f, indent=2, default=str)


def _safe_mean(vals: list) -> Optional[int]:
    vals = [v for v in vals if v is not None]
    return int(statistics.mean(vals)) if vals else None

def _safe_median(vals: list) -> Optional[int]:
    vals = [v for v in vals if v is not None]
    return int(statistics.median(vals)) if vals else None


if __name__ == "__main__":
    listings = load_listings()
    analysis = analyze(listings)
    save_analysis(analysis)
    p = analysis["pricing"]
    print(
        f"Analysis: {analysis['total_listings']} listings | "
        f"avg ${p['avg_list_price']:,} | "
        f"signal: {analysis['inventory']['market_signal']}"
    )
    print("\nBy source:")
    for src, s in analysis.get("by_source", {}).items():
        print(f"  {src}: {s['count']} listings, avg ${s['avg_price']:,}")
    print("\nBy city:")
    for city, s in analysis.get("by_city", {}).items():
        print(f"  {city}: {s['count']} listings, avg ${s['avg_price']:,}")
