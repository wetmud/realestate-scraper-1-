"""
RE:PULSE Pipeline Scheduler
Scrape → Analyze → Save → (optionally) trigger dashboard refresh

Usage:
  python3 scheduler.py                           # all sites, all cities
  python3 scheduler.py --site kijiji             # one site, all its cities
  python3 scheduler.py --city "Vancouver"        # all sites, only Vancouver
  python3 scheduler.py --site realtor_ca --city "Toronto, ON"
  python3 scheduler.py --demo                    # mock data, no network
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from scraper.scraper import run_all, demo_listings
from scraper.analysis import analyze, save_analysis

logger = logging.getLogger("scheduler")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


async def pipeline(
    site: str = None,
    city: str = None,
    config: str = "cities.yml",
    output_dir: str = "data",
    demo: bool = False,
):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    logger.info(f"=== RE:PULSE Pipeline start {ts} ===")
    logger.info(f"    site={site or 'all'}  city={city or 'all'}  demo={demo}")

    # 1. Scrape
    if demo:
        listings = demo_listings()
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        with open(f"{output_dir}/listings.json", "w") as f:
            json.dump(listings, f, indent=2)
    else:
        listings = await run_all(
            config_path=config,
            site_filter=site,
            city_filter=city,
            output_dir=output_dir,
        )

    if not listings:
        logger.warning("No listings scraped. Check selectors or network. Aborting.")
        return

    # 2. Analyze
    analysis = analyze(listings)
    save_analysis(analysis, f"{output_dir}/analysis.json")

    # 3. Summary
    p = analysis["pricing"]
    inv = analysis["inventory"]
    logger.info(
        f"=== Pipeline complete: {len(listings)} listings | "
        f"avg ${p['avg_list_price']:,} | "
        f"signal: {inv['market_signal']} ==="
    )

    # Print per-site breakdown
    by_source = analysis.get("by_source", {})
    if by_source:
        for source, stats in by_source.items():
            logger.info(f"    {source}: {stats['count']} listings, avg ${stats['avg_price']:,}")

    return analysis


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RE:PULSE Pipeline")
    parser.add_argument("--site",   help="Run only this site key (realtor_ca | kijiji | point2homes)")
    parser.add_argument("--city",   help='Filter to cities matching this string, e.g. "Vancouver"')
    parser.add_argument("--config", default="cities.yml")
    parser.add_argument("--output", default="data")
    parser.add_argument("--demo",   action="store_true", help="Use mock data only")
    args = parser.parse_args()

    asyncio.run(pipeline(
        site=args.site,
        city=args.city,
        config=args.config,
        output_dir=args.output,
        demo=args.demo,
    ))
