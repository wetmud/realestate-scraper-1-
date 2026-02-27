"""
RE:PULSE — Multi-Site Canadian Real Estate Scraper
Supports: Realtor.ca · Kijiji Real Estate · Point2Homes

- Reads city/URL targets from cities.yml (config-driven)
- CLI --city flag overrides which city runs for a given site
- Respects robots.txt, adds jitter delays, uses exponential backoff
- All 18 output fields; missing fields return null, never fabricated
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import yaml
from playwright.async_api import (
    async_playwright,
    Page,
    TimeoutError as PWTimeout,
)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log", mode="a"),
    ],
)
logger = logging.getLogger("re_scraper")


# ── Data model ─────────────────────────────────────────────────────────────────
@dataclass
class Listing:
    listing_id: str
    property_address: str
    city: str
    state_or_province: str
    postal_code: Optional[str]
    listing_price: int
    property_type: str
    bedrooms: Optional[int]
    bathrooms: Optional[float]
    square_feet: Optional[int]
    price_per_sqft: Optional[int]
    listing_date: Optional[str]
    days_on_market: Optional[int]
    listing_status: str
    listing_url: str
    image_url: Optional[str]
    agent_name: Optional[str]
    brokerage_name: Optional[str]
    source_site: str          # which site this listing came from
    scrape_timestamp: str     # ISO timestamp of when it was scraped

    def to_dict(self) -> dict:
        return asdict(self)


# ── Utilities ──────────────────────────────────────────────────────────────────

def load_cities_config(path: str = "cities.yml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def check_robots(base_url: str, path: str = "/") -> bool:
    """Return True if scraping the given path is allowed by robots.txt."""
    try:
        rp = RobotFileParser()
        rp.set_url(urljoin(base_url, "/robots.txt"))
        rp.read()
        allowed = rp.can_fetch("*", urljoin(base_url, path))
        if not allowed:
            logger.warning(f"robots.txt DISALLOWS scraping {base_url}{path}")
        return allowed
    except Exception as e:
        logger.warning(f"Could not read robots.txt for {base_url}: {e} — proceeding cautiously")
        return True


def parse_price(raw: str) -> Optional[int]:
    if not raw:
        return None
    cleaned = re.sub(r"[^\d]", "", raw)
    return int(cleaned) if cleaned else None


def parse_float(raw: str) -> Optional[float]:
    if not raw:
        return None
    m = re.search(r"[\d.]+", raw.replace(",", ""))
    return float(m.group()) if m else None


def parse_int(raw: str) -> Optional[int]:
    v = parse_float(raw)
    return int(v) if v is not None else None


def calc_ppsf(price: Optional[int], sqft: Optional[int]) -> Optional[int]:
    if price and sqft and sqft > 0:
        return math.floor(price / sqft)
    return None


def extract_postal(text: str) -> Optional[str]:
    """Extract Canadian postal code or US ZIP from a string."""
    m = re.search(r"[A-Z]\d[A-Z]\s?\d[A-Z]\d|\b\d{5}(?:-\d{4})?\b", text, re.IGNORECASE)
    return m.group().upper().strip() if m else None


def extract_id_from_url(url: str) -> Optional[str]:
    # Realtor.ca: /real-estate/12345678/
    # Kijiji: /v-real-estate/city/title/1234567890
    # Point2Homes: /real-estate/address-id-12345
    m = re.search(r"/(\d{6,})", url)
    return m.group(1) if m else None


def dedupe_id(url: str, address: str) -> str:
    """Generate a stable dedup key from URL or address fallback."""
    from_url = extract_id_from_url(url)
    if from_url:
        return from_url
    return str(abs(hash(address or url)))[:12]


async def with_retry(coro_fn, retries: int = 3, base_delay: float = 2.0):
    for attempt in range(1, retries + 1):
        try:
            return await coro_fn()
        except Exception as e:
            if attempt == retries:
                raise
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0.3, 1.2)
            logger.warning(f"Attempt {attempt}/{retries} failed ({e}). Retrying in {delay:.1f}s…")
            await asyncio.sleep(delay)


# ── Base scraper ───────────────────────────────────────────────────────────────

class BaseScraper:
    SITE_KEY = "base"
    BASE_URL = ""
    MAX_LISTINGS = 25
    DELAY_RANGE = (2.0, 4.5)

    def __init__(self, city_cfg: dict):
        """
        city_cfg: one entry from cities.yml, e.g.
          { name: "Toronto, ON", search_url: "...", city: "Toronto", province: "ON" }
        """
        self.city_cfg = city_cfg
        self.search_url = city_cfg["search_url"]
        self.city = city_cfg["city"]
        self.province = city_cfg["province"]
        self._seen: set[str] = set()
        self._ts = datetime.utcnow().isoformat()

    async def _delay(self):
        await asyncio.sleep(random.uniform(*self.DELAY_RANGE))

    async def scrape(self) -> list[dict]:
        if not check_robots(self.BASE_URL):
            logger.error(f"[{self.SITE_KEY}] robots.txt blocks scraping. Skipping.")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 900},
                locale="en-CA",
                timezone_id="America/Toronto",
            )
            page = await ctx.new_page()

            # Block images/fonts to speed up loading
            await page.route(
                "**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf}",
                lambda r: r.abort()
            )

            try:
                listings = await with_retry(lambda: self._scrape_page(page))
            except Exception as e:
                logger.error(f"[{self.SITE_KEY}/{self.city}] Fatal scrape error: {e}")
                listings = []
            finally:
                await browser.close()

        logger.info(f"[{self.SITE_KEY}/{self.city}] Done — {len(listings)} listings extracted")
        return [l.to_dict() for l in listings]

    async def _scrape_page(self, page: Page) -> list[Listing]:
        raise NotImplementedError

    async def _get_text(self, el, selector: str) -> Optional[str]:
        try:
            node = await el.query_selector(selector)
            if node:
                return (await node.inner_text()).strip()
        except Exception as e:
            logger.debug(f"[{self.SITE_KEY}] Selector '{selector}' text failed: {e}")
        return None

    async def _get_attr(self, el, selector: str, attr: str) -> Optional[str]:
        try:
            node = await el.query_selector(selector)
            if node:
                return await node.get_attribute(attr)
        except Exception as e:
            logger.debug(f"[{self.SITE_KEY}] Selector '{selector}' attr '{attr}' failed: {e}")
        return None

    def _normalize_type(self, raw: str) -> str:
        r = (raw or "").lower()
        if any(x in r for x in ["condo", "apartment", "apt", "unit", "loft"]):
            return "Condo"
        if any(x in r for x in ["townhouse", "town house", "townhome", "row house", "semi"]):
            return "Townhouse"
        if any(x in r for x in ["house", "detached", "bungalow", "cottage", "duplex", "triplex"]):
            return "House"
        if any(x in r for x in ["land", "lot", "acreage", "farm", "vacant"]):
            return "Land"
        if any(x in r for x in ["commercial", "retail", "office", "industrial"]):
            return "Commercial"
        return raw.title() if raw else "Unknown"


# ── Realtor.ca scraper ─────────────────────────────────────────────────────────

class RealtorCaScraper(BaseScraper):
    """
    Realtor.ca — Canada's national MLS-backed listing portal.
    Uses Playwright to render the JS-heavy list view, then extracts cards.

    Selector maintenance notes:
    - Cards live in elements matching [class*='cardCon'] or [data-listing-id]
    - Prices are in [class*='listingCardPrice']
    - Address spans two elements: streetAddress + cityAddress
    - Beds/baths/sqft are in spans with aria-labels or title attributes
    - If layout breaks: open realtor.ca in DevTools, search for a price value
      in the Elements panel to find the new wrapper class name.
    """
    SITE_KEY = "realtor_ca"
    BASE_URL = "https://www.realtor.ca"

    async def _scrape_page(self, page: Page) -> list[Listing]:
        logger.info(f"[realtor_ca/{self.city}] Loading {self.search_url}")
        await page.goto(self.search_url, wait_until="domcontentloaded", timeout=40_000)
        await self._delay()

        # Scroll to trigger lazy rendering
        for scroll_y in ["document.body.scrollHeight * 0.4", "document.body.scrollHeight * 0.8", "document.body.scrollHeight"]:
            await page.evaluate(f"window.scrollTo(0, {scroll_y})")
            await asyncio.sleep(1.2)

        # Wait for listing cards
        card_sel = "[class*='cardCon'], [data-listing-id], [class*='listingCard']"
        try:
            await page.wait_for_selector(card_sel, timeout=20_000)
        except PWTimeout:
            logger.error(f"[realtor_ca/{self.city}] Card selector timed out — page may have changed")
            return []

        cards = await page.query_selector_all(card_sel)
        logger.info(f"[realtor_ca/{self.city}] Found {len(cards)} cards")

        listings: list[Listing] = []
        for card in cards[: self.MAX_LISTINGS]:
            await self._delay()
            try:
                listing = await self._extract(card, page)
                if listing and listing.listing_id not in self._seen:
                    self._seen.add(listing.listing_id)
                    listings.append(listing)
            except Exception as e:
                logger.warning(f"[realtor_ca/{self.city}] Card extraction failed: {e}")
        return listings

    async def _extract(self, card, page: Page) -> Optional[Listing]:
        raw_price   = await self._get_text(card, "[class*='listingCardPrice'], [class*='price']")
        raw_street  = await self._get_text(card, "[class*='streetAddress'], [class*='address']")
        raw_city    = await self._get_text(card, "[class*='cityAddress']")
        raw_beds    = await self._get_text(card, "[title*='Bedroom'], [aria-label*='Bedroom'], [class*='beds']")
        raw_baths   = await self._get_text(card, "[title*='Bathroom'], [aria-label*='Bathroom'], [class*='baths']")
        raw_sqft    = await self._get_text(card, "[title*='Square'], [aria-label*='sqft'], [class*='sqft']")
        raw_type    = await self._get_text(card, "[class*='listingCardType'], [class*='propertyType']")
        raw_status  = await self._get_text(card, "[class*='listingCardBadge'], [class*='status']")
        image_url   = await self._get_attr(card, "img", "src")
        link_href   = await self._get_attr(card, "a", "href")

        price = parse_price(raw_price or "")
        if not price:
            return None

        address_parts = " ".join(filter(None, [raw_street, raw_city or self.city]))
        postal = extract_postal(raw_city or "")

        link = urljoin(self.BASE_URL, link_href) if link_href else page.url
        lid = dedupe_id(link, address_parts)

        sqft = parse_int(raw_sqft or "")
        return Listing(
            listing_id       = lid,
            property_address = raw_street or address_parts,
            city             = self.city,
            state_or_province= self.province,
            postal_code      = postal,
            listing_price    = price,
            property_type    = self._normalize_type(raw_type or ""),
            bedrooms         = parse_int(raw_beds or ""),
            bathrooms        = parse_float(raw_baths or ""),
            square_feet      = sqft,
            price_per_sqft   = calc_ppsf(price, sqft),
            listing_date     = None,
            days_on_market   = None,
            listing_status   = self._normalize_status(raw_status or ""),
            listing_url      = link,
            image_url        = image_url,
            agent_name       = None,
            brokerage_name   = None,
            source_site      = "realtor.ca",
            scrape_timestamp = self._ts,
        )

    def _normalize_status(self, raw: str) -> str:
        r = raw.lower()
        if "sold" in r:    return "Sold"
        if "pending" in r or "conditional" in r: return "Pending"
        return "Active"


# ── Kijiji scraper ─────────────────────────────────────────────────────────────

class KijijiScraper(BaseScraper):
    """
    Kijiji Real Estate — Canada's largest classifieds platform.
    Much simpler HTML than Realtor.ca. Renders server-side with minimal JS.

    Selector maintenance notes:
    - Listing cards: [data-listing-id] or .search-item
    - Price: [class*='price'] or data-testid="listing-price"
    - Title/address is in the <a> link text
    - Beds/baths are usually in the description snippet or attribute spans
    - If layout breaks: right-click a listing price → Inspect to find new class
    """
    SITE_KEY = "kijiji"
    BASE_URL = "https://www.kijiji.ca"
    DELAY_RANGE = (1.5, 3.5)

    async def _scrape_page(self, page: Page) -> list[Listing]:
        logger.info(f"[kijiji/{self.city}] Loading {self.search_url}")
        await page.goto(self.search_url, wait_until="domcontentloaded", timeout=35_000)
        await self._delay()

        # Dismiss cookie banner if present
        try:
            await page.click("[id*='cookie'] button, [class*='consent'] button", timeout=3_000)
        except Exception:
            pass

        card_sel = "[data-listing-id], li[class*='regular-ad'], div[class*='search-item']"
        try:
            await page.wait_for_selector(card_sel, timeout=15_000)
        except PWTimeout:
            logger.error(f"[kijiji/{self.city}] Card selector timed out")
            return []

        cards = await page.query_selector_all(card_sel)
        logger.info(f"[kijiji/{self.city}] Found {len(cards)} cards")

        listings: list[Listing] = []
        for card in cards[: self.MAX_LISTINGS]:
            try:
                listing = await self._extract(card)
                if listing and listing.listing_id not in self._seen:
                    self._seen.add(listing.listing_id)
                    listings.append(listing)
            except Exception as e:
                logger.warning(f"[kijiji/{self.city}] Card extraction failed: {e}")
        return listings

    async def _extract(self, card) -> Optional[Listing]:
        lid_attr  = await card.get_attribute("data-listing-id")
        raw_price = await self._get_text(card, "[class*='price'], [data-testid*='price']")
        raw_title = await self._get_text(card, "a[class*='title'], [class*='title'] a, h3 a, h2 a")
        raw_loc   = await self._get_text(card, "[class*='location'], [class*='address']")
        raw_desc  = await self._get_text(card, "[class*='description'], p[class*='desc']")
        link_href = await self._get_attr(card, "a[class*='title'], a[href*='/v-real-estate/']", "href")
        image_url = await self._get_attr(card, "img", "src")

        price = parse_price(raw_price or "")
        if not price or price < 10_000:   # filter out low-noise listings
            return None

        # Kijiji titles often contain address info
        address = raw_title or ""
        postal  = extract_postal(raw_loc or address)

        # Try to extract beds/baths from description text
        beds  = self._extract_beds(raw_desc or raw_title or "")
        baths = self._extract_baths(raw_desc or raw_title or "")
        prop_type = self._type_from_title(raw_title or "")

        link = urljoin(self.BASE_URL, link_href) if link_href else ""
        lid = lid_attr or dedupe_id(link, address)

        return Listing(
            listing_id       = lid,
            property_address = address,
            city             = self.city,
            state_or_province= self.province,
            postal_code      = postal,
            listing_price    = price,
            property_type    = prop_type,
            bedrooms         = beds,
            bathrooms        = baths,
            square_feet      = None,
            price_per_sqft   = None,
            listing_date     = None,
            days_on_market   = None,
            listing_status   = "Active",
            listing_url      = link,
            image_url        = image_url,
            agent_name       = None,
            brokerage_name   = None,
            source_site      = "kijiji.ca",
            scrape_timestamp = self._ts,
        )

    def _extract_beds(self, text: str) -> Optional[int]:
        m = re.search(r"(\d+)\s*(?:bed|bdrm|br\b)", text, re.IGNORECASE)
        return int(m.group(1)) if m else None

    def _extract_baths(self, text: str) -> Optional[float]:
        m = re.search(r"(\d+(?:\.\d+)?)\s*(?:bath|ba\b|bathroom)", text, re.IGNORECASE)
        return float(m.group(1)) if m else None

    def _type_from_title(self, title: str) -> str:
        return self._normalize_type(title)


# ── Point2Homes scraper ────────────────────────────────────────────────────────

class Point2HomesScraper(BaseScraper):
    """
    Point2Homes — public Canadian real estate aggregator.
    Good coverage of mid-sized cities. Has more structured metadata than Kijiji.

    Selector maintenance notes:
    - Listing cards: .item-info-container or [class*='listing-card']
    - Price: .item-price or [class*='price']
    - Address: .item-address or h2 inside card
    - Beds/baths/sqft: .listing-main-features li items or [class*='feature']
    - Agent/brokerage: [class*='agent-name'], [class*='brokerage']
    - If layout breaks: search the DOM for a known price value to re-anchor
    """
    SITE_KEY = "point2homes"
    BASE_URL = "https://www.point2homes.com"

    async def _scrape_page(self, page: Page) -> list[Listing]:
        logger.info(f"[point2homes/{self.city}] Loading {self.search_url}")
        await page.goto(self.search_url, wait_until="networkidle", timeout=45_000)
        await self._delay()

        # Scroll to trigger lazy images / JS rendering
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.6)")
        await asyncio.sleep(2.0)

        card_sel = ".item-info-container, [class*='listing-card'], article[class*='listing']"
        try:
            await page.wait_for_selector(card_sel, timeout=20_000)
        except PWTimeout:
            logger.error(f"[point2homes/{self.city}] Card selector timed out")
            return []

        cards = await page.query_selector_all(card_sel)
        logger.info(f"[point2homes/{self.city}] Found {len(cards)} cards")

        listings: list[Listing] = []
        for card in cards[: self.MAX_LISTINGS]:
            await self._delay()
            try:
                listing = await self._extract(card, page)
                if listing and listing.listing_id not in self._seen:
                    self._seen.add(listing.listing_id)
                    listings.append(listing)
            except Exception as e:
                logger.warning(f"[point2homes/{self.city}] Card extraction failed: {e}")
        return listings

    async def _extract(self, card, page: Page) -> Optional[Listing]:
        raw_price  = await self._get_text(card, ".item-price, [class*='price']:not([class*='sqft'])")
        raw_addr   = await self._get_text(card, ".item-address, h2[class*='address'], [class*='address']")
        raw_type   = await self._get_text(card, "[class*='property-type'], [class*='prop-type']")
        raw_status = await self._get_text(card, "[class*='status'], [class*='badge']")
        raw_beds   = await self._get_text(card, "[class*='beds'], [title*='Bedrooms'], li:has([class*='bed'])")
        raw_baths  = await self._get_text(card, "[class*='baths'], [title*='Bathrooms'], li:has([class*='bath'])")
        raw_sqft   = await self._get_text(card, "[class*='sqft'], [class*='area'], [title*='sq']")
        raw_ppsf   = await self._get_text(card, "[class*='per-sqft'], [class*='ppsf']")
        raw_agent  = await self._get_text(card, "[class*='agent-name'], [class*='agent']")
        raw_broker = await self._get_text(card, "[class*='brokerage'], [class*='agency']")
        image_url  = await self._get_attr(card, "img[class*='listing'], img.thumb, img", "src")
        link_href  = await self._get_attr(card, "a[class*='listing'], a[href*='/real-estate/']", "href")
        raw_dom    = await self._get_text(card, "[class*='days-on'], [class*='dom']")
        raw_date   = await self._get_text(card, "[class*='list-date'], [class*='date-listed']")

        price = parse_price(raw_price or "")
        if not price:
            return None

        postal  = extract_postal(raw_addr or "")
        sqft    = parse_int(raw_sqft or "")

        # Prefer explicit ppsf, fall back to calculation
        ppsf = parse_int(raw_ppsf or "") or calc_ppsf(price, sqft)

        link = urljoin(self.BASE_URL, link_href) if link_href else page.url
        lid  = dedupe_id(link, raw_addr or "")

        dom = parse_int(raw_dom or "")
        listing_date = None
        if dom is not None:
            listing_date = (datetime.utcnow() - timedelta(days=dom)).strftime("%Y-%m-%d")
        elif raw_date:
            listing_date = self._parse_date(raw_date)

        return Listing(
            listing_id       = lid,
            property_address = raw_addr or "",
            city             = self.city,
            state_or_province= self.province,
            postal_code      = postal,
            listing_price    = price,
            property_type    = self._normalize_type(raw_type or ""),
            bedrooms         = parse_int(raw_beds or ""),
            bathrooms        = parse_float(raw_baths or ""),
            square_feet      = sqft,
            price_per_sqft   = ppsf,
            listing_date     = listing_date,
            days_on_market   = dom,
            listing_status   = self._normalize_status(raw_status or ""),
            listing_url      = link,
            image_url        = image_url,
            agent_name       = raw_agent,
            brokerage_name   = raw_broker,
            source_site      = "point2homes.com",
            scrape_timestamp = self._ts,
        )

    def _normalize_status(self, raw: str) -> str:
        r = raw.lower()
        if "sold" in r:                              return "Sold"
        if "pending" in r or "conditional" in r:    return "Pending"
        if "foreclosure" in r:                      return "Foreclosure"
        return "Active"

    def _parse_date(self, raw: str) -> Optional[str]:
        """Try common date formats; return ISO string or None."""
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None


# ── Registry ───────────────────────────────────────────────────────────────────

SCRAPER_REGISTRY: dict[str, type[BaseScraper]] = {
    "realtor_ca":   RealtorCaScraper,
    "kijiji":       KijijiScraper,
    "point2homes":  Point2HomesScraper,
}


# ── Runner ─────────────────────────────────────────────────────────────────────

async def run_site(
    site_key: str,
    city_cfg: dict,
) -> list[dict]:
    """Instantiate the right scraper and run it for one city."""
    cls = SCRAPER_REGISTRY.get(site_key)
    if not cls:
        logger.error(f"Unknown site key: {site_key}")
        return []
    scraper = cls(city_cfg)
    return await scraper.scrape()


async def run_all(
    config_path: str = "cities.yml",
    site_filter: Optional[str] = None,
    city_filter: Optional[str] = None,
    output_dir: str = "data",
) -> list[dict]:
    """
    Run all enabled sites/cities from config.

    Args:
        config_path:  Path to cities.yml
        site_filter:  If set, only run this site key (e.g. 'kijiji')
        city_filter:  If set, only run cities whose name contains this string
                      (case-insensitive, e.g. 'Vancouver')
        output_dir:   Where to write per-run JSON files
    """
    cfg = load_cities_config(config_path)
    all_listings: list[dict] = []
    seen_ids: set[str] = set()
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for site_key, site_data in cfg["sites"].items():
        if not site_data.get("enabled", True):
            logger.info(f"[{site_key}] Disabled in config — skipping")
            continue
        if site_filter and site_key != site_filter:
            continue

        for city_cfg in site_data["cities"]:
            city_name = city_cfg["name"]
            if city_filter and city_filter.lower() not in city_name.lower():
                continue

            logger.info(f"━━ Scraping [{site_key}] {city_name} ━━")
            try:
                listings = await run_site(site_key, city_cfg)
            except Exception as e:
                logger.error(f"[{site_key}/{city_name}] Unhandled error: {e}")
                listings = []

            # Cross-site deduplication
            for l in listings:
                key = l["listing_id"]
                if key not in seen_ids:
                    seen_ids.add(key)
                    all_listings.append(l)
                else:
                    logger.debug(f"Duplicate listing_id {key} skipped")

            # Per-run polite pause between cities
            await asyncio.sleep(random.uniform(3.0, 6.0))

    # Save all listings
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    out_file = f"{output_dir}/listings_{ts}.json"
    latest   = f"{output_dir}/listings.json"
    for path in [out_file, latest]:
        with open(path, "w") as f:
            json.dump(all_listings, f, indent=2, default=str)

    logger.info(f"✅ Total: {len(all_listings)} listings across all sites → {latest}")
    return all_listings


# ── Demo mode ──────────────────────────────────────────────────────────────────

def demo_listings() -> list[dict]:
    """Return mock data for testing without network access."""
    from datetime import timedelta
    rows = [
        ("24680123","42 Elm St","Toronto","ON","M5G 1H5",1125000,"House",4,3.0,1950,5,"Active","Sarah Chen","Royal LePage","realtor.ca"),
        ("24681456","810 King St W #305","Toronto","ON","M5V 1N7",699000,"Condo",1,1.0,560,12,"Active","Michael Park","Sutton Group","realtor.ca"),
        ("24682789","55 Harbord St","Toronto","ON","M5S 1G4",2250000,"House",5,4.5,3200,3,"Active","Linda Zhao","Sotheby's Realty","realtor.ca"),
        ("KJ9001234","West End Condo for Sale","Vancouver","BC","V6G 1A1",889000,"Condo",2,1.0,750,8,"Active",None,None,"kijiji.ca"),
        ("KJ9005678","3BR House Kitsilano","Vancouver","BC","V6K 1X2",1650000,"House",3,2.0,None,15,"Active",None,None,"kijiji.ca"),
        ("P2H123456","123 Jasper Ave","Calgary","AB","T2P 1C7",620000,"Condo",2,2.0,900,6,"Active","James Osei","Re/Max","point2homes.com"),
        ("P2H654321","456 Richmond Rd SW","Calgary","AB","T3E 4M7",785000,"House",3,2.5,1420,11,"Active","Emma Walsh","Royal LePage","point2homes.com"),
        ("24691456","88 Davenport Rd #301","Toronto","ON","M5R 1H8",1150000,"Condo",2,2.0,1050,30,"Active","Nancy Wu","Keller Williams","realtor.ca"),
        ("24692789","10 Niagara St","Toronto","ON","M5V 1C4",4500000,"House",5,6.0,5800,1,"Active","Victor Tran","Platinum Realty","realtor.ca"),
        ("KJ9009012","Character Home Mount Pleasant","Vancouver","BC","V5T 1B2",1890000,"House",4,3.0,None,4,"Pending",None,None,"kijiji.ca"),
        ("P2H777888","321 Wellington Cres","Ottawa","ON","K1S 3B9",1250000,"House",4,3.0,2100,9,"Active","Rita Singh","Engel & Völkers","point2homes.com"),
        ("P2H111222","88 Sparks St #1505","Ottawa","ON","K1P 5B7",549000,"Condo",1,1.0,620,21,"Active","Carlos Rivera","Realty Executives","point2homes.com"),
        ("24683012","1200 Bay St #1802","Toronto","ON","M5R 2A5",1480000,"Condo",2,2.0,1100,9,"Pending","David Kim","Bosley Real Estate","realtor.ca"),
        ("24688567","48 Ossington Ave","Toronto","ON","M6J 2Y8",1895000,"House",4,3.0,2100,4,"Sold","Tom Bradley","Forest Hill","realtor.ca"),
        ("KJ9013579","Studio Loft Downtown MTL","Montreal","QC","H2Y 1N9",325000,"Condo",0,1.0,None,17,"Active",None,None,"kijiji.ca"),
    ]
    ts = datetime.utcnow().isoformat()
    result = []
    for r in rows:
        lid,addr,city,prov,postal,price,ptype,beds,baths,sqft,dom,status,agent,broker,source = r
        ld = (datetime.utcnow() - timedelta(days=dom)).strftime("%Y-%m-%d")
        result.append(Listing(
            listing_id=lid, property_address=addr, city=city,
            state_or_province=prov, postal_code=postal,
            listing_price=price, property_type=ptype,
            bedrooms=beds, bathrooms=baths, square_feet=sqft,
            price_per_sqft=calc_ppsf(price, sqft),
            listing_date=ld, days_on_market=dom,
            listing_status=status, listing_url=f"https://{source}/listing/{lid}",
            image_url=None, agent_name=agent, brokerage_name=broker,
            source_site=source, scrape_timestamp=ts,
        ).to_dict())
    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(description="RE:PULSE Multi-Site Scraper")
    parser.add_argument("--site",   help="Run only this site key (e.g. kijiji)")
    parser.add_argument("--city",   help='Run only cities matching this string (e.g. "Vancouver")')
    parser.add_argument("--config", default="cities.yml", help="Path to cities.yml")
    parser.add_argument("--output", default="data", help="Output directory")
    parser.add_argument("--demo",   action="store_true", help="Use mock data, no network")
    args = parser.parse_args()

    if args.demo:
        listings = demo_listings()
        Path(args.output).mkdir(parents=True, exist_ok=True)
        for path in [f"{args.output}/listings.json"]:
            with open(path, "w") as f:
                json.dump(listings, f, indent=2)
        logger.info(f"Demo mode: wrote {len(listings)} mock listings")
        sys.exit(0)

    asyncio.run(run_all(
        config_path=args.config,
        site_filter=args.site,
        city_filter=args.city,
        output_dir=args.output,
    ))
