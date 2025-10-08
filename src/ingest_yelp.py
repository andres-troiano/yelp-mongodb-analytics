import sys
import time
import json
import hashlib
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from .config import get_auth_headers
from .utils import get_mongo_collection, upsert_businesses, utc_now_iso


YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"
USER_AGENT = "yelp-mongodb-analytics/1.0"

# Simple filesystem cache for GET requests
CACHE_DIR = Path("data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_key(url: str, params: Dict[str, object]) -> Path:
    payload = json.dumps([url, params], sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(payload).hexdigest()
    return CACHE_DIR / f"{digest}.json"


def _get_with_cache(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, object],
    timeout: int = 30,
    min_sleep_s: float = 0.2,
    max_retries: int = 5,
    backoff_base_s: float = 0.5,
) -> Dict:
    cache_path = _cache_key(url, params)
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    # Retry with exponential backoff and jitter
    attempt = 0
    sleep_s = min_sleep_s
    while True:
        attempt += 1
        try:
            response = session.get(url, headers=headers, params=params, timeout=timeout)
            # Dynamic throttle on 429 using Retry-After if present
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else max(sleep_s * 2, 1.0)
                time.sleep(wait)
                sleep_s = min(wait * 1.5, 10.0)
                if attempt <= max_retries:
                    continue
            response.raise_for_status()
            data = response.json()
            # Cache only successful responses
            with cache_path.open("w", encoding="utf-8") as f:
                json.dump(data, f)
            # polite sleep between requests
            time.sleep(sleep_s)
            # Gradually ramp up throttle if things are fine (but cap)
            sleep_s = min(sleep_s * 0.9, 1.0)
            return data
        except requests.RequestException as exc:
            if attempt > max_retries:
                raise
            # Exponential backoff with jitter
            jitter = 0.1 * sleep_s
            wait = sleep_s + jitter
            time.sleep(wait)
            sleep_s = min(max(sleep_s * 2, backoff_base_s), 10.0)


def fetch_businesses_for_city(
    city: str, limit_per_city: int = 50, per_page: int = 50
) -> List[Dict]:
    """Fetch a list of restaurant businesses for a given city from Yelp.

    Respects pagination using the `offset` parameter and applies throttling,
    retries with backoff, and caching.
    """
    headers = get_auth_headers()
    if not headers:
        raise RuntimeError(
            "YELP_API_KEY is not set. Please add it to your .env file."
        )
    # Identify the client if allowed
    headers.setdefault("User-Agent", USER_AGENT)

    # Yelp limits: max 50 per page; offset for pagination
    per_page = max(1, min(50, int(per_page)))
    remaining = max(1, min(1000, int(limit_per_city)))  # safety upper bound

    session = requests.Session()

    all_businesses: List[Dict] = []
    offset = 0
    fetched_at = utc_now_iso()

    while remaining > 0:
        page_limit = min(per_page, remaining)
        params: Dict[str, object] = {
            "term": "restaurants",
            "location": city,
            "limit": page_limit,
            "offset": offset,
            "sort_by": "best_match",
        }

        data = _get_with_cache(session, YELP_SEARCH_URL, headers=headers, params=params)
        businesses = data.get("businesses", [])

        # Enrich
        for b in businesses:
            b["search_city"] = city
            b["fetched_at"] = fetched_at
        all_businesses.extend(businesses)

        total_returned = len(businesses)
        print(
            f"Fetched city='{city}' page_offset={offset} returned={total_returned} accumulated={len(all_businesses)}"
        )

        if total_returned < page_limit:
            # No more pages
            break

        remaining -= total_returned
        offset += total_returned

    return all_businesses


def ingest_cities(cities: Iterable[str], limit_per_city: int = 50) -> Dict[str, int]:
    """Ingest businesses for each city and upsert them into MongoDB."""
    collection = get_mongo_collection()
    total_matched = 0
    total_upserted = 0

    for city in cities:
        businesses = fetch_businesses_for_city(city, limit_per_city=limit_per_city)
        summary = upsert_businesses(collection, businesses)
        total_matched += summary.get("matched", 0)
        total_upserted += summary.get("upserted", 0)
        print(f"City={city} matched={summary['matched']} upserted={summary['upserted']}")

    return {"matched": total_matched, "upserted": total_upserted}


def main(argv: List[str]) -> None:
    # Default set of major US cities; can be overridden via CLI args
    default_cities = [
        "New York, NY",
        "San Francisco, CA",
        "Los Angeles, CA",
        "Chicago, IL",
        "Houston, TX",
    ]

    cities = argv if argv else default_cities
    summary = ingest_cities(cities)
    print(f"Total matched={summary['matched']} upserted={summary['upserted']}")


if __name__ == "__main__":
    main(sys.argv[1:])
