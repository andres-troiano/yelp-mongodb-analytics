import sys
from typing import Dict, Iterable, List

import requests

from .config import get_auth_headers
from .utils import get_mongo_collection, upsert_businesses, utc_now_iso


YELP_SEARCH_URL = "https://api.yelp.com/v3/businesses/search"


def fetch_businesses_for_city(city: str, limit_per_city: int = 50) -> List[Dict]:
    """Fetch a list of restaurant businesses for a given city from Yelp."""
    headers = get_auth_headers()
    if not headers:
        raise RuntimeError(
            "YELP_API_KEY is not set. Please add it to your .env file."
        )

    params = {
        "term": "restaurants",
        "location": city,
        "limit": max(1, min(50, int(limit_per_city))),
        "sort_by": "best_match",
    }

    response = requests.get(YELP_SEARCH_URL, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    businesses = data.get("businesses", [])

    fetched_at = utc_now_iso()
    enriched: List[Dict] = []
    for b in businesses:
        # Attach helpful metadata for analysis
        b["search_city"] = city
        b["fetched_at"] = fetched_at
        enriched.append(b)

    return enriched


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
