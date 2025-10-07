from datetime import datetime, timezone
from typing import Iterable, List, Dict, Optional

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection

from .config import get_config


# Ensure environment variables are loaded for Mongo connection
load_dotenv()


def get_mongo_client() -> MongoClient:
    config = get_config()
    mongodb_uri = config.get("MONGODB_URI", "")
    if not mongodb_uri:
        raise RuntimeError(
            "MONGODB_URI is not set. Please configure it in your .env file."
        )
    return MongoClient(mongodb_uri)


def get_mongo_collection(
    db_name: Optional[str] = None, collection_name: Optional[str] = None
) -> Collection:
    config = get_config()
    database_name = db_name or config.get("DB_NAME", "yelp_analytics")
    coll_name = collection_name or config.get("COLLECTION_NAME", "businesses")

    client = get_mongo_client()
    collection = client[database_name][coll_name]

    # Ensure a unique index on Yelp business id for idempotent upserts
    try:
        collection.create_index("id", unique=True, name="unique_yelp_business_id")
    except Exception:
        # Index may already exist or the user may not have permissions
        pass

    return collection


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_businesses(
    collection: Collection,
    businesses: Iterable[Dict],
) -> Dict[str, int]:
    """Upsert a batch of Yelp business documents by their Yelp `id`.

    Returns a summary with counts of matched and upserted documents.
    """
    operations: List[UpdateOne] = []
    for business in businesses:
        business_id = business.get("id")
        if not business_id:
            # Skip documents that don't have a Yelp id
            continue
        operations.append(
            UpdateOne(
                {"id": business_id},
                {"$set": business},
                upsert=True,
            )
        )

    if not operations:
        return {"matched": 0, "upserted": 0}

    result = collection.bulk_write(operations, ordered=False)
    matched = result.matched_count
    upserted = len(result.upserted_ids) if result.upserted_ids else 0
    return {"matched": matched, "upserted": upserted}
