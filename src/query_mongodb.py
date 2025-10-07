from typing import Any, Dict, Iterable, List

from pymongo.collection import Collection


def run_pipeline(collection: Collection, pipeline: List[Dict[str, Any]]) -> List[Dict]:
    return list(collection.aggregate(pipeline, allowDiskUse=True))


def average_rating_per_category(
    collection: Collection, min_businesses: int = 5
) -> List[Dict]:
    """Return average rating and counts per category title."""
    pipeline = [
        {"$unwind": "$categories"},
        {"$group": {
            "_id": "$categories.title",
            "avg_rating": {"$avg": "$rating"},
            "num_businesses": {"$sum": 1},
        }},
        {"$match": {"num_businesses": {"$gte": int(min_businesses)}}},
        {"$sort": {"avg_rating": -1}},
    ]
    results = run_pipeline(collection, pipeline)
    # Normalize keys for convenient DataFrame usage
    for r in results:
        r["category"] = r.pop("_id", None)
    return results


def price_level_distribution(collection: Collection) -> List[Dict]:
    """Return distribution of businesses by price level with average rating."""
    pipeline = [
        {"$project": {
            "price": {"$ifNull": ["$price", "Unknown"]},
            "rating": 1,
        }},
        {"$group": {
            "_id": "$price",
            "count": {"$sum": 1},
            "avg_rating": {"$avg": "$rating"},
        }},
        {"$sort": {"count": -1}},
    ]
    results = run_pipeline(collection, pipeline)
    for r in results:
        r["price"] = r.pop("_id", None)
    return results


def rating_reviewcount_pairs(
    collection: Collection, min_review_count: int = 0
) -> List[Dict]:
    """Return pairs of rating and review_count for correlation analysis."""
    pipeline = [
        {"$match": {"rating": {"$type": "number"}, "review_count": {"$type": "number"}}},
        {"$match": {"review_count": {"$gte": int(min_review_count)}}},
        {"$project": {"_id": 0, "rating": 1, "review_count": 1}},
    ]
    return run_pipeline(collection, pipeline)


def ratings_by_price_level(collection: Collection) -> List[Dict]:
    """Return individual ratings with associated price levels for distribution plots."""
    pipeline = [
        {"$project": {
            "_id": 0,
            "rating": 1,
            "price": {"$ifNull": ["$price", "Unknown"]},
        }},
        {"$match": {"rating": {"$type": "number"}}},
    ]
    return run_pipeline(collection, pipeline)
