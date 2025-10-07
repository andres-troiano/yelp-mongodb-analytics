import os
from typing import Dict

from dotenv import load_dotenv


# Load environment variables from a .env file if present
load_dotenv()


def get_config() -> Dict[str, str]:
    """Return application configuration sourced from environment variables.

    Expected variables:
    - YELP_API_KEY: Yelp Fusion API Bearer token
    - MONGODB_URI: MongoDB connection string (e.g., mongodb+srv://...)
    - DB_NAME: MongoDB database name (default: yelp_analytics)
    - COLLECTION_NAME: MongoDB collection name (default: businesses)
    """
    return {
        "YELP_API_KEY": os.getenv("YELP_API_KEY", ""),
        "MONGODB_URI": os.getenv("MONGODB_URI", ""),
        "DB_NAME": os.getenv("DB_NAME", "yelp_analytics"),
        "COLLECTION_NAME": os.getenv("COLLECTION_NAME", "businesses"),
    }


def get_auth_headers() -> Dict[str, str]:
    """Return HTTP headers for authenticating with the Yelp Fusion API."""
    api_key = os.getenv("YELP_API_KEY", "")
    if not api_key:
        # Leave header empty; caller should validate and raise a friendly error
        return {}
    return {"Authorization": f"Bearer {api_key}"}
