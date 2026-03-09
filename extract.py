"""
extract.py
Extracts CMS Medicare ACO data from the CMS Open Payments API and saves to JSON.
"""
import requests
import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://data.cms.gov/data-api/v1/dataset/9767cb68-8ea9-4f0b-8179-9431abc89f11/data"

def extract_data(limit: int = 1000) -> list:
    """
    Pull Medicare ACO data from CMS API and save raw response to JSON.

    Args:
        limit: Number of records to fetch (default 1000)

    Returns:
        Raw data as a list of dicts
    """
    logger.info("Fetching data from CMS API...")

    params = {
        "limit": limit,
        "offset": 0
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Records returned: {len(data)}")

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    output_path = Path(f"raw_data_{timestamp}.json")
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    logger.info(f"Raw data saved → {output_path}")
    return data


if __name__ == "__main__":
    data = extract_data()
    logger.info(f"Extract complete. {len(data)} records saved to disk.")
