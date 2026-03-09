"""
extract.py
Extracts CMS Medicare ACO data from the CMS Open Payments API and saves to JSON.
"""
import requests
import json
import logging
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://data.cms.gov/data-api/v1/dataset/9767cb68-8ea9-4f0b-8179-9431abc89f11/data"

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds between retries


def extract_data(limit: int = 1000) -> list:
    """
    Pull Medicare ACO data from CMS API and save raw response to JSON.
    Retries up to MAX_RETRIES times on failure with exponential backoff.

    Args:
        limit: Number of records to fetch (default 1000)

    Returns:
        Raw data as a list of dicts
    """
    params = {
        "limit": limit,
        "offset": 0
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Fetching data from CMS API (attempt {attempt}/{MAX_RETRIES})...")

            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if not data:
                raise ValueError("API returned empty dataset")

            if not isinstance(data, list):
                raise ValueError(f"Unexpected response format: {type(data)}")

            logger.info(f"Records returned: {len(data)}")

            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            output_path = Path(f"raw_data_{timestamp}.json")
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2)

            logger.info(f"Raw data saved → {output_path}")
            return data

        except requests.exceptions.Timeout:
            logger.warning(f"Request timed out on attempt {attempt}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error on attempt {attempt}")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error on attempt {attempt}: {e}")
        except ValueError as e:
            logger.warning(f"Data validation error on attempt {attempt}: {e}")

        if attempt < MAX_RETRIES:
            wait = RETRY_DELAY * attempt  # exponential backoff: 5s, 10s, 15s
            logger.info(f"Retrying in {wait} seconds...")
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch data after {MAX_RETRIES} attempts")


if __name__ == "__main__":
    data = extract_data()
    logger.info(f"Extract complete. {len(data)} records saved to disk.")
