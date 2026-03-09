"""
load.py
Loads transformed CMS Medicare ACO data into PostgreSQL (AWS RDS).
"""
import os
import glob
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host':     os.environ.get('DB_HOST',     'localhost'),
    'database': os.environ.get('DB_NAME',     'healthcare_data'),
    'user':     os.environ.get('DB_USER',     'postgres'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'port':     os.environ.get('DB_PORT',     '5432'),
}


def get_engine():
    """Create and return a SQLAlchemy engine using environment variables."""
    url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url)


def load_data(csv_file: str) -> None:
    """
    Load transformed CMS ACO data from CSV into PostgreSQL.

    Args:
        csv_file: Path to the transformed CSV file produced by transform.py
    """
    csv_path = Path(csv_file)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    logger.info(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from CSV")

    engine = get_engine()

    try:
        logger.info("Connecting to PostgreSQL (AWS RDS)...")
        df.to_sql(
            'healthcare_records',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500
        )
        logger.info(f"Successfully inserted {len(df)} rows into healthcare_records")

        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM healthcare_records;"))
            total = result.scalar()
            logger.info(f"Total rows now in healthcare_records: {total}")

    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise

    finally:
        engine.dispose()
        logger.info("Database connection closed")


if __name__ == "__main__":
    files = sorted(glob.glob("transformed_data_*.csv"))
    if not files:
        raise FileNotFoundError("No transformed_data_*.csv found. Run transform.py first.")
    latest = files[-1]
    logger.info(f"Using most recent transform: {latest}")
    load_data(latest)
