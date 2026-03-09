"""
transform.py
Cleans & transforms raw CMS Medicare ACO data for loading into PostgreSQL.
"""
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def transform_data(input_file: str) -> pd.DataFrame:
    """
    Transform raw CMS Medicare ACO JSON data into a clean DataFrame.

    Args:
        input_file: Path to the raw JSON file produced by extract.py

    Returns:
        Cleaned and enriched DataFrame ready for loading into PostgreSQL.
    """
    logger.info(f"Loading raw data from {input_file}")

    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    with open(input_path, 'r') as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")

    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')

    for date_col in ['initial_start_date', 'current_start_date']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            nulls = df[date_col].isna().sum()
            if nulls > 0:
                logger.warning(f"{nulls} unparseable values in '{date_col}'")

    flag_cols = [
        'basic_track', 'enhanced_track', 'high_revenue_aco', 'low_revenue_aco',
        'adv_pay', 'aim', 'aip', 'pss', 'snf_3_day_rule_waiver',
        'prospective_assignment', 'retrospective_assignment',
        're_entering_aco', 'pc_flex_agreement_status'
    ]
    for col in flag_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    numeric_cols = ['agreement_period_num']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    string_cols = [
        'par_lbn', 'aco_name', 'aco_service_area', 'basic_track_level',
        'aco_address', 'aco_public_reporting_website', 'aco_exec_name',
        'aco_exec_email', 'aco_exec_phone', 'aco_public_name',
        'aco_public_email', 'aco_public_phone',
        'aco_compliance_contact_name', 'aco_medical_director_name'
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({'': None, 'nan': None})

    df['track_type'] = df.apply(
        lambda r: 'ENHANCED' if r.get('enhanced_track') == 1 else (
                  'BASIC'    if r.get('basic_track')    == 1 else 'UNKNOWN'),
        axis=1
    )
    df['transformed_at'] = datetime.utcnow()

    _validate(df)

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    output_file = f"transformed_data_{timestamp}.csv"
    df.to_csv(output_file, index=False)

    logger.info(f"Saved cleaned data → {output_file}")
    logger.info(f"Final shape: {df.shape}")

    return df


def _validate(df: pd.DataFrame) -> None:
    """Run basic data quality checks and log warnings."""
    if 'aco_id' in df.columns:
        nulls = df['aco_id'].isna().sum()
        if nulls > 0:
            logger.warning(f"DATA QUALITY: {nulls} records missing aco_id (primary key)")

    if 'aco_service_area' in df.columns:
        invalid = df[~df['aco_service_area'].isin(
            ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID',
             'IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS',
             'MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK',
             'OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV',
             'WI','WY','DC']
        )]
        if len(invalid) > 0:
            logger.warning(f"DATA QUALITY: {len(invalid)} records with unexpected state codes")

    logger.info("Data validation complete")


if __name__ == "__main__":
    import glob
    raw_files = sorted(glob.glob("raw_data_*.json"))
    if not raw_files:
        raise FileNotFoundError("No raw_data_*.json files found. Run extract.py first.")
    latest = raw_files[-1]
    logger.info(f"Using most recent extract: {latest}")
    transform_data(latest)
