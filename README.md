# Healthcare ETL Pipeline

A production-style ETL pipeline that extracts live CMS Medicare ACO data, transforms and validates it with Python, and loads it into a PostgreSQL database hosted on AWS RDS.

## Status
✅ Live and operational

## What It Does

Extracts data from the [CMS Medicare Shared Savings Program ACO dataset](https://data.cms.gov/accountable-care-organizations/medicare-shared-savings-program-accountable-care-organizations) — a public dataset tracking Accountable Care Organizations (ACOs) across the US. ACOs are groups of doctors, hospitals, and other healthcare providers who coordinate care for Medicare patients.

**Pipeline flow:**
```
CMS REST API → extract.py → raw_data_TIMESTAMP.json
                           → transform.py → transformed_data_TIMESTAMP.csv
                                          → load.py → AWS RDS (PostgreSQL)
```

## Tech Stack

- Python 3.x
- Pandas
- Requests
- SQLAlchemy
- psycopg2
- PostgreSQL (AWS RDS)
- AWS S3 / EC2

## Project Structure
```
healthcare-etl-pipeline/
├── extract.py       # Pulls data from CMS API with retry logic
├── transform.py     # Cleans, validates, and enriches raw data
├── load.py          # Bulk loads transformed data into PostgreSQL on AWS RDS
└── sql/             # Analytics queries against the loaded dataset
```

## Setup

**1. Clone the repo**
```bash
git clone https://github.com/ts-data627/healthcare-etl-pipeline.git
cd healthcare-etl-pipeline
```

**2. Install dependencies**
```bash
pip install requests pandas sqlalchemy psycopg2-binary
```

**3. Set environment variables**
```bash
export DB_HOST=your-rds-endpoint.us-east-1.rds.amazonaws.com
export DB_NAME=healthcare_data
export DB_USER=postgres
export DB_PASSWORD=your_password
export DB_PORT=5432
```

## Running the Pipeline
```bash
# Step 1 — Extract
python extract.py

# Step 2 — Transform
python transform.py

# Step 3 — Load
python load.py
```

## Output

- **1,000 records** extracted per run
- **33 columns** after transformation including calculated fields
- Data loaded into `healthcare_records` table in PostgreSQL on AWS RDS

## Key Features

- Retry logic with exponential backoff on API failures
- Timestamped raw JSON and CSV outputs — no overwrites
- Type-specific data cleaning (dates, flags, strings, numerics)
- Data validation with logged warnings on quality issues
- Bulk inserts with chunked loading for performance
- Credentials managed via environment variables — no hardcoded secrets

## SQL Analytics

The `/sql` folder contains queries that answer business questions about ACO performance, track distribution, and geographic coverage across the US healthcare system.

## Author

Tevin Sellers | [GitHub](https://github.com/ts-data627) | [LinkedIn](https://www.linkedin.com/in/tevin-s-ba030512b)
