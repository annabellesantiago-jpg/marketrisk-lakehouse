"""
load_desk_limits.py
─────────────────────────────────────────────────────────────────────────────
Governance script — loads board-approved desk limits into Bronze.

This script is NOT part of the daily pipeline. It is triggered automatically
by GitHub Actions only when data/raw/reference/desk_limits.csv is changed
via a reviewed and approved Pull Request.

Trigger flow:
  1. Traded Risk Manager updates data/raw/reference/desk_limits.csv
  2. PR raised → reviewed and approved by CRO → merged to main
  3. GitHub Actions detects file change → runs this script automatically
  4. Script uploads to S3 → reloads Bronze
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import pathlib
import pandas as pd
from databricks import sql as databricks_sql
from ingestion.config import (
    S3_BUCKET,
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    DATABRICKS_HTTP_PATH,
    setup_logging,
)
from ingestion.s3_utils import get_client, verify_bucket, upload_df

logger = logging.getLogger(__name__)

LIMITS_FILE = pathlib.Path(__file__).parent.parent / "data" / "raw" / "reference" / "desk_limits.csv"
S3_KEY      = "raw/reference/desk_limits.csv"

TRUNCATE_SQL = "TRUNCATE TABLE market_risk_dev.bronze.desk_limits"

COPY_INTO_SQL = f"""
COPY INTO market_risk_dev.bronze.desk_limits
FROM 's3://{S3_BUCKET}/raw/reference/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
PATTERN = 'desk_limits.csv'
"""


def upload_limits(client) -> None:
    logger.info("Reading desk limits from %s", LIMITS_FILE)
    df = pd.read_csv(LIMITS_FILE)
    logger.info("Loaded %d rows:\n%s", len(df), df.to_string(index=False))
    verify_bucket(client, S3_BUCKET)
    upload_df(client, df, S3_BUCKET, S3_KEY)
    logger.info("Uploaded to s3://%s/%s", S3_BUCKET, S3_KEY)


def reload_bronze() -> None:
    logger.info("Connecting to Databricks...")
    with databricks_sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            logger.info("Truncating bronze.desk_limits...")
            cursor.execute(TRUNCATE_SQL)
            logger.info("Running COPY INTO...")
            cursor.execute(COPY_INTO_SQL)
            logger.info("Bronze reload complete.")


def main():
    client = get_client()
    upload_limits(client)
    reload_bronze()
    logger.info("Desk limits governance update complete.")


if __name__ == "__main__":
    setup_logging()
    main()