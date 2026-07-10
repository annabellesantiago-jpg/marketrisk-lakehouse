"""
setup_environment.py
─────────────────────────────────────────────────────────────────────────────
One-time environment setup script — creates the Databricks catalog, schemas,
and all Bronze tables by executing 00_setup.sql.

Triggered manually via GitHub Actions (workflow_dispatch) when setting up
a new environment. Never runs as part of the daily pipeline.
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import pathlib
from databricks import sql as databricks_sql
from ingestion.config import (
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    DATABRICKS_HTTP_PATH,
    setup_logging,
)

logger = logging.getLogger(__name__)

SETUP_SQL = pathlib.Path(__file__).parent.parent / "databricks" / "notebooks" / "00_setup.sql"


def strip_comments(sql: str) -> str:
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    return "\n".join(lines).strip()


def run_setup() -> None:
    logger.info("Reading setup SQL from %s", SETUP_SQL)
    sql_content = SETUP_SQL.read_text()

    # Split on semicolon to get individual statements
    # Filter out empty strings and comment-only blocks
    statements = [
        stripped
        for s in sql_content.split(";")
        if (stripped := strip_comments(s))
    ]

    logger.info("Connecting to Databricks...")
    with databricks_sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            for i, statement in enumerate(statements, 1):
                logger.info("Executing statement %d/%d:\n%s", i, len(statements), statement[:120])
                cursor.execute(statement)
                logger.info("Statement %d complete.", i)

    logger.info("Environment setup complete.")


if __name__ == "__main__":
    setup_logging()
    run_setup()