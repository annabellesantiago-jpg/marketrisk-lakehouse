"""
upload_to_s3.py
─────────────────────────────────────────────────────────────────────────────
Uploads all raw CSV files from data/raw/ to the AWS S3 landing zone.
Preserves the sub-folder structure as S3 object prefixes.

Run the three fetch/generate scripts first, then run this once to upload:
  python ingestion/fetch_market_data.py
  python ingestion/generate_positions.py
  python ingestion/fetch_reference_data.py
  python ingestion/upload_to_s3.py

Credentials:
  Does NOT pass explicit credentials. Uses boto3 default credential chain:
    1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    2. ~/.aws/credentials (set via: aws configure)
    3. IAM role (when running on EC2/Lambda — used in production)
  Set credentials via .env or aws configure before running locally.
─────────────────────────────────────────────────────────────────────────────
"""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.config import S3_BUCKET, AWS_REGION

RAW_DIR = Path("data/raw")


def get_client():
    """
    Create an S3 client using boto3 default credential chain.
    No credentials are passed explicitly — boto3 reads them from
    environment variables or ~/.aws/credentials automatically.
    """
    return boto3.client("s3", region_name=AWS_REGION)


def verify_bucket_access(client) -> bool:
    """
    Check bucket is accessible before attempting uploads.
    Surfaces clear error messages for common failure modes.
    """
    try:
        client.head_bucket(Bucket=S3_BUCKET)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "403":
            print(f"ERROR: Access denied to bucket '{S3_BUCKET}'.")
            print("Check your AWS credentials and IAM permissions.")
        elif code == "404":
            print(f"ERROR: Bucket '{S3_BUCKET}' does not exist.")
        else:
            print(f"ERROR: Could not access bucket — {e}")
        return False
    except NoCredentialsError:
        print("ERROR: No AWS credentials found.")
        print("Run 'aws configure' or set AWS_ACCESS_KEY_ID and "
              "AWS_SECRET_ACCESS_KEY in your .env file.")
        return False


def upload_all():
    """
    Upload every CSV file in data/raw/ to S3.
    Folder structure is preserved as S3 object prefixes:
      data/raw/prices/AAPL.csv     →  s3://bucket/prices/AAPL.csv
      data/raw/positions/positions.csv  →  s3://bucket/positions/positions.csv
      data/raw/reference/fx_rates.csv  →  s3://bucket/reference/fx_rates.csv
    """
    client = get_client()

    if not verify_bucket_access(client):
        sys.exit(1)

    files = sorted(RAW_DIR.rglob("*.csv"))

    if not files:
        print(
            "No CSV files found in data/raw/ — "
            "run the fetch and generate scripts first:\n"
            "  python ingestion/fetch_market_data.py\n"
            "  python ingestion/generate_positions.py\n"
            "  python ingestion/fetch_reference_data.py"
        )
        return

    print(f"\nUploading {len(files)} files to s3://{S3_BUCKET}/\n")

    uploaded = 0
    failed   = 0

    for file_path in files:
        # Derive S3 key from path relative to data/raw/
        # e.g. data/raw/prices/AAPL.csv  →  prices/AAPL.csv
        key = str(file_path.relative_to(RAW_DIR)).replace("\\", "/")
        try:
            client.upload_file(str(file_path), S3_BUCKET, key)
            print(f"  ✓  s3://{S3_BUCKET}/{key}")
            uploaded += 1
        except ClientError as e:
            print(f"  ✗  FAILED: {key} — {e}")
            failed += 1

    print(f"\nUpload complete — {uploaded} succeeded, {failed} failed.")
    if uploaded > 0:
        print(
            f"Verify at: "
            f"https://s3.console.aws.amazon.com/s3/buckets/{S3_BUCKET}"
        )


if __name__ == "__main__":
    upload_all()