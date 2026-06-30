"""
Shared helper function for all ingestion scripts.

All scripts import from here - S3 connection logic stored in one place.
Change this script to point to a different S3-compatible store.

In actual production set-up:
    - credentials come from IAM roles (e.g. runs on EC2 instance or ECS container), not environment variables
    - the client setup here would be replaced by a Secrets Manager call (example, HashiCorp Vault, AWS secret manager)
"""

import io
import logging
import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError


logger = logging.getLogger(__name__)


def get_client():
    """
    Returns a boto3 S3 client using the default credential chain:
      1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
      2. ~/.aws/credentials
      3. IAM role (when running on EC2/ECS — production)
    """
    return boto3.client("s3", region_name=os.getenv("AWS_REGION"))


def verify_bucket(client, bucket: str) -> None:
    """
    Verify the S3 bucket is accessible before uploading.
    Called once at the start of each script — fail fast with a clear error.    
    """
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "403":
            raise PermissionError(
                f"Access denied to bucket '{bucket}'. "
                f"Check your AWS credentials and IAM permissions."
            )
        elif code == "404":
            raise FileNotFoundError(
                f"Bucket '{bucket}' does not exist."
            )
        raise
    except NoCredentialsError:
        raise PermissionError(
            "No AWS credentials found. "
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your .env file."            
        )


def upload_df(client, df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    Upload a DataFrame directly to S3 as CSV.
    No local file is written — data streams straight to S3.
    Overwrites the object if it already exists (idempotent).
    """
    body = df.to_csv(index=False).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body)
    logger.info("Uploaded s3://%s/%s (%d rows)", bucket, key, len(df))


def read_df(client, bucket: str, key: str) -> pd.DataFrame:
    """
    Read a CSV directly from S3 into a DataFrame.
    No local file is written.
    Raises FileNotFoundError if the key does not exist.
    """
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(response["Body"].read()))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise FileNotFoundError(
                f"File not found in S3: s3://{bucket}/{key}\n"
                f"Ensure the upstream script ran successfully first."
            )
        raise