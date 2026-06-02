"""
upload_to_minio.py
Uploads all raw CSV files from data/raw/ to the MinIO bucket.
Preserves the folder structure as S3 object prefixes.
"""

import boto3
from botocore.client import Config
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.config import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, MINIO_BUCKET

RAW_DIR = Path("data/raw")

def get_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def upload_all():
    client = get_client()
    files = list(RAW_DIR.rglob("*.csv"))

    if not files:
        print("No CSV files found in data/raw/ — run the fetch scripts first.")
        return

    print(f"\nUploading {len(files)} files to MinIO bucket '{MINIO_BUCKET}'...\n")

    for file_path in files:
        # e.g. data/raw/prices/AAPL.csv  →  prices/AAPL.csv
        key = str(file_path.relative_to(RAW_DIR)).replace("\\", "/")
        client.upload_file(str(file_path), MINIO_BUCKET, key)
        print(f"  Uploaded → {key}")    

    print(f"\nAll files uploaded. Check http://localhost:9001 to verify.")

if __name__ == "__main__":
    upload_all()
