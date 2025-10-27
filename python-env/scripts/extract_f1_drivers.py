"""
F1 Drivers Data Extraction Script
==================================
Extracts Formula 1 driver data from the OpenF1 API and stores it in MinIO datalake.

API Endpoint: https://api.openf1.org/v1/drivers
Output: bronze/drivers/f1_drivers.csv in MinIO datalake bucket

Author: Data Engineering Team
Date: 2025-01-27
"""

from urllib.request import urlopen
import pandas as pd
import json
import io
import os
from minio import Minio
from datetime import datetime

# MinIO Configuration
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "datalake"
API_URL = "https://api.openf1.org/v1/drivers"

def main():
    """Main extraction function"""
    print(f"[{datetime.now()}] Starting F1 Drivers extraction...")

    # Initialize MinIO client
    try:
        minio_client = Minio(
            MINIO_HOST,
            access_key=MINIO_USER,
            secret_key=MINIO_PASSWORD,
            secure=False
        )
        print(f"✅ Connected to MinIO at {MINIO_HOST}")
    except Exception as e:
        print(f"❌ Failed to connect to MinIO: {e}")
        raise

    # Fetch data from API
    try:
        print(f"📡 Fetching data from {API_URL}...")
        response = urlopen(API_URL, timeout=60)
        data = json.loads(response.read().decode('utf-8'))
        print(f"✅ Received {len(data)} records from API")
    except Exception as e:
        print(f"❌ Failed to fetch data from API: {e}")
        raise

    # Convert to DataFrame
    df = pd.DataFrame(data)
    print(f"📊 DataFrame shape: {df.shape}")
    print(f"📋 Columns: {', '.join(df.columns.tolist())}")

    # Convert to CSV
    csv_data = df.to_csv(index=False).encode("utf-8")

    # Upload to MinIO
    try:
        object_name = 'bronze/drivers/f1_drivers.csv'
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            data=io.BytesIO(csv_data),
            length=len(csv_data),
            content_type='text/csv'
        )
        print(f"✅ Data uploaded to MinIO: {BUCKET_NAME}/{object_name}")
        print(f"📦 File size: {len(csv_data):,} bytes")
    except Exception as e:
        print(f"❌ Failed to upload to MinIO: {e}")
        raise

    print(f"[{datetime.now()}] ✨ F1 Drivers extraction completed successfully!")

if __name__ == "__main__":
    main()
