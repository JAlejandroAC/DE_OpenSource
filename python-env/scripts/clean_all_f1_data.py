"""
F1 Data Cleaning Script
=======================
Cleans all F1 datasets from MinIO bronze layer and stores cleaned versions in silver layer.

Datasets processed:
- sessions
- drivers
- session_result
- starting_grid
- weather

Author: Data Engineering Team
Date: 2025-01-27
"""

from minio import Minio
import pandas as pd
from io import BytesIO
import os
from datetime import datetime

# MinIO Configuration
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "datalake"

# Dataset mapping
DATASETS = {
    "sessions": {
        "bronze_path": "bronze/sessions/f1_sessions.csv",
        "silver_path": "silver/sessions/f1_sessions_clean.csv"
    },
    "drivers": {
        "bronze_path": "bronze/drivers/f1_drivers.csv",
        "silver_path": "silver/drivers/f1_drivers_clean.csv"
    },
    "session_result": {
        "bronze_path": "bronze/session_result/f1_session_result.csv",
        "silver_path": "silver/session_result/f1_session_result_clean.csv"
    },
    "starting_grid": {
        "bronze_path": "bronze/start_grid/f1_starting_grid.csv",
        "silver_path": "silver/start_grid/f1_starting_grid_clean.csv"
    },
    "weather": {
        "bronze_path": "bronze/weather/f1_weather.csv",
        "silver_path": "silver/weather/f1_weather_clean.csv"
    }
}

def clean_sessions(df):
    """Clean sessions data"""
    print("  🧹 Cleaning sessions data...")
    
    # Remove duplicates based on session_key
    df = df.drop_duplicates(subset=['session_key'], keep='first')
    
    # Handle missing values
    df = df.dropna(subset=['session_key', 'session_name'])
    
    # Convert date columns to datetime
    date_cols = ['date_start', 'date_end']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Sort by date
    if 'date_start' in df.columns:
        df = df.sort_values('date_start', ascending=False)
    
    print(f"  ✅ Cleaned {len(df)} session records")
    return df

def clean_drivers(df):
    """Clean drivers data"""
    print("  🧹 Cleaning drivers data...")
    
    # Remove duplicates based on driver_number and session_key
    df = df.drop_duplicates(subset=['driver_number', 'session_key'], keep='first')
    
    # Handle missing values in critical columns
    df = df.dropna(subset=['driver_number'])
    
    # Fill missing names with 'Unknown'
    if 'full_name' in df.columns:
        df['full_name'] = df['full_name'].fillna('Unknown')
    
    # Sort by session and driver number
    if 'session_key' in df.columns and 'driver_number' in df.columns:
        df = df.sort_values(['session_key', 'driver_number'])
    
    print(f"  ✅ Cleaned {len(df)} driver records")
    return df

def clean_session_result(df):
    """Clean session results data"""
    print("  🧹 Cleaning session results data...")
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['session_key', 'driver_number'], keep='first')
    
    # Handle missing values
    df = df.dropna(subset=['session_key', 'driver_number'])
    
    # Convert time columns to proper format
    if 'time' in df.columns:
        df['time'] = pd.to_timedelta(df['time'], errors='coerce')
    
    # Sort by session and position
    if 'position' in df.columns:
        df = df.sort_values(['session_key', 'position'])
    
    print(f"  ✅ Cleaned {len(df)} session result records")
    return df

def clean_starting_grid(df):
    """Clean starting grid data"""
    print("  🧹 Cleaning starting grid data...")
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['session_key', 'driver_number'], keep='first')
    
    # Handle missing values
    df = df.dropna(subset=['session_key', 'driver_number'])
    
    # Sort by session and position
    if 'position' in df.columns:
        df = df.sort_values(['session_key', 'position'])
    
    print(f"  ✅ Cleaned {len(df)} starting grid records")
    return df

def clean_weather(df):
    """Clean weather data"""
    print("  🧹 Cleaning weather data...")
    
    # Remove duplicates based on meeting_key and date
    df = df.drop_duplicates(subset=['meeting_key', 'date'], keep='first')
    
    # Handle missing values
    df = df.dropna(subset=['meeting_key'])
    
    # Convert date column to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Sort by date
    if 'date' in df.columns:
        df = df.sort_values('date')
    
    print(f"  ✅ Cleaned {len(df)} weather records")
    return df

# Cleaning function mapping
CLEANING_FUNCTIONS = {
    "sessions": clean_sessions,
    "drivers": clean_drivers,
    "session_result": clean_session_result,
    "starting_grid": clean_starting_grid,
    "weather": clean_weather
}

def main():
    """Main cleaning function"""
    print(f"[{datetime.now()}] Starting F1 Data Cleaning Process...")
    print("=" * 80)
    
    # Initialize MinIO client
    try:
        minio_client = Minio(
            MINIO_HOST,
            access_key=MINIO_USER,
            secret_key=MINIO_PASSWORD,
            secure=False
        )
        print(f"✅ Connected to MinIO at {MINIO_HOST}\n")
    except Exception as e:
        print(f"❌ Failed to connect to MinIO: {e}")
        raise
    
    total_processed = 0
    
    # Process each dataset
    for dataset_name, paths in DATASETS.items():
        print(f"📦 Processing: {dataset_name}")
        print("-" * 80)
        
        try:
            # Read from bronze layer
            print(f"  📥 Reading from {paths['bronze_path']}...")
            response = minio_client.get_object(BUCKET_NAME, paths['bronze_path'])
            df = pd.read_csv(BytesIO(response.read()))
            response.close()
            response.release_conn()
            print(f"  📊 Loaded {len(df)} records")
            
            # Apply cleaning function
            if dataset_name in CLEANING_FUNCTIONS:
                df_clean = CLEANING_FUNCTIONS[dataset_name](df)
            else:
                print(f"  ⚠️  No specific cleaning function for {dataset_name}, applying generic cleaning")
                df_clean = df.drop_duplicates().dropna()
            
            # Convert to CSV
            csv_data = df_clean.to_csv(index=False).encode("utf-8")
            
            # Upload to silver layer
            minio_client.put_object(
                bucket_name=BUCKET_NAME,
                object_name=paths['silver_path'],
                data=BytesIO(csv_data),
                length=len(csv_data),
                content_type='text/csv'
            )
            print(f"  ✅ Uploaded to {paths['silver_path']}")
            print(f"  📦 File size: {len(csv_data):,} bytes\n")
            
            total_processed += 1
            
        except Exception as e:
            print(f"  ❌ Error processing {dataset_name}: {e}\n")
            continue
    
    print("=" * 80)
    print(f"[{datetime.now()}] ✨ Cleaning completed!")
    print(f"📊 Summary: {total_processed}/{len(DATASETS)} datasets processed successfully")

if __name__ == "__main__":
    main()
