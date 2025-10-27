"""
F1 Data SQL Loading Script
===========================
Loads cleaned F1 data from MinIO silver layer into PostgreSQL database.

Datasets loaded:
- sessions → f1_data.sessions
- drivers → f1_data.drivers  
- session_result → f1_data.results
- starting_grid → f1_data.starting_grid
- weather → f1_data.weather

Author: Data Engineering Team
Date: 2025-01-27
"""

from minio import Minio
import pandas as pd
from io import BytesIO
import os
from datetime import datetime
from sqlalchemy import create_engine, text

# MinIO Configuration
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "datalake"

# PostgreSQL Configuration
DB_USER = os.getenv("DWH_USER", "dwh_user")
DB_PASSWORD = os.getenv("DWH_PASSWORD", "dwh_password")
DB_HOST = os.getenv("DWH_HOST", "postgres")
DB_PORT = os.getenv("DWH_PORT", "5432")
DB_NAME = os.getenv("DWH_DB", "f1_datawarehouse")
DB_SCHEMA = "f1_data"

# Dataset mapping: silver path -> SQL table name
DATASETS = {
    "silver/sessions/f1_sessions_clean.csv": "sessions",
    "silver/drivers/f1_drivers_clean.csv": "drivers",
    "silver/session_result/f1_session_result_clean.csv": "results",
    "silver/start_grid/f1_starting_grid_clean.csv": "starting_grid",
    "silver/weather/f1_weather_clean.csv": "weather"
}

def create_schema_if_not_exists(engine, schema_name):
    """Create schema if it doesn't exist"""
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        conn.commit()
    print(f"  ✅ Schema '{schema_name}' ready")

def main():
    """Main loading function"""
    print(f"[{datetime.now()}] Starting F1 Data Loading to PostgreSQL...")
    print("=" * 80)
    
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
    
    # Initialize PostgreSQL connection
    try:
        connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ Connected to PostgreSQL: {version.split(',')[0]}")
        
        # Create schema
        create_schema_if_not_exists(engine, DB_SCHEMA)
        print()
        
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        raise
    
    total_loaded = 0
    total_rows = 0
    
    # Load each dataset
    for silver_path, table_name in DATASETS.items():
        print(f"📦 Loading: {table_name}")
        print("-" * 80)
        
        try:
            # Read from silver layer
            print(f"  📥 Reading from {silver_path}...")
            response = minio_client.get_object(BUCKET_NAME, silver_path)
            df = pd.read_csv(BytesIO(response.read()))
            response.close()
            response.release_conn()
            print(f"  📊 Loaded {len(df)} records from MinIO")
            
            # Load to PostgreSQL
            print(f"  💾 Writing to PostgreSQL table: {DB_SCHEMA}.{table_name}...")
            df.to_sql(
                name=table_name,
                con=engine,
                schema=DB_SCHEMA,
                if_exists='replace',  # Options: 'fail', 'replace', 'append'
                index=False,
                method='multi',
                chunksize=1000
            )
            print(f"  ✅ Successfully loaded {len(df)} rows to {DB_SCHEMA}.{table_name}")
            print(f"  📋 Columns: {', '.join(df.columns.tolist()[:5])}{'...' if len(df.columns) > 5 else ''}\n")
            
            total_loaded += 1
            total_rows += len(df)
            
        except Exception as e:
            print(f"  ❌ Error loading {table_name}: {e}\n")
            continue
    
    print("=" * 80)
    print(f"[{datetime.now()}] ✨ Loading completed!")
    print(f"📊 Summary: {total_loaded}/{len(DATASETS)} tables loaded successfully")
    print(f"📈 Total rows inserted: {total_rows:,}")
    print(f"🗄️  Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"📚 Schema: {DB_SCHEMA}")

if __name__ == "__main__":
    main()
