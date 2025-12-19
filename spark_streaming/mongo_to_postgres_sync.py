"""
MongoDB to PostgreSQL Sync Job

This module syncs aggregated window data from MongoDB to PostgreSQL
for querying and analytics. Runs every 5 minutes.

Why sync to PostgreSQL?
- MongoDB: High write throughput (optimized for writes)
- PostgreSQL: SQL queries, dbt compatibility, ACID transactions (optimized for reads)
- Separation of concerns: Write to MongoDB, read from PostgreSQL

Usage:
    python mongo_to_postgres_sync.py
    # Or schedule with cron: */5 * * * * python mongo_to_postgres_sync.py
"""

import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pymongo import MongoClient
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@localhost:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "iot_data")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "real_time_aggregates")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "iot_analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_TABLE = "real_time_aggregates"


# ============================================================================
# MongoDB Connection Functions
# ============================================================================

def get_mongo_client() -> MongoClient:
    """
    Create MongoDB client connection.
    
    Returns:
        MongoClient instance
    """
    try:
        client = MongoClient(MONGO_URI)
        # Test connection
        client.admin.command('ping')
        logger.debug("MongoDB connection successful")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


def read_from_mongodb(client: MongoClient, last_sync_time: datetime = None) -> List[Dict[str, Any]]:
    """
    Read aggregated window data from MongoDB.
    
    Args:
        client: MongoClient instance
        last_sync_time: Only read records updated after this time (for incremental sync)
        
    Returns:
        List of aggregation records
    """
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]
    
    # Build query
    query = {}
    if last_sync_time:
        query["processed_at"] = {"$gte": last_sync_time}
    
    # Read all records (or incremental if last_sync_time provided)
    records = list(collection.find(query))
    
    logger.info(f"Read {len(records)} records from MongoDB")
    
    return records


# ============================================================================
# PostgreSQL Connection Functions
# ============================================================================

def get_postgres_connection():
    """
    Create PostgreSQL connection.
    
    Returns:
        psycopg2 connection object
    """
    try:
        conn = connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        logger.debug("PostgreSQL connection successful")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def create_table_if_not_exists(conn):
    """
    Create PostgreSQL table if it doesn't exist.
    
    Schema matches MongoDB collection structure.
    
    Args:
        conn: PostgreSQL connection
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
        sensor_id VARCHAR(50) NOT NULL,
        window_start TIMESTAMP NOT NULL,
        window_end TIMESTAMP NOT NULL,
        avg_temperature DOUBLE PRECISION,
        max_temperature DOUBLE PRECISION,
        min_temperature DOUBLE PRECISION,
        avg_humidity DOUBLE PRECISION,
        total_energy_consumption DOUBLE PRECISION,
        count BIGINT,
        location VARCHAR(100),
        state VARCHAR(100),
        device_type VARCHAR(100),
        processed_at TIMESTAMP NOT NULL,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (sensor_id, window_start)
    );
    
    CREATE INDEX IF NOT EXISTS idx_window_start ON {POSTGRES_TABLE}(window_start DESC);
    CREATE INDEX IF NOT EXISTS idx_sensor_id ON {POSTGRES_TABLE}(sensor_id);
    CREATE INDEX IF NOT EXISTS idx_location ON {POSTGRES_TABLE}(location);
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()
            logger.info(f"Table {POSTGRES_TABLE} created/verified")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()
        raise


def sync_to_postgresql(conn, records: List[Dict[str, Any]]):
    """
    Sync records from MongoDB to PostgreSQL using UPSERT (INSERT ... ON CONFLICT).
    
    Strategy: Last write wins (handles conflicts)
    - If record exists (sensor_id + window_start): Update with new values
    - If record doesn't exist: Insert new record
    
    Args:
        conn: PostgreSQL connection
        records: List of records to sync
    """
    if not records:
        logger.info("No records to sync")
        return
    
    # Prepare data for insertion
    insert_data = []
    for record in records:
        # Convert MongoDB ObjectId and handle None values
        row = (
            record.get("sensor_id"),
            record.get("window_start"),
            record.get("window_end"),
            record.get("avg_temperature"),
            record.get("max_temperature"),
            record.get("min_temperature"),
            record.get("avg_humidity"),
            record.get("total_energy_consumption"),
            record.get("count"),
            record.get("location"),
            record.get("state"),
            record.get("device_type"),
            record.get("processed_at"),
            datetime.now()  # synced_at
        )
        insert_data.append(row)
    
    # UPSERT query (INSERT ... ON CONFLICT UPDATE)
    upsert_sql = f"""
    INSERT INTO {POSTGRES_TABLE} (
        sensor_id, window_start, window_end,
        avg_temperature, max_temperature, min_temperature,
        avg_humidity, total_energy_consumption, count,
        location, state, device_type, processed_at, synced_at
    ) VALUES %s
    ON CONFLICT (sensor_id, window_start)
    DO UPDATE SET
        window_end = EXCLUDED.window_end,
        avg_temperature = EXCLUDED.avg_temperature,
        max_temperature = EXCLUDED.max_temperature,
        min_temperature = EXCLUDED.min_temperature,
        avg_humidity = EXCLUDED.avg_humidity,
        total_energy_consumption = EXCLUDED.total_energy_consumption,
        count = EXCLUDED.count,
        location = EXCLUDED.location,
        state = EXCLUDED.state,
        device_type = EXCLUDED.device_type,
        processed_at = EXCLUDED.processed_at,
        synced_at = EXCLUDED.synced_at
    """
    
    try:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, insert_data)
            conn.commit()
            logger.info(f"Synced {len(records)} records to PostgreSQL")
    except Exception as e:
        logger.error(f"Error syncing to PostgreSQL: {e}")
        conn.rollback()
        raise


# ============================================================================
# Main Sync Function
# ============================================================================

def sync_mongo_to_postgres(last_sync_time: datetime = None):
    """
    Main sync function: Read from MongoDB, write to PostgreSQL.
    
    Args:
        last_sync_time: Only sync records updated after this time (incremental)
    """
    mongo_client = None
    postgres_conn = None
    
    try:
        # Connect to MongoDB
        mongo_client = get_mongo_client()
        
        # Read from MongoDB
        records = read_from_mongodb(mongo_client, last_sync_time)
        
        if not records:
            logger.info("No new records to sync")
            return
        
        # Connect to PostgreSQL
        postgres_conn = get_postgres_connection()
        
        # Create table if not exists
        create_table_if_not_exists(postgres_conn)
        
        # Sync records
        sync_to_postgresql(postgres_conn, records)
        
        logger.info("Sync completed successfully")
        
    except Exception as e:
        logger.error(f"Error in sync: {e}", exc_info=True)
        raise
    finally:
        if mongo_client:
            mongo_client.close()
        if postgres_conn:
            postgres_conn.close()


# ============================================================================
# Main Function
# ============================================================================

def main():
    """
    Main function to run sync job.
    
    Can be run:
    - Manually: python mongo_to_postgres_sync.py
    - Scheduled: cron job every 5 minutes
    - Airflow: Task in DAG (Topic 8)
    """
    logger.info("=" * 60)
    logger.info("Starting MongoDB to PostgreSQL Sync Job")
    logger.info("=" * 60)
    logger.info(f"MongoDB: {MONGO_DATABASE}.{MONGO_COLLECTION}")
    logger.info(f"PostgreSQL: {POSTGRES_DB}.{POSTGRES_TABLE}")
    logger.info("=" * 60)
    
    try:
        # Sync all records (full sync)
        # For incremental sync, pass last_sync_time parameter
        sync_mongo_to_postgres()
        
    except Exception as e:
        logger.error(f"Sync job failed: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
