"""
MongoDB to PostgreSQL Sync Job (Production-Ready)

This module syncs aggregated window data from MongoDB to PostgreSQL
for querying and analytics. Runs every 5 minutes.

Why sync to PostgreSQL?
- MongoDB: High write throughput (optimized for writes)
- PostgreSQL: SQL queries, dbt compatibility, ACID transactions (optimized for reads)
- Separation of concerns: Write to MongoDB, read from PostgreSQL

Production Features:
- Incremental sync with state tracking (no duplicate syncing)
- Retry logic with tenacity (handles transient failures)
- Connection pooling (efficient resource usage)
- Batch processing (handles large datasets)
- Schema validation (data quality checks)
- Monitoring and metrics (observability)
- Error recovery (idempotent operations)

Usage:
    python mongo_to_postgres_sync.py
    # Or schedule with cron: */5 * * * * python mongo_to_postgres_sync.py
"""

import os
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
from psycopg2.errors import DatabaseError, OperationalError
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log
)
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
SYNC_METADATA_TABLE = "sync_metadata"  # Table to track last sync time
BATCH_SIZE = 1000  # Process records in batches to avoid OOM
MAX_RETRY_ATTEMPTS = 3
RETRY_WAIT_MIN = 2
RETRY_WAIT_MAX = 30


# ============================================================================
# MongoDB Connection Functions (Production-Ready)
# ============================================================================

@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=2, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
    retry=retry_if_exception_type((ConnectionFailure, ServerSelectionTimeoutError, PyMongoError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.ERROR),
    reraise=True
)
def get_mongo_client() -> MongoClient:
    """
    Create MongoDB client connection with connection pooling.
    
    Returns:
        MongoClient instance with connection pooling
    """
    client = MongoClient(
        MONGO_URI,
        maxPoolSize=50,
        minPoolSize=10,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000,
        socketTimeoutMS=30000,
        retryWrites=True,
        retryReads=True
    )
    # Test connection
    client.admin.command('ping')
    logger.debug("MongoDB connection successful")
    return client


def read_from_mongodb(
    client: MongoClient, 
    last_sync_time: Optional[datetime] = None,
    batch_size: int = BATCH_SIZE
) -> List[Dict[str, Any]]:
    """
    Read aggregated window data from MongoDB with incremental sync support.
    
    Args:
        client: MongoClient instance
        last_sync_time: Only read records updated after this time (for incremental sync)
        batch_size: Maximum number of records to read (for memory efficiency)
        
    Returns:
        List of aggregation records
    """
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]
    
    # Build query for incremental sync
    query = {}
    if last_sync_time:
        # Use processed_at for incremental sync (when data was processed by Spark)
        query["processed_at"] = {"$gte": last_sync_time}
        logger.info(f"Incremental sync: Reading records after {last_sync_time}")
    else:
        logger.info("Full sync: Reading all records")
    
    # Read records with limit for memory efficiency
    # In production, you might want to use cursor for very large datasets
    records = list(collection.find(query).sort("processed_at", 1).limit(batch_size))
    
    logger.info(f"Read {len(records)} records from MongoDB")
    
    return records


# ============================================================================
# PostgreSQL Connection Functions (Production-Ready)
# ============================================================================

@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=2, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
    retry=retry_if_exception_type((OperationalError, DatabaseError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.ERROR),
    reraise=True
)
def get_postgres_connection():
    """
    Create PostgreSQL connection with retry logic.
    
    Returns:
        psycopg2 connection object
    """
    conn = connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10
    )
    logger.debug("PostgreSQL connection successful")
    return conn


def create_tables_if_not_exists(conn):
    """
    Create PostgreSQL tables if they don't exist.
    
    Creates:
    1. Main sync table (real_time_aggregates)
    2. Metadata table (sync_metadata) - tracks last sync time
    
    Args:
        conn: PostgreSQL connection
    """
    # Create main sync table
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
    CREATE INDEX IF NOT EXISTS idx_processed_at ON {POSTGRES_TABLE}(processed_at);
    """
    
    # Create metadata table for tracking sync state
    create_metadata_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SYNC_METADATA_TABLE} (
        sync_key VARCHAR(100) PRIMARY KEY,
        last_sync_time TIMESTAMP NOT NULL,
        last_sync_count INTEGER DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Initialize if not exists
    INSERT INTO {SYNC_METADATA_TABLE} (sync_key, last_sync_time, last_sync_count)
    VALUES ('mongo_to_postgres_sync', '1970-01-01 00:00:00', 0)
    ON CONFLICT (sync_key) DO NOTHING;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            cur.execute(create_metadata_table_sql)
            conn.commit()
            logger.info(f"Tables {POSTGRES_TABLE} and {SYNC_METADATA_TABLE} created/verified")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        conn.rollback()
        raise


def get_last_sync_time(conn) -> Optional[datetime]:
    """
    Get last successful sync time from metadata table.
    
    Args:
        conn: PostgreSQL connection
        
    Returns:
        Last sync time or None if never synced
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT last_sync_time FROM {SYNC_METADATA_TABLE} WHERE sync_key = %s",
                ('mongo_to_postgres_sync',)
            )
            result = cur.fetchone()
            if result and result[0]:
                last_sync = result[0]
                logger.info(f"Last sync time: {last_sync}")
                return last_sync
            return None
    except Exception as e:
        logger.warning(f"Error getting last sync time (assuming first run): {e}")
        return None


def update_last_sync_time(conn, sync_time: datetime, record_count: int):
    """
    Update last successful sync time in metadata table.
    
    Args:
        conn: PostgreSQL connection
        sync_time: Time of successful sync
        record_count: Number of records synced
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {SYNC_METADATA_TABLE}
                SET last_sync_time = %s,
                    last_sync_count = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE sync_key = 'mongo_to_postgres_sync'
                """,
                (sync_time, record_count)
            )
            conn.commit()
            logger.info(f"Updated last sync time to {sync_time} ({record_count} records)")
    except Exception as e:
        logger.error(f"Error updating last sync time: {e}")
        conn.rollback()
        raise


def validate_record(record: Dict[str, Any]) -> bool:
    """
    Validate record schema before syncing.
    
    Args:
        record: Record to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = ['sensor_id', 'window_start', 'window_end', 'processed_at']
    for field in required_fields:
        if field not in record or record[field] is None:
            logger.warning(f"Record missing required field: {field}")
            return False
    return True


def sync_to_postgresql(conn, records: List[Dict[str, Any]], batch_size: int = BATCH_SIZE):
    """
    Sync records from MongoDB to PostgreSQL using UPSERT (INSERT ... ON CONFLICT).
    
    Features:
    - Batch processing (handles large datasets)
    - Schema validation
    - Idempotent operations (safe to retry)
    - Last write wins strategy
    
    Args:
        conn: PostgreSQL connection
        records: List of records to sync
        batch_size: Number of records per batch
    """
    if not records:
        logger.info("No records to sync")
        return 0
    
    # Validate and filter records
    valid_records = [r for r in records if validate_record(r)]
    invalid_count = len(records) - len(valid_records)
    if invalid_count > 0:
        logger.warning(f"Skipped {invalid_count} invalid records")
    
    if not valid_records:
        logger.warning("No valid records to sync")
        return 0
    
    total_synced = 0
    sync_start_time = datetime.now()
    
    # Process in batches to avoid memory issues
    for i in range(0, len(valid_records), batch_size):
        batch = valid_records[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(valid_records) + batch_size - 1) // batch_size
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)")
        
        # Prepare data for insertion
        insert_data = []
        for record in batch:
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
                sync_start_time  # synced_at (same for all records in batch)
            )
            insert_data.append(row)
        
        # UPSERT query (INSERT ... ON CONFLICT UPDATE) - Idempotent
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
                total_synced += len(batch)
                logger.info(f"Batch {batch_num}/{total_batches} synced successfully")
        except Exception as e:
            logger.error(f"Error syncing batch {batch_num}: {e}")
            conn.rollback()
            raise
    
    elapsed = (datetime.now() - sync_start_time).total_seconds()
    logger.info(
        f"Synced {total_synced} records to PostgreSQL in {elapsed:.2f}s "
        f"({total_synced/elapsed:.1f} records/sec)"
    )
    
    return total_synced


# ============================================================================
# Main Sync Function (Production-Ready)
# ============================================================================

def sync_mongo_to_postgres(force_full_sync: bool = False):
    """
    Main sync function: Read from MongoDB, write to PostgreSQL.
    
    Features:
    - Incremental sync by default (tracks last sync time)
    - Automatic state management
    - Error recovery
    - Monitoring and metrics
    
    Args:
        force_full_sync: If True, sync all records (ignores last_sync_time)
    """
    mongo_client = None
    postgres_conn = None
    sync_start_time = datetime.now()
    
    try:
        # Step 1: Connect to PostgreSQL first (to get last sync time)
        postgres_conn = get_postgres_connection()
        create_tables_if_not_exists(postgres_conn)
        
        # Step 2: Get last sync time (for incremental sync)
        last_sync_time = None
        if not force_full_sync:
            last_sync_time = get_last_sync_time(postgres_conn)
            if last_sync_time:
                logger.info(f"Starting incremental sync from {last_sync_time}")
            else:
                logger.info("First sync: Reading all records")
        else:
            logger.info("Force full sync: Reading all records")
        
        # Step 3: Connect to MongoDB
        mongo_client = get_mongo_client()
        
        # Step 4: Read from MongoDB (incremental or full)
        records = read_from_mongodb(mongo_client, last_sync_time)
        
        if not records:
            logger.info("No new records to sync")
            # Update sync time even if no records (to track sync attempts)
            update_last_sync_time(postgres_conn, sync_start_time, 0)
            return
        
        # Step 5: Sync records to PostgreSQL (with batch processing)
        record_count = sync_to_postgresql(postgres_conn, records)
        
        # Step 6: Update last sync time (CRITICAL: This enables incremental sync)
        # Use max processed_at from synced records as the new sync time
        max_processed_at = max(
            (r.get("processed_at") for r in records if r.get("processed_at")),
            default=sync_start_time
        )
        update_last_sync_time(postgres_conn, max_processed_at, record_count)
        
        elapsed = (datetime.now() - sync_start_time).total_seconds()
        logger.info(
            f"Sync completed successfully: {record_count} records in {elapsed:.2f}s"
        )
        
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
    Main function to run sync job (production-ready).
    
    Features:
    - Incremental sync by default (efficient)
    - Automatic state tracking
    - Error handling with retry
    - Monitoring and logging
    
    Can be run:
    - Manually: python mongo_to_postgres_sync.py
    - Scheduled: cron job every 5 minutes: */5 * * * * python mongo_to_postgres_sync.py
    - Airflow: Task in DAG (Topic 8)
    
    Environment Variables:
    - MONGO_URI: MongoDB connection string
    - POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Sync MongoDB to PostgreSQL")
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Force full sync (ignore last sync time)"
    )
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Starting MongoDB to PostgreSQL Sync Job (Production-Ready)")
    logger.info("=" * 60)
    logger.info(f"MongoDB: {MONGO_DATABASE}.{MONGO_COLLECTION}")
    logger.info(f"PostgreSQL: {POSTGRES_DB}.{POSTGRES_TABLE}")
    logger.info(f"Sync mode: {'Full sync' if args.full_sync else 'Incremental sync'}")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max retry attempts: {MAX_RETRY_ATTEMPTS}")
    logger.info("=" * 60)
    
    try:
        sync_start = time.time()
        sync_mongo_to_postgres(force_full_sync=args.full_sync)
        elapsed = time.time() - sync_start
        logger.info(f"Total sync time: {elapsed:.2f}s")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Sync job failed: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
