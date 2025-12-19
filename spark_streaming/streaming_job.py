"""
Spark Streaming Job - Real-Time IoT Data Processing

This module implements Spark Structured Streaming to:
1. Consume from Kafka (validated_iot_data topic)
2. Perform 5-minute tumbling window aggregations per sensor
3. Handle late data with watermarking (1 minute)
4. Write aggregations to MongoDB
5. Use UPDATE output mode for latest results

All logic uses functions - no classes needed for streaming.

Key Features:
- Micro-batch interval: 10 seconds
- Window size: 5 minutes (tumbling windows)
- Watermark: 1 minute (handles late data)
- Checkpointing: Enabled for fault tolerance
- Output mode: UPDATE (shows latest window results)

Usage:
    spark-submit streaming_job.py
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, avg, max as spark_max, min as spark_min, sum as spark_sum, count,
    from_json, to_timestamp, struct, lit, when, isnan, isnull
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, LongType
)
from pyspark.sql.streaming import StreamingQuery
from pymongo import MongoClient
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
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "validated_iot_data")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@localhost:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "iot_data")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "real_time_aggregates")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark_streaming_checkpoint")
WINDOW_DURATION = "5 minutes"
WATERMARK_DELAY = "1 minute"
MICRO_BATCH_INTERVAL = "10 seconds"


# ============================================================================
# Schema Definitions
# ============================================================================

def get_input_schema() -> StructType:
    """
    Define schema for Kafka input messages (validated IoT data).
    
    Returns:
        StructType schema for IoT sensor records
    """
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("state", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("energy_consumption", DoubleType(), True),
        StructField("timestamp", StringType(), True),  # ISO format string
        StructField("signal_strength", IntegerType(), True),
        StructField("battery_level", IntegerType(), True),
        StructField("message_id", StringType(), True),
        StructField("generated_at", StringType(), True),
        StructField("data_quality_flag", StringType(), True),
    ])


def get_aggregation_schema() -> StructType:
    """
    Define schema for aggregated window results.
    
    Returns:
        StructType schema for window aggregations
    """
    return StructType([
        StructField("sensor_id", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("avg_temperature", DoubleType(), True),
        StructField("max_temperature", DoubleType(), True),
        StructField("min_temperature", DoubleType(), True),
        StructField("avg_humidity", DoubleType(), True),
        StructField("total_energy_consumption", DoubleType(), True),
        StructField("count", LongType(), True),
        StructField("location", StringType(), True),
        StructField("state", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("processed_at", TimestampType(), False),
    ])


# ============================================================================
# Spark Session Creation
# ============================================================================

def create_spark_session(app_name: str = "IoT_Streaming_Job") -> SparkSession:
    """
    Create Spark session with streaming configuration.
    
    Key configurations:
    - spark.sql.streaming.checkpointLocation: For fault tolerance
    - spark.sql.streaming.stateStore.providerClass: RocksDB for state management
    - spark.sql.shuffle.partitions: Parallelism for aggregations
    
    Args:
        app_name: Application name
        
    Returns:
        Configured SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Spark session created: {app_name}")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Checkpoint location: {CHECKPOINT_LOCATION}")
    
    return spark


# ============================================================================
# Kafka Reading Functions
# ============================================================================

def read_from_kafka(spark: SparkSession, topic: str, bootstrap_servers: str):
    """
    Read streaming data from Kafka topic.
    
    Args:
        spark: SparkSession
        topic: Kafka topic name
        bootstrap_servers: Kafka broker address
        
    Returns:
        DataFrame with Kafka message data
    """
    logger.info(f"Reading from Kafka topic: {topic}")
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.maxOffsetsPerTrigger", "10000") \
        .load()
    
    return df


def parse_kafka_messages(df, schema: StructType):
    """
    Parse JSON messages from Kafka into structured DataFrame.
    
    Args:
        df: Kafka DataFrame with value column
        schema: Expected schema for JSON parsing
        
    Returns:
        DataFrame with parsed IoT sensor data
    """
    # Parse JSON from Kafka value column
    parsed_df = df.select(
        col("key").cast("string").alias("kafka_key"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), schema).alias("data")
    )
    
    # Extract all fields from nested data column
    sensor_df = parsed_df.select(
        col("kafka_timestamp").alias("kafka_received_at"),
        col("data.*")
    )
    
    # Convert timestamp string to TimestampType
    sensor_df = sensor_df.withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    )
    
    # Filter out records with invalid timestamps
    sensor_df = sensor_df.filter(col("event_timestamp").isNotNull())
    
    # Filter out null sensor_id (critical field)
    sensor_df = sensor_df.filter(col("sensor_id").isNotNull())
    
    logger.info("Kafka messages parsed successfully")
    
    return sensor_df


# ============================================================================
# Window Aggregation Functions
# ============================================================================

def create_window_aggregations(df):
    """
    Create 5-minute tumbling window aggregations per sensor.
    
    Why 5-minute windows?
    - Balance between real-time responsiveness and computation efficiency
    - 1-minute windows: Too frequent, high computation cost
    - 10-minute windows: Too slow, poor real-time experience
    - 5-minute windows: Good balance for IoT dashboards
    
    Tumbling windows:
    - Non-overlapping windows (00:00-00:05, 00:05-00:10, etc.)
    - Each window aggregates independently
    - Simpler than sliding windows (no overlap)
    
    Args:
        df: Input DataFrame with sensor data
        
    Returns:
        DataFrame with window aggregations
    """
    logger.info(f"Creating {WINDOW_DURATION} tumbling window aggregations")
    
    # Group by sensor_id and 5-minute tumbling window
    windowed_df = df \
        .withWatermark("event_timestamp", WATERMARK_DELAY) \
        .groupBy(
            col("sensor_id"),
            window(col("event_timestamp"), WINDOW_DURATION).alias("time_window")
        ) \
        .agg(
            # Temperature aggregations
            avg(col("temperature")).alias("avg_temperature"),
            spark_max(col("temperature")).alias("max_temperature"),
            spark_min(col("temperature")).alias("min_temperature"),
            
            # Humidity aggregations
            avg(col("humidity")).alias("avg_humidity"),
            
            # Energy aggregations
            spark_sum(col("energy_consumption")).alias("total_energy_consumption"),
            
            # Count of readings
            count("*").alias("count"),
            
            # Metadata (take first value - all same for sensor_id)
            spark_max(col("location")).alias("location"),  # Use max to get non-null
            spark_max(col("state")).alias("state"),
            spark_max(col("device_type")).alias("device_type"),
        )
    
    # Extract window start and end timestamps
    windowed_df = windowed_df.select(
        col("sensor_id"),
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        col("avg_temperature"),
        col("max_temperature"),
        col("min_temperature"),
        col("avg_humidity"),
        col("total_energy_consumption"),
        col("count"),
        col("location"),
        col("state"),
        col("device_type"),
    )
    
    # Add processing timestamp
    windowed_df = windowed_df.withColumn(
        "processed_at",
        lit(datetime.now()).cast(TimestampType())
    )
    
    logger.info("Window aggregations created")
    
    return windowed_df


# ============================================================================
# MongoDB Writing Functions
# ============================================================================

def write_to_mongodb_batch(df, epoch_id):
    """
    Write DataFrame batch to MongoDB.
    
    This function is called for each micro-batch by foreachBatch.
    
    Why MongoDB for writes?
    - High write throughput (handles streaming writes efficiently)
    - Flexible schema (easy to add new fields)
    - Document-based (natural fit for JSON-like aggregations)
    - Indexed queries (fast lookups by sensor_id + window_start)
    
    Args:
        df: DataFrame batch to write
        epoch_id: Batch ID (from Spark)
    """
    try:
        # Convert Spark DataFrame to list of dictionaries
        records = df.toPandas().to_dict('records')
        
        if not records:
            logger.debug(f"Batch {epoch_id}: No records to write")
            return
        
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]
        
        # Prepare records for upsert (update if exists, insert if not)
        # Use sensor_id + window_start as unique key
        for record in records:
            # Convert Timestamp objects to datetime
            if 'window_start' in record and hasattr(record['window_start'], 'to_pydatetime'):
                record['window_start'] = record['window_start'].to_pydatetime()
            if 'window_end' in record and hasattr(record['window_end'], 'to_pydatetime'):
                record['window_end'] = record['window_end'].to_pydatetime()
            if 'processed_at' in record and hasattr(record['processed_at'], 'to_pydatetime'):
                record['processed_at'] = record['processed_at'].to_pydatetime()
            
            # Upsert: Update if exists, insert if not
            # Unique key: sensor_id + window_start
            filter_query = {
                "sensor_id": record["sensor_id"],
                "window_start": record["window_start"]
            }
            
            # Use replace_one with upsert=True
            collection.replace_one(
                filter_query,
                record,
                upsert=True
            )
        
        # Create/update indexes for fast queries
        collection.create_index([("sensor_id", 1), ("window_start", 1)], unique=True)
        collection.create_index([("window_start", -1)])  # For time-based queries
        
        client.close()
        
        logger.info(f"Batch {epoch_id}: Wrote {len(records)} records to MongoDB")
        
    except Exception as e:
        logger.error(f"Batch {epoch_id}: Error writing to MongoDB: {e}", exc_info=True)
        raise


# ============================================================================
# Streaming Query Management
# ============================================================================

def start_streaming_query(spark: SparkSession) -> StreamingQuery:
    """
    Start the Spark Streaming query.
    
    Flow:
    1. Read from Kafka (validated_iot_data)
    2. Parse JSON messages
    3. Create 5-minute window aggregations
    4. Write to MongoDB using foreachBatch
    
    Args:
        spark: SparkSession
        
    Returns:
        StreamingQuery object
    """
    logger.info("Starting Spark Streaming query...")
    
    # Step 1: Read from Kafka
    kafka_df = read_from_kafka(spark, KAFKA_INPUT_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
    
    # Step 2: Parse messages
    schema = get_input_schema()
    sensor_df = parse_kafka_messages(kafka_df, schema)
    
    # Step 3: Create window aggregations
    aggregated_df = create_window_aggregations(sensor_df)
    
    # Step 4: Write to MongoDB using foreachBatch
    query = aggregated_df \
        .writeStream \
        .outputMode("update") \
        .trigger(processingTime=MICRO_BATCH_INTERVAL) \
        .foreachBatch(write_to_mongodb_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()
    
    logger.info("Streaming query started")
    logger.info(f"Query ID: {query.id}")
    logger.info(f"Query name: {query.name}")
    logger.info(f"Micro-batch interval: {MICRO_BATCH_INTERVAL}")
    logger.info(f"Output mode: UPDATE")
    logger.info(f"Watermark: {WATERMARK_DELAY}")
    logger.info(f"Window duration: {WINDOW_DURATION}")
    
    return query


def wait_for_termination(query: StreamingQuery):
    """
    Wait for streaming query to terminate (graceful shutdown).
    
    Args:
        query: StreamingQuery to wait for
    """
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, stopping query...")
        query.stop()
        logger.info("Query stopped")


# ============================================================================
# Main Function
# ============================================================================

def main():
    """
    Main function to run Spark Streaming job.
    
    Execution flow:
    1. Create Spark session
    2. Start streaming query
    3. Wait for termination (Ctrl+C to stop)
    """
    logger.info("=" * 60)
    logger.info("Starting IoT Spark Streaming Job")
    logger.info("=" * 60)
    logger.info(f"Kafka topic: {KAFKA_INPUT_TOPIC}")
    logger.info(f"MongoDB collection: {MONGO_DATABASE}.{MONGO_COLLECTION}")
    logger.info(f"Window duration: {WINDOW_DURATION}")
    logger.info(f"Watermark: {WATERMARK_DELAY}")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Start streaming query
        query = start_streaming_query(spark)
        
        # Wait for termination
        wait_for_termination(query)
        
    except Exception as e:
        logger.error(f"Error in streaming job: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Streaming job completed")


if __name__ == "__main__":
    main()
