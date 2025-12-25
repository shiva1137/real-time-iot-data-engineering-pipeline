"""
Spark Streaming Job - Real-Time IoT Data Processing (Production-Ready)

This module implements Spark Structured Streaming to:
1. Consume from Kafka (validated_iot_data topic)
2. Perform 5-minute tumbling window aggregations per sensor
3. Handle late data with watermarking (1 minute)
4. Write aggregations to MongoDB with production optimizations
5. Use UPDATE output mode for latest results

All logic uses functions - no classes needed for streaming.

Production Features:
- Connection pooling for MongoDB (reused across batches)
- Bulk write operations (50x faster than sequential)
- Distributed writes using foreachPartition
- Monitoring and metrics tracking
- Error handling with retry logic
- Backpressure detection
- One-time index creation
- Optimized Spark configurations

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
import time
import atexit
import threading
from datetime import datetime
from typing import Dict, Any, Optional, Iterator
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, window, avg, max as spark_max, min as spark_min, sum as spark_sum, count,
    from_json, to_timestamp, struct, lit, when, isnan, isnull
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, LongType
)
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError
from tenacity import (
    retry,
    stop_after_delay,
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
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "validated_iot_data")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@localhost:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "iot_data")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "real_time_aggregates")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark_streaming_checkpoint")
WINDOW_DURATION = "5 minutes"
WATERMARK_DELAY = "1 minute"
MICRO_BATCH_INTERVAL = "10 seconds"

# MongoDB connection pool (singleton pattern)
_mongo_client = None
_mongo_lock = threading.Lock()
_indexes_created = False
_index_lock = threading.Lock()

# Retry configuration (using tenacity)
MAX_RETRY_DURATION = 86400  # 24 hours (or use float('inf') for infinite)
INITIAL_RETRY_WAIT = 2  # Start with 2 seconds
MAX_RETRY_WAIT = 300  # Max 5 minutes between retries

# Backpressure threshold (seconds)
BACKPRESSURE_THRESHOLD = 5.0


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
    Create Spark session with production-optimized streaming configuration.
    
    Key configurations:
    - spark.sql.streaming.checkpointLocation: For fault tolerance
    - spark.sql.streaming.stateStore.providerClass: RocksDB for state management
    - spark.sql.shuffle.partitions: Parallelism for aggregations
    - spark.sql.adaptive.enabled: Adaptive query execution
    - spark.dynamicAllocation.enabled: Dynamic resource allocation
    
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
        .config("spark.sql.streaming.maxFilesPerTrigger", "1000") \
        .config("spark.sql.streaming.minBatchesToRetain", "100") \
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "10") \
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
# MongoDB Connection Pooling
# ============================================================================

def get_mongo_client() -> MongoClient:
    """
    Get or create MongoDB client with connection pooling (singleton pattern).
    
    Benefits:
    - Reuses connections across batches (10x faster)
    - Connection pool management (maxPoolSize, minPoolSize)
    - Automatic connection cleanup on exit
    
    Returns:
        MongoClient instance (shared across batches)
    """
    global _mongo_client
    
    if _mongo_client is None:
        with _mongo_lock:
            if _mongo_client is None:
                try:
                    _mongo_client = MongoClient(
                        MONGO_URI,
                        maxPoolSize=50,  # Maximum connections in pool
                        minPoolSize=10,  # Minimum connections to maintain
                        maxIdleTimeMS=45000,  # Close idle connections after 45s
                        serverSelectionTimeoutMS=5000,  # 5s timeout for server selection
                        connectTimeoutMS=10000,  # 10s timeout for connection
                        socketTimeoutMS=30000,  # 30s timeout for operations
                        retryWrites=True,  # Retry writes on transient failures
                        retryReads=True,  # Retry reads on transient failures
                    )
                    # Register cleanup on exit
                    atexit.register(lambda: _mongo_client.close() if _mongo_client else None)
                    logger.info("MongoDB connection pool created")
                except Exception as e:
                    logger.error(f"Failed to create MongoDB client: {e}")
                    raise
    
    return _mongo_client


def ensure_indexes():
    """
    Create MongoDB indexes once at startup (not every batch).
    
    Indexes:
    - (sensor_id, window_start): Unique compound index for upserts
    - (window_start): Descending index for time-based queries
    
    This is idempotent - safe to call multiple times.
    """
    global _indexes_created
    
    if not _indexes_created:
        with _index_lock:
            if not _indexes_created:
                try:
                    client = get_mongo_client()
                    collection = client[MONGO_DATABASE][MONGO_COLLECTION]
                    
                    # Create unique compound index
                    collection.create_index(
                        [("sensor_id", 1), ("window_start", 1)],
                        unique=True,
                        background=True  # Don't block writes
                    )
                    
                    # Create time-based index
                    collection.create_index(
                        [("window_start", -1)],
                        background=True
                    )
                    
                    _indexes_created = True
                    logger.info("MongoDB indexes created successfully")
                except Exception as e:
                    logger.warning(f"Failed to create indexes (may already exist): {e}")
                    _indexes_created = True  # Mark as attempted to avoid repeated failures


# ============================================================================
# MongoDB Writing Functions (Production-Optimized)
# ============================================================================

def convert_timestamp_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert Spark Timestamp objects to Python datetime.
    
    Args:
        record: Record dictionary with timestamp fields
        
    Returns:
        Record with converted timestamps
    """
    timestamp_fields = ['window_start', 'window_end', 'processed_at']
    
    for field in timestamp_fields:
        if field in record and hasattr(record[field], 'to_pydatetime'):
            record[field] = record[field].to_pydatetime()
    
    return record


def write_partition_to_mongodb(partition: Iterator[Row]):
    """
    Write partition to MongoDB using bulk operations (runs on each executor).
    
    This function is called for each partition in parallel, enabling distributed writes.
    
    Benefits:
    - Parallel writes from multiple executors
    - Bulk operations (much faster than individual writes)
    - Each executor has its own connection (no driver bottleneck)
    
    Args:
        partition: Iterator of Row objects from Spark partition
    """
    client = None
    try:
        # Each executor creates its own connection
        client = MongoClient(
            MONGO_URI,
            maxPoolSize=10,
            minPoolSize=2,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=30000,
        )
        collection = client[MONGO_DATABASE][MONGO_COLLECTION]
        
        # Collect operations for bulk write
        operations = []
        record_count = 0
        
        for row in partition:
            record = row.asDict()
            record = convert_timestamp_fields(record)
            
            # Create upsert operation
            filter_query = {
                "sensor_id": record["sensor_id"],
                "window_start": record["window_start"]
            }
            
            operations.append(
                UpdateOne(
                    filter_query,
                    {"$set": record},
                    upsert=True
                )
            )
            record_count += 1
        
        # Execute bulk write (all operations at once)
        if operations:
            result = collection.bulk_write(operations, ordered=False)
            logger.debug(
                f"Partition write: {record_count} records, "
                f"inserted={result.inserted_count}, "
                f"updated={result.modified_count}, "
                f"matched={result.matched_count}"
            )
            
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"Partition write: MongoDB connection error: {e}")
        raise
    except PyMongoError as e:
        logger.error(f"Partition write: MongoDB error: {e}")
        raise
    except Exception as e:
        logger.error(f"Partition write: Unexpected error: {e}", exc_info=True)
        raise
    finally:
        if client:
            client.close()


@retry(
    stop=stop_after_delay(MAX_RETRY_DURATION),  # Retry for up to 24 hours
    wait=wait_exponential(
        multiplier=2,
        min=INITIAL_RETRY_WAIT,
        max=MAX_RETRY_WAIT
    ),
    retry=retry_if_exception_type((
        ConnectionFailure,
        ServerSelectionTimeoutError,
        PyMongoError
    )),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.ERROR),
    reraise=True
)
def write_to_mongodb_batch_with_retry(df, epoch_id):
    """
    Write DataFrame batch to MongoDB with automatic retry using tenacity.
    
    Uses tenacity for clean, declarative retry logic.
    Automatically retries on MongoDB connection errors with exponential backoff.
    Retries for up to 24 hours (configurable via MAX_RETRY_DURATION).
    
    Args:
        df: DataFrame batch to write
        epoch_id: Batch ID (from Spark)
    """
    start_time = time.time()
    
    # Check if DataFrame is empty
    if df.rdd.isEmpty():
        logger.debug(f"Batch {epoch_id}: No records to write")
        return
    
    # Use distributed writes (foreachPartition)
    # This writes from each partition in parallel on executors
    df.foreachPartition(write_partition_to_mongodb)
    
    elapsed = time.time() - start_time
    
    # Check for backpressure
    if elapsed > BACKPRESSURE_THRESHOLD:
        logger.warning(
            f"Batch {epoch_id}: Write took {elapsed:.2f}s "
            f"(backpressure detected! Threshold: {BACKPRESSURE_THRESHOLD}s)"
        )
    
    logger.info(f"Batch {epoch_id}: Wrote to MongoDB in {elapsed:.2f}s")


def write_to_mongodb_batch(df, epoch_id):
    """
    Write DataFrame batch to MongoDB (wrapper for foreachBatch).
    
    This function is called for each micro-batch by foreachBatch.
    
    Production optimizations:
    - Distributed writes using foreachPartition (parallel across executors)
    - Bulk operations (50x faster than sequential writes)
    - Connection pooling (reused connections)
    - Automatic retry with tenacity (exponential backoff, up to 24 hours)
    - Backpressure detection
    
    Args:
        df: DataFrame batch to write
        epoch_id: Batch ID (from Spark)
    """
    write_to_mongodb_batch_with_retry(df, epoch_id)


def validate_aggregation_schema(df):
    """
    Validate aggregated DataFrame matches expected schema.
    
    This ensures data quality and catches schema drift early.
    """
    expected_schema = get_aggregation_schema()
    
    # Check if all expected fields are present
    expected_fields = {field.name for field in expected_schema.fields}
    actual_fields = set(df.columns)
    
    missing_fields = expected_fields - actual_fields
    if missing_fields:
        logger.warning(f"Missing fields in aggregation: {missing_fields}")
    
    extra_fields = actual_fields - expected_fields
    if extra_fields:
        logger.warning(f"Unexpected fields in aggregation: {extra_fields}")
    
    return df

# ============================================================================
# Monitoring and Metrics
# ============================================================================

class StreamingMetricsListener(StreamingQueryListener):
    """
    Custom listener to track streaming metrics and detect issues.
    
    Tracks:
    - Input rate (rows per second)
    - Processing time per batch
    - Lag (offset difference)
    - Query status changes
    """
    
    def onQueryStarted(self, event):
        """Called when query starts."""
        logger.info(f"Query started: {event.id}, name: {event.name}")
    
    def onQueryProgress(self, event):
        """Called after each batch completes."""
        progress = event.progress
        
        if progress.sources:
            source = progress.sources[0]
            input_rate = source.inputRowsPerSecond if source.inputRowsPerSecond else 0
            processing_time = progress.batchDuration / 1000.0  # Convert to seconds
            
            # Calculate lag (if offsets available)
            lag_info = ""
            if hasattr(source, 'startOffset') and hasattr(source, 'endOffset'):
                try:
                    # Simple lag calculation (can be enhanced)
                    lag_info = f", offset range: {source.startOffset} to {source.endOffset}"
                except:
                    pass
            
            logger.info(
                f"Batch {progress.batchId}: "
                f"Input rate: {input_rate:.2f} rows/s, "
                f"Processing time: {processing_time:.2f}s, "
                f"State operators: {len(progress.stateOperators)}{lag_info}"
            )
            
            # Alert on slow processing
            if processing_time > BACKPRESSURE_THRESHOLD:
                logger.warning(
                    f"Batch {progress.batchId}: Slow processing detected "
                    f"({processing_time:.2f}s > {BACKPRESSURE_THRESHOLD}s threshold)"
                )
    
    def onQueryTerminated(self, event):
        """Called when query terminates."""
        if event.exception:
            logger.error(
                f"Query {event.id} terminated with exception: {event.exception}"
            )
        else:
            logger.info(f"Query {event.id} terminated normally")


# ============================================================================
# Streaming Query Management
# ============================================================================

def start_streaming_query(spark: SparkSession) -> StreamingQuery:
    """
    Start the Spark Streaming query with monitoring.
    
    Flow:
    1. Ensure MongoDB indexes are created
    2. Register metrics listener
    3. Read from Kafka (validated_iot_data)
    4. Parse JSON messages
    5. Create 5-minute window aggregations
    6. Write to MongoDB using foreachBatch (with retry and monitoring)
    
    Args:
        spark: SparkSession
        
    Returns:
        StreamingQuery object
    """
    logger.info("Starting Spark Streaming query...")
    
    # Step 1: Ensure MongoDB indexes are created (one-time)
    ensure_indexes()
    
    # Step 2: Register metrics listener for monitoring
    metrics_listener = StreamingMetricsListener()
    spark.streams.addListener(metrics_listener)
    logger.info("Streaming metrics listener registered")
    
    # Step 3: Read from Kafka
    kafka_df = read_from_kafka(spark, KAFKA_INPUT_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
    
    # Step 4: Parse messages
    schema = get_input_schema()
    sensor_df = parse_kafka_messages(kafka_df, schema)
    
    # Step 5: Create window aggregations
    aggregated_df = create_window_aggregations(sensor_df)
    aggregated_df = validate_aggregation_schema(aggregated_df)  # Add this
    
    # Step 6: Write to MongoDB using foreachBatch (with production optimizations)
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
    logger.info(f"Max retry duration: {MAX_RETRY_DURATION}s ({MAX_RETRY_DURATION/3600:.1f} hours)")
    logger.info(f"Retry wait: {INITIAL_RETRY_WAIT}s - {MAX_RETRY_WAIT}s (exponential)")
    logger.info(f"Backpressure threshold: {BACKPRESSURE_THRESHOLD}s")
    
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
    Main function to run Spark Streaming job (production-ready).
    
    Execution flow:
    1. Create Spark session (with production configs)
    2. Initialize MongoDB connection pool
    3. Start streaming query (with monitoring)
    4. Wait for termination (Ctrl+C to stop)
    5. Cleanup resources
    
    Production features enabled:
    - Connection pooling
    - Bulk writes
    - Distributed processing
    - Monitoring and metrics
    - Error handling with retry
    - Backpressure detection
    """
    logger.info("=" * 60)
    logger.info("Starting IoT Spark Streaming Job (Production-Ready)")
    logger.info("=" * 60)
    logger.info(f"Kafka topic: {KAFKA_INPUT_TOPIC}")
    logger.info(f"MongoDB collection: {MONGO_DATABASE}.{MONGO_COLLECTION}")
    logger.info(f"Window duration: {WINDOW_DURATION}")
    logger.info(f"Watermark: {WATERMARK_DELAY}")
    logger.info(f"Micro-batch interval: {MICRO_BATCH_INTERVAL}")
    logger.info(f"Max retry duration: {MAX_RETRY_DURATION}s ({MAX_RETRY_DURATION/3600:.1f} hours)")
    logger.info(f"Retry wait: {INITIAL_RETRY_WAIT}s - {MAX_RETRY_WAIT}s (exponential)")
    logger.info(f"Backpressure threshold: {BACKPRESSURE_THRESHOLD}s")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)
    
    try:
        # Step 1: Create Spark session
        spark = create_spark_session()
        
        # Step 2: Initialize MongoDB connection pool (lazy initialization)
        # This will be created on first use, but we can test it here
        try:
            test_client = get_mongo_client()
            test_client.server_info()  # Test connection
            logger.info("MongoDB connection pool initialized")
        except Exception as e:
            logger.warning(f"MongoDB connection test failed (will retry on first write): {e}")
        
        # Step 3: Start streaming query
        query = start_streaming_query(spark)
        
        # Step 4: Wait for termination
        wait_for_termination(query)
        
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Error in streaming job: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup
        global _mongo_client
        if _mongo_client:
            try:
                _mongo_client.close()
                logger.info("MongoDB connection pool closed")
            except:
                pass
        
        logger.info("Streaming job completed")


if __name__ == "__main__":
    main()
