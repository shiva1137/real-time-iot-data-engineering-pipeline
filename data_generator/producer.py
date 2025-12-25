"""
Kafka Producer for IoT Data - MVP (Minimum Viable Product)

This module implements an idempotent Kafka producer that sends IoT sensor data
to the raw_iot_data topic. All logic uses functions - producer instance is passed
as a parameter to functions that need it.

MVP Features (Essential Only):
- Schema validation before sending (lightweight checks)
- Idempotent producer (prevents duplicates on retry)
- Partitioning by sensor_id hash
- Simple retry logic with exponential backoff (5 retries max)
- Thread-safe statistics tracking
- Basic logging

Removed in MVP (Add in Phase 2/3):
- Prometheus metrics & HTTP server
- Kafka status checking (let send failures handle it)
- Topic existence checking (let send failures handle it)
- Local queue buffering (add in Phase 3 with persistence)
- DLQ logic (add in Phase 3 when monitoring is ready)

Usage:
    python producer.py
"""

import os
import json
import time
import logging
import threading
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, Tuple
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
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

# Import generator functions (not class)
from generator import (
    initialize_sensors,
    generate_record,
    simulate_network_failure,
    print_statistics as print_generator_stats,
    get_statistics as get_generator_stats,
)

# Thread-safe lock for shared state
_stats_lock = threading.Lock()

# Module-level statistics (simplified MVP)
_producer_stats = {
    "messages_sent": 0,
    "messages_failed": 0,
    "retries": 0,
    "schema_validation_failed": 0,
}


# ============================================================================
# Thread-Safe Statistics Functions
# ============================================================================

def increment_stat(stat_name: str, value: int = 1):
    """Thread-safe increment of statistics."""
    global _producer_stats, _stats_lock
    with _stats_lock:
        if stat_name in _producer_stats:
            _producer_stats[stat_name] += value


def get_stat(stat_name: str) -> int:
    """Thread-safe get of statistics."""
    global _producer_stats, _stats_lock
    with _stats_lock:
        return _producer_stats.get(stat_name, 0)


def get_all_stats() -> Dict[str, Any]:
    """Thread-safe get all statistics."""
    global _producer_stats, _stats_lock
    with _stats_lock:
        return _producer_stats.copy()


# ============================================================================
# Schema Validation Functions (Lightweight Checks)
# ============================================================================

def validate_schema(record: Any) -> Tuple[bool, Optional[str]]:
    """
    Perform lightweight schema validation before sending to Kafka.
    
    Lightweight checks only:
    - Required fields present
    - Basic type checks (not deep validation)
    - Field format checks (sensor_id pattern)
    
    Args:
        record: Message record to validate (can be None or any type)
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check if record is None or not a dict
    if record is None:
        increment_stat("schema_validation_failed")
        return False, "Record is None"
    
    if not isinstance(record, dict):
        increment_stat("schema_validation_failed")
        return False, f"Record must be a dictionary, got {type(record).__name__}"
    
    # Required fields check
    required_fields = ["sensor_id", "timestamp", "temperature", "humidity"]
    for field in required_fields:
        if field not in record:
            increment_stat("schema_validation_failed")
            return False, f"Missing required field: {field}"
        if record[field] is None:
            increment_stat("schema_validation_failed")
            return False, f"Required field is None: {field}"
    
    # Basic type checks (lightweight)
    if not isinstance(record.get("sensor_id"), str):
        increment_stat("schema_validation_failed")
        return False, "sensor_id must be a string"
    
    if not isinstance(record.get("timestamp"), str):
        increment_stat("schema_validation_failed")
        return False, "timestamp must be a string"
    
    # Sensor ID format check (basic pattern)
    sensor_id = record.get("sensor_id", "")
    if not sensor_id.startswith("SENSOR_"):
        increment_stat("schema_validation_failed")
        return False, f"sensor_id format invalid: {sensor_id}"
    
    # Timestamp format check (basic ISO format)
    timestamp = record.get("timestamp", "")
    if "T" not in timestamp and " " not in timestamp:
        increment_stat("schema_validation_failed")
        return False, f"timestamp format invalid: {timestamp}"
    
    return True, None


# ============================================================================
# Producer Creation Functions
# ============================================================================

def create_kafka_producer(
    bootstrap_servers: str = "localhost:9092",
    enable_idempotence: bool = True,
    acks: str = "all",
    retries: int = 3
) -> KafkaProducer:
    """
    Create idempotent Kafka producer.
    
    Key configurations:
    - enable.idempotence=True: Prevents duplicates on retry (CRITICAL for data accuracy)
    - acks='all': Waits for all replicas to acknowledge (required for idempotency)
    - retries=3: Retry up to 3 times (Kafka client-level retries)
    - compression_type='snappy': Compresses messages for efficiency
    
    Args:
        bootstrap_servers: Kafka broker address
        enable_idempotence: Enable idempotent producer (NON-NEGOTIABLE)
        acks: Acknowledgment mode (must be 'all' for idempotency)
        retries: Number of retries at Kafka client level
        
    Returns:
        Configured KafkaProducer instance
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            
            # Idempotency configuration (CRITICAL - prevents duplicates)
            enable_idempotence=enable_idempotence,
            
            # Reliability configuration
            acks=acks,  # Required for idempotency
            retries=retries,  # Kafka client-level retries
            max_in_flight_requests_per_connection=5,  # Parallel requests
            
            # Performance configuration
            compression_type='snappy',  # Compress messages
            batch_size=16384,  # Batch size in bytes
            linger_ms=10,  # Wait 10ms to batch messages
            
            # Timeout configuration
            request_timeout_ms=30000,  # 30 seconds
            delivery_timeout_ms=120000,  # 2 minutes total
        )
        
        logger.info("Kafka producer created successfully (idempotent mode)")
        return producer
        
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise


def get_partition(sensor_id: str, num_partitions: int = 3) -> int:
    """
    Calculate partition for a sensor_id using hash.
    
    Why hash partitioning?
    - Ensures same sensor always goes to same partition
    - Maintains ordering per sensor (important for time-series data)
    - Distributes load evenly across partitions
    
    Args:
        sensor_id: Sensor identifier
        num_partitions: Number of partitions in topic
        
    Returns:
        Partition number (0 to num_partitions-1)
    """
    return hash(sensor_id) % num_partitions


# ============================================================================
# Message Sending Functions (Simplified MVP)
# ============================================================================

def send_to_kafka(
    producer: KafkaProducer,
    record: Dict[str, Any],
    topic: str,
    num_partitions: int = 3,
    timeout: int = 10
):
    """
    Send message to Kafka (single attempt, raises exception on failure).
    
    This function makes one attempt to send. Retry logic is handled
    by tenacity decorator in send_with_retry().
    
    Args:
        producer: KafkaProducer instance
        record: Message record to send
        topic: Target Kafka topic
        num_partitions: Number of partitions
        timeout: Timeout in seconds
        
    Returns:
        RecordMetadata if sent successfully
        
    Raises:
        KafkaError: If send fails
        KafkaTimeoutError: If send times out
    """
    sensor_id = record.get("sensor_id", "unknown")
    message_id = record.get("message_id", "unknown")
    
    # Get partition for sensor_id (ensures ordering per sensor)
    partition = get_partition(sensor_id, num_partitions)
    
    # Send to Kafka
    future = producer.send(
        topic=topic,
        key=sensor_id,  # Partition key
        value=record,
        partition=partition  # Explicit partition
    )
    
    # Wait for acknowledgment (with timeout)
    record_metadata = future.get(timeout=timeout)
    
    logger.debug(
        f"Message sent successfully: "
        f"topic={record_metadata.topic}, "
        f"partition={record_metadata.partition}, "
        f"offset={record_metadata.offset}, "
        f"sensor_id={sensor_id}, "
        f"message_id={message_id}"
    )
    return record_metadata


@retry(
    stop=stop_after_attempt(5),  # Retry up to 5 times
    wait=wait_exponential(multiplier=1, min=1, max=16),  # 1s, 2s, 4s, 8s, 16s
    retry=retry_if_exception_type((KafkaTimeoutError, KafkaError)),
    before_sleep=before_sleep_log(logger, logging.INFO),
    after=after_log(logger, logging.WARNING),
)
def send_with_retry(
    producer: KafkaProducer,
    record: Dict[str, Any],
    topic: str,
    num_partitions: int = 3
) -> bool:
    """
    Send message to Kafka with automatic retry using tenacity.
    
    Uses tenacity for clean, declarative retry logic.
    Automatically retries on Kafka errors with exponential backoff.
    
    Strategy:
    - Try up to 5 times
    - Exponential backoff: 1s → 2s → 4s → 8s → 16s
    - On final failure: Returns False (doesn't raise)
    
    Why 5 retries?
    - Handles transient network issues
    - Gives Kafka time to recover from brief outages
    - Exponential backoff prevents overwhelming broker
    - Final failure indicates persistent issue (let it fail)
    
    Args:
        producer: KafkaProducer instance
        record: Message record to send
        topic: Target Kafka topic
        num_partitions: Number of partitions
        
    Returns:
        True if sent successfully, False otherwise
    """
    sensor_id = record.get("sensor_id", "unknown")
    
    try:
        # Try to send (will retry automatically on exception)
        send_to_kafka(producer, record, topic, num_partitions)
        increment_stat("messages_sent")
        return True
    except (KafkaTimeoutError, KafkaError) as e:
        # After all retries exhausted, log and return False
        logger.warning(
            f"Max retries exceeded for {topic}: sensor_id={sensor_id}, error={e}"
        )
        increment_stat("messages_failed")
        return False


def send_record(
    producer: KafkaProducer,
    record: Dict[str, Any],
    topic: str = "raw_iot_data",
    num_partitions: int = 3
) -> bool:
    """
    Send a record to Kafka (MVP - simplified flow).
    
    Flow:
    1. Validate schema (lightweight checks)
    2. Add metadata (message_id, generated_at)
    3. Send with retry logic (5 retries, exponential backoff)
    4. Return result
    
    No Kafka status check: Let send() failures indicate problems
    No topic existence check: Let send() failures indicate problems
    No local queue: Add in Phase 3 with persistence
    No DLQ: Add in Phase 3 when monitoring is ready
    
    Args:
        producer: KafkaProducer instance
        record: IoT sensor record dictionary
        topic: Target topic (default: raw_iot_data)
        num_partitions: Number of partitions
        
    Returns:
        True if sent successfully, False otherwise
    """
    # Add metadata if missing
    if "message_id" not in record:
        record["message_id"] = str(uuid.uuid4())
    
    if "generated_at" not in record:
        record["generated_at"] = datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()
    
    # Step 1: Schema validation (lightweight checks)
    is_valid, error_msg = validate_schema(record)
    if not is_valid:
        logger.warning(f"Schema validation failed: {error_msg}. Message will not be sent.")
        increment_stat("messages_failed")
        return False
    
    # Step 2: Send with retry logic (automatic retry with tenacity, up to 5 attempts)
    return send_with_retry(producer, record, topic, num_partitions)


def flush_producer(producer: KafkaProducer):
    """Flush all pending messages."""
    producer.flush()
    logger.info("Producer flushed")


def close_producer(producer: KafkaProducer):
    """Close the producer."""
    producer.close()
    logger.info("Producer closed")


def get_producer_statistics() -> Dict[str, Any]:
    """
    Get producer statistics (MVP - simplified).
    
    Returns:
        Dictionary with producer statistics
    """
    return get_all_stats()


def print_producer_statistics():
    """Print producer statistics to logger."""
    stats = get_producer_statistics()
    
    logger.info("=" * 60)
    logger.info("Kafka Producer Statistics")
    logger.info("=" * 60)
    logger.info(f"Messages sent: {stats['messages_sent']}")
    logger.info(f"Messages failed: {stats['messages_failed']}")
    logger.info(f"Retries: {stats['retries']}")
    logger.info(f"Schema validation failed: {stats['schema_validation_failed']}")
    logger.info("=" * 60)


# ============================================================================
# Main Function
# ============================================================================

def main():
    """
    Main function to run the producer with data generator.
    
    Continuously generates and sends IoT data to Kafka.
    """
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "raw_iot_data")
    num_partitions = 3
    
    # Initialize sensors
    sensors = initialize_sensors(100)
    
    # Create Kafka producer (idempotent mode)
    producer = create_kafka_producer(bootstrap_servers=kafka_servers)
    
    logger.info("Starting IoT Data Producer (MVP)...")
    logger.info(f"Kafka servers: {kafka_servers}")
    logger.info(f"Topic: {topic}")
    logger.info(f"Generating data for {len(sensors)} sensors")
    logger.info("Sending data every 10 seconds per sensor")
    logger.info("Press Ctrl+C to stop")
    
    try:
        while True:
            for sensor in sensors:
                # Generate record
                record = generate_record(sensor, sensors)
                
                # Simulate network failure (5% chance)
                if simulate_network_failure():
                    logger.debug(f"Network failure simulated for sensor {sensor['sensor_id']}")
                    continue
                
                # Send to Kafka (with retry logic)
                success = send_record(producer, record, topic, num_partitions)
                
                if not success:
                    logger.warning(f"Failed to send record for sensor {sensor['sensor_id']}")
                
                # Wait 10 seconds before next sensor (distributed across all sensors)
                time.sleep(10 / len(sensors))
            
            # Print statistics every 100 messages
            gen_stats = get_generator_stats()
            if gen_stats["total_messages"] % 100 == 0:
                print_generator_stats()
                print_producer_statistics()
                
    except KeyboardInterrupt:
        logger.info("\nStopping producer...")
        flush_producer(producer)
        close_producer(producer)
        print_generator_stats()
        print_producer_statistics()
        logger.info("Producer stopped.")


if __name__ == "__main__":
    main()
