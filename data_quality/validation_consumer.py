"""
Kafka Validation Consumer for IoT Data - Function-Based Implementation

This module implements a comprehensive validation consumer that:
1. Reads from raw_iot_data topic
2. Validates data quality comprehensively
3. Routes valid data to validated_iot_data topic
4. Routes invalid data to dlq_iot_data topic with failure reasons
5. Tracks quality metrics

All logic uses functions - no classes needed for validation.

Validation Rules:
- Schema validation (all required fields present, no extra unexpected fields)
- Type validation (temperature is float, timestamp is valid datetime)
- Range validation (temperature -50 to 50°C, humidity 0-100%, etc.)
- Format validation (sensor_id matches pattern, timestamp format correct)
- Freshness validation (timestamp < 5 minutes old, not in future)
- Duplicate detection (same sensor_id + timestamp within 5-second window)
- Completeness validation (critical fields not null)

Usage:
    python validation_consumer.py
"""

import os
import json
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
REQUIRED_FIELDS = [
    "sensor_id", "location", "state", "device_type",
    "temperature", "humidity", "energy_consumption",
    "timestamp", "signal_strength", "battery_level"
]

FIELD_TYPES = {
    "sensor_id": str,
    "location": str,
    "state": str,
    "device_type": str,
    "temperature": (float, int),
    "humidity": (float, int),
    "energy_consumption": (float, int),
    "timestamp": str,
    "signal_strength": (int, float),
    "battery_level": (int, float),
}

VALUE_RANGES = {
    "temperature": (-50, 50),  # Celsius
    "humidity": (0, 100),  # Percentage
    "energy_consumption": (0, 10),  # kWh
    "signal_strength": (-150, 0),  # dBm
    "battery_level": (0, 100),  # Percentage
}

CRITICAL_FIELDS = ["sensor_id", "temperature", "timestamp"]
DUPLICATE_WINDOW_SECONDS = 5

# Module-level state (for duplicate tracking and metrics)
_duplicate_tracker = defaultdict(set)  # {(sensor_id, timestamp_window): set of message_ids}
_validation_metrics = {
    "total_processed": 0,
    "valid_count": 0,
    "invalid_count": 0,
    "failure_types": defaultdict(int),
    "by_sensor": defaultdict(lambda: {"valid": 0, "invalid": 0}),
    "by_location": defaultdict(lambda: {"valid": 0, "invalid": 0}),
    "by_device_type": defaultdict(lambda: {"valid": 0, "invalid": 0}),
    "last_processed_timestamp": None,
}


# ============================================================================
# Validation Result Helper (replaces ValidationResult class)
# ============================================================================

def create_validation_result(is_valid: bool = True, failure_reasons: List[str] = None) -> Dict[str, Any]:
    """
    Create a validation result dictionary.
    
    Args:
        is_valid: Whether validation passed
        failure_reasons: List of failure reasons
        
    Returns:
        Dictionary with is_valid and failure_reasons
    """
    return {
        "is_valid": is_valid,
        "failure_reasons": failure_reasons or []
    }


def add_failure_reason(result: Dict[str, Any], reason: str):
    """
    Add a failure reason to validation result.
    
    Args:
        result: Validation result dictionary
        reason: Failure reason to add
    """
    result["failure_reasons"].append(reason)
    result["is_valid"] = False


# ============================================================================
# Validation Functions
# ============================================================================

def validate_schema(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate schema: all required fields present, no unexpected fields.
    
    Args:
        record: Record dictionary to validate
        
    Returns:
        Validation result dictionary
    """
    result = create_validation_result()
    
    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in record:
            add_failure_reason(result, f"Missing required field: {field}")
    
    # Check for unexpected fields
    expected_fields = set(REQUIRED_FIELDS) | {"message_id", "generated_at", "data_quality_flag"}
    unexpected_fields = set(record.keys()) - expected_fields
    if unexpected_fields:
        known_metadata = {"unexpected_field"}
        unexpected = unexpected_fields - known_metadata
        if unexpected:
            add_failure_reason(result, f"Unexpected fields: {', '.join(unexpected)}")
    
    return result


def validate_types(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate field types.
    
    Args:
        record: Record dictionary to validate
        
    Returns:
        Validation result dictionary
    """
    result = create_validation_result()
    
    for field, expected_type in FIELD_TYPES.items():
        if field not in record:
            continue
        
        value = record[field]
        
        # Skip null values
        if value is None:
            continue
        
        # Check type
        if isinstance(expected_type, tuple):
            if not isinstance(value, expected_type):
                # Try to coerce if it's a string representation
                if isinstance(value, str):
                    try:
                        if float in expected_type or int in expected_type:
                            float(value)  # Test if convertible
                            continue
                    except (ValueError, TypeError):
                        pass
                
                add_failure_reason(
                    result,
                    f"Type mismatch for {field}: expected {expected_type}, got {type(value).__name__}"
                )
        else:
            if not isinstance(value, expected_type):
                add_failure_reason(
                    result,
                    f"Type mismatch for {field}: expected {expected_type.__name__}, got {type(value).__name__}"
                )
    
    return result


def validate_ranges(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate value ranges.
    
    Args:
        record: Record dictionary to validate
        
    Returns:
        Validation result dictionary
    """
    result = create_validation_result()
    
    for field, (min_val, max_val) in VALUE_RANGES.items():
        if field not in record:
            continue
        
        value = record[field]
        
        # Skip null values
        if value is None:
            continue
        
        # Convert to float if string
        try:
            if isinstance(value, str):
                if value.lower() in ["null", "none", "n/a", ""]:
                    continue
                value = float(value)
            
            # Check range
            if value < min_val or value > max_val:
                add_failure_reason(
                    result,
                    f"Out of range for {field}: {value} (expected {min_val} to {max_val})"
                )
        except (ValueError, TypeError):
            continue
    
    return result


def validate_format(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate field formats.
    
    Args:
        record: Record dictionary to validate (may be modified in place)
        
    Returns:
        Validation result dictionary
    """
    result = create_validation_result()
    
    # Validate sensor_id format
    if "sensor_id" in record and record["sensor_id"]:
        sensor_id = str(record["sensor_id"]).strip()
        pattern = r"^SENSOR_[A-Z]{3}_\d{3}$"
        if not re.match(pattern, sensor_id):
            add_failure_reason(result, f"Invalid sensor_id format: {sensor_id} (expected SENSOR_XXX_###)")
        else:
            record["sensor_id"] = sensor_id  # Update with trimmed version
    
    # Validate timestamp format
    if "timestamp" in record and record["timestamp"]:
        timestamp_str = record["timestamp"]
        try:
            datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            add_failure_reason(result, f"Invalid timestamp format: {timestamp_str} (expected ISO 8601)")
    
    # Normalize location
    if "location" in record and record["location"]:
        record["location"] = str(record["location"]).strip()
    
    return result


def validate_freshness(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate data freshness.
    
    Args:
        record: Record dictionary to validate
        
    Returns:
        Validation result dictionary
    """
    result = create_validation_result()
    
    if "timestamp" not in record or not record["timestamp"]:
        return result
    
    try:
        timestamp_str = record["timestamp"]
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        now = datetime.utcnow()
        
        # Check if in future
        if dt > now:
            add_failure_reason(
                result,
                f"Future timestamp: {timestamp_str} (current time: {now.isoformat()})"
            )
        
        # Check if too old (> 5 minutes)
        age = now - dt
        if age > timedelta(minutes=5):
            add_failure_reason(
                result,
                f"Stale data: {age.total_seconds():.0f} seconds old (max 5 minutes)"
            )
    except (ValueError, AttributeError):
        pass
    
    return result


def validate_completeness(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate completeness: critical fields not null.
    
    Args:
        record: Record dictionary to validate
        
    Returns:
        Validation result dictionary
    """
    result = create_validation_result()
    
    for field in CRITICAL_FIELDS:
        if field not in record:
            continue
        
        value = record[field]
        
        # Check for null
        if value is None:
            add_failure_reason(result, f"Critical field {field} is null")
        
        # Check for null strings
        elif isinstance(value, str) and value.lower() in ["null", "none", "n/a", ""]:
            add_failure_reason(result, f"Critical field {field} is null string: '{value}'")
    
    return result


def validate_duplicates(record: Dict[str, Any], message_id: str) -> Dict[str, Any]:
    """
    Detect duplicate records.
    
    Args:
        record: Record dictionary to validate
        message_id: Message ID for duplicate detection
        
    Returns:
        Validation result dictionary
    """
    global _duplicate_tracker
    
    result = create_validation_result()
    
    sensor_id = record.get("sensor_id")
    timestamp = record.get("timestamp")
    
    if not sensor_id or not timestamp:
        return result
    
    try:
        # Parse timestamp to get window
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        window_start = dt - timedelta(seconds=dt.second % DUPLICATE_WINDOW_SECONDS)
        window_key = (sensor_id, window_start.isoformat())
        
        # Check for exact duplicate
        if message_id in _duplicate_tracker.get(window_key, set()):
            add_failure_reason(result, f"Exact duplicate: message_id {message_id} already seen")
            return result
        
        # Check for near-duplicate
        if window_key in _duplicate_tracker:
            add_failure_reason(
                result,
                f"Near-duplicate: same sensor_id {sensor_id} and timestamp window"
            )
        
        # Track this message
        if window_key not in _duplicate_tracker:
            _duplicate_tracker[window_key] = set()
        _duplicate_tracker[window_key].add(message_id)
        
        # Cleanup old windows
        cutoff = datetime.utcnow() - timedelta(minutes=1)
        keys_to_remove = [
            k for k in _duplicate_tracker.keys()
            if datetime.fromisoformat(k[1]) < cutoff
        ]
        for k in keys_to_remove:
            del _duplicate_tracker[k]
            
    except (ValueError, AttributeError):
        pass
    
    return result


def validate_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform comprehensive validation.
    
    Args:
        record: IoT sensor record
        
    Returns:
        Validation result dictionary
    """
    result = create_validation_result()
    
    # Run all validations
    validations = [
        validate_schema(record),
        validate_types(record),
        validate_completeness(record),
        validate_format(record),
        validate_ranges(record),
        validate_freshness(record),
    ]
    
    # Check for duplicates
    message_id = record.get("message_id", "unknown")
    validations.append(validate_duplicates(record, message_id))
    
    # Aggregate results
    for validation in validations:
        if not validation["is_valid"]:
            result["failure_reasons"].extend(validation["failure_reasons"])
            result["is_valid"] = False
    
    return result


# ============================================================================
# Consumer Functions
# ============================================================================

def create_kafka_consumer(
    bootstrap_servers: str,
    input_topic: str,
    group_id: str = "iot_validation_consumer_group"
) -> KafkaConsumer:
    """
    Create Kafka consumer.
    
    Args:
        bootstrap_servers: Kafka broker address
        input_topic: Topic to consume from
        group_id: Consumer group ID
        
    Returns:
        KafkaConsumer instance
    """
    return KafkaConsumer(
        input_topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create Kafka producer for routing.
    
    Args:
        bootstrap_servers: Kafka broker address
        
    Returns:
        KafkaProducer instance
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
    )


def send_to_topic(producer: KafkaProducer, topic: str, record: Dict[str, Any], key: str):
    """
    Send record to Kafka topic.
    
    Args:
        producer: KafkaProducer instance
        topic: Target topic
        record: Record to send
        key: Partition key (sensor_id)
    """
    try:
        future = producer.send(topic, key=key, value=record)
        future.get(timeout=5)
    except Exception as e:
        logger.error(f"Failed to send to {topic}: {e}")
        raise


def process_message(
    message: Any,
    producer: KafkaProducer,
    output_topic: str,
    dlq_topic: str
) -> bool:
    """
    Process a single message: validate and route.
    
    Args:
        message: Kafka message
        producer: KafkaProducer instance
        output_topic: Topic for valid data
        dlq_topic: Topic for invalid data
        
    Returns:
        True if processed successfully, False otherwise
    """
    global _validation_metrics
    
    try:
        record = message.value
        _validation_metrics["total_processed"] += 1
        
        # Validate
        validation_result = validate_record(record)
        
        # Update metrics
        sensor_id = record.get("sensor_id", "unknown")
        location = record.get("location", "unknown")
        device_type = record.get("device_type", "unknown")
        
        if validation_result["is_valid"]:
            # Route to validated topic
            send_to_topic(producer, output_topic, record, sensor_id)
            
            _validation_metrics["valid_count"] += 1
            _validation_metrics["by_sensor"][sensor_id]["valid"] += 1
            _validation_metrics["by_location"][location]["valid"] += 1
            _validation_metrics["by_device_type"][device_type]["valid"] += 1
            
            record["data_quality_flag"] = "valid"
            logger.debug(f"Valid record: sensor_id={sensor_id}")
            
        else:
            # Route to DLQ
            dlq_record = record.copy()
            dlq_record["validation_failures"] = validation_result["failure_reasons"]
            dlq_record["data_quality_flag"] = "invalid"
            
            send_to_topic(producer, dlq_topic, dlq_record, sensor_id)
            
            _validation_metrics["invalid_count"] += 1
            _validation_metrics["by_sensor"][sensor_id]["invalid"] += 1
            _validation_metrics["by_location"][location]["invalid"] += 1
            _validation_metrics["by_device_type"][device_type]["invalid"] += 1
            
            # Track failure types
            for reason in validation_result["failure_reasons"]:
                failure_type = reason.split(":")[0] if ":" in reason else reason
                _validation_metrics["failure_types"][failure_type] += 1
            
            logger.warning(
                f"Invalid record sent to DLQ: sensor_id={sensor_id}, "
                f"failures={len(validation_result['failure_reasons'])}"
            )
        
        # Update last processed timestamp
        _validation_metrics["last_processed_timestamp"] = datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        return False


def get_validation_metrics() -> Dict[str, Any]:
    """
    Get validation metrics.
    
    Returns:
        Dictionary with validation metrics
    """
    global _validation_metrics
    return _validation_metrics.copy()


def print_validation_metrics():
    """Print quality metrics to logger."""
    global _validation_metrics
    
    total = _validation_metrics["total_processed"]
    if total == 0:
        return
    
    valid_pct = (_validation_metrics["valid_count"] / total) * 100
    invalid_pct = (_validation_metrics["invalid_count"] / total) * 100
    
    logger.info("=" * 60)
    logger.info("Data Quality Metrics")
    logger.info("=" * 60)
    logger.info(f"Total processed: {total}")
    logger.info(f"Valid: {_validation_metrics['valid_count']} ({valid_pct:.1f}%)")
    logger.info(f"Invalid: {_validation_metrics['invalid_count']} ({invalid_pct:.1f}%)")
    logger.info(f"Last processed: {_validation_metrics['last_processed_timestamp']}")
    
    logger.info("\nFailure Types:")
    for failure_type, count in sorted(
        _validation_metrics["failure_types"].items(),
        key=lambda x: x[1],
        reverse=True
    )[:10]:
        logger.info(f"  {failure_type}: {count}")
    
    logger.info("=" * 60)


# ============================================================================
# Main Function
# ============================================================================

def main():
    """Main function to run validation consumer."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", "raw_iot_data")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "validated_iot_data")
    dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "dlq_iot_data")
    
    # Create Kafka consumer and producer
    consumer = create_kafka_consumer(bootstrap_servers, input_topic)
    producer = create_kafka_producer(bootstrap_servers)
    
    logger.info("Validation consumer initialized")
    logger.info(f"Input topic: {input_topic}")
    logger.info(f"Output topic: {output_topic}")
    logger.info(f"DLQ topic: {dlq_topic}")
    logger.info("Starting validation consumer...")
    logger.info("Press Ctrl+C to stop")
    
    try:
        for message in consumer:
            process_message(message, producer, output_topic, dlq_topic)
            
            # Print metrics every 100 messages
            if _validation_metrics["total_processed"] % 100 == 0:
                print_validation_metrics()
                
    except KeyboardInterrupt:
        logger.info("\nStopping validation consumer...")
    finally:
        consumer.close()
        producer.close()
        print_validation_metrics()
        logger.info("Validation consumer closed")


if __name__ == "__main__":
    main()
