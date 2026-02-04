"""
Kafka Validation Consumer for IoT Data - Function-Based Implementation

Reads from raw_iot_data, validates via data_quality.validators, routes valid
data to validated_iot_data and invalid to dlq_iot_data. Tracks quality metrics.

Usage:
    python validation_consumer.py
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

try:
    from data_quality.validators import validate_record
except ImportError:
    from validators import validate_record

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Module-level metrics
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
