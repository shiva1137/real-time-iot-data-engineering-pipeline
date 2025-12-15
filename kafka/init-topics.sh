#!/bin/bash

# Initialize Kafka Topics
# This script creates all required Kafka topics for the IoT data pipeline

KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/topics_config.json"

echo "=========================================="
echo "Initializing Kafka Topics"
echo "Bootstrap Server: ${KAFKA_BOOTSTRAP_SERVERS}"
echo "=========================================="

# Check if kafka-topics.sh is available
if ! command -v kafka-topics.sh &> /dev/null; then
    echo "Error: kafka-topics.sh not found. Make sure Kafka is installed or use Docker exec."
    echo "For Docker: docker exec -it iot_kafka kafka-topics.sh --bootstrap-server localhost:9092 --list"
    exit 1
fi

# Create topics from JSON config
# Note: This is a simplified version. In production, use a proper JSON parser
# or Python script to read topics_config.json

echo "Creating topic: raw_iot"
kafka-topics.sh --create \
    --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
    --topic raw_iot \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --if-not-exists

echo "Creating topic: cleaned_iot"
kafka-topics.sh --create \
    --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
    --topic cleaned_iot \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --if-not-exists

echo "Creating topic: aggregated_iot"
kafka-topics.sh --create \
    --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS} \
    --topic aggregated_iot \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=2592000000 \
    --if-not-exists

echo "=========================================="
echo "Listing all topics:"
kafka-topics.sh --list --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS}
echo "=========================================="

echo "Done! Topics initialized successfully."

