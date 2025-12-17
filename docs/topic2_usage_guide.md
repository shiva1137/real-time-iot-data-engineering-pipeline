# Topic 2: Data Ingestion with Kafka - Usage Guide

## Overview

This guide explains how to run the Kafka-based data ingestion system with India-specific IoT data generation and comprehensive validation.

## Architecture

```
Data Generator → Kafka (raw_iot_data) → Validation Consumer → Kafka (validated_iot_data / dlq_iot_data)
```

## Prerequisites

1. Docker and Docker Compose installed
2. Python 3.11+
3. Kafka running (via docker-compose)

## Setup

### 1. Start Kafka (KRaft Mode)

```bash
cd docker
docker-compose up -d kafka
```

Wait for Kafka to be ready (check logs):
```bash
docker logs iot_kafka
```

### 2. Initialize Kafka Topics

**Option 1: Using Python script (Recommended)**
```bash
cd kafka
pip install -r requirements.txt
python init_topics.py
```

**Option 2: Verify topics exist**
```bash
docker exec -it iot_kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

Expected topics:
- `raw_iot_data` (3 partitions)
- `validated_iot_data` (3 partitions)
- `dlq_iot_data` (1 partition)

### 3. Install Dependencies

```bash
# Data generator dependencies
cd data_generator
pip install -r requirements.txt

# Validation consumer dependencies
cd ../data_quality
pip install -r requirements.txt
```

## Running the System

### Option 1: Run Producer and Consumer Separately

**Terminal 1 - Data Generator + Producer:**
```bash
cd data_generator
python producer.py
```

**Terminal 2 - Validation Consumer:**
```bash
cd data_quality
python validation_consumer.py
```

### Option 2: Run Generator Only (for testing)

```bash
cd data_generator
python generator.py
```

This generates data but doesn't send to Kafka (for testing data generation).

## Configuration

Set environment variables (or use `.env` file):

```bash
# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=raw_iot_data
export KAFKA_INPUT_TOPIC=raw_iot_data
export KAFKA_OUTPUT_TOPIC=validated_iot_data
export KAFKA_DLQ_TOPIC=dlq_iot_data
```

## Monitoring

### Check Kafka Topics

```bash
# List topics
docker exec -it iot_kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
docker exec -it iot_kafka kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic raw_iot_data

# Consume messages (for debugging)
docker exec -it iot_kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic raw_iot_data \
  --from-beginning
```

### Check Data Quality Metrics

The validation consumer prints metrics every 100 messages:
- Total processed
- Valid vs invalid count
- Failure types breakdown
- Metrics by sensor, location, device type

### Check DLQ

```bash
docker exec -it iot_kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dlq_iot_data \
  --from-beginning \
  --property print.key=true
```

## Data Quality Issues Introduced

The generator intentionally introduces:

1. **Null Values (15%)**: Missing values in various fields
2. **Duplicates (8%)**: Exact, near, and late duplicates
3. **Late Data (10%)**: Timestamps 1-60 minutes in the past
4. **Out-of-Range (12%)**: Values outside valid ranges
5. **Type Mismatches (5%)**: Wrong data types
6. **Schema Violations (4%)**: Missing/extra fields
7. **Formatting Issues (6%)**: Extra spaces, wrong case, encoding issues

## Validation Rules

The validation consumer checks:

1. **Schema**: All required fields present, no unexpected fields
2. **Types**: Correct data types (float, int, string, datetime)
3. **Ranges**: Values within valid ranges (temperature -50 to 50°C, etc.)
4. **Format**: sensor_id pattern, timestamp format
5. **Freshness**: Timestamp < 5 minutes old, not in future
6. **Duplicates**: Same sensor_id + timestamp within 5-second window
7. **Completeness**: Critical fields not null

## Expected Output

### Producer Output
```
INFO - Starting IoT Data Producer...
INFO - Kafka servers: localhost:9092
INFO - Topic: raw_iot_data
INFO - Generating data for 100 sensors
INFO - Sending data every 10 seconds per sensor
```

### Consumer Output
```
INFO - Validation consumer initialized
INFO - Input topic: raw_iot_data
INFO - Output topic: validated_iot_data
INFO - DLQ topic: dlq_iot_data
INFO - Valid record: sensor_id=SENSOR_MUM_001
WARNING - Invalid record sent to DLQ: sensor_id=SENSOR_DEL_002, failures=2
```

### Quality Metrics (every 100 messages)
```
============================================================
Data Quality Metrics
============================================================
Total processed: 100
Valid: 65 (65.0%)
Invalid: 35 (35.0%)
Last processed: 2024-01-15T10:30:00.123456

Failure Types:
  Out of range: 12
  Null values: 8
  Duplicates: 5
  ...
============================================================
```

## Troubleshooting

### Kafka Not Available

**Error**: `kafka.errors.KafkaError: Unable to bootstrap from [('localhost', 9092)]`

**Solution**: 
1. Check Kafka is running: `docker ps | grep kafka`
2. Check Kafka logs: `docker logs iot_kafka`
3. Wait for Kafka to be ready (may take 30-60 seconds)

### Topics Not Created

**Error**: `Topic 'raw_iot_data' does not exist`

**Solution**:
1. Run the Python initialization script:
```bash
cd kafka
pip install -r requirements.txt
python init_topics.py
```

2. Or manually create topics using Kafka CLI:
```bash
docker exec -it iot_kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic raw_iot_data \
  --partitions 3 \
  --replication-factor 1
```

### High Invalid Rate

**Expected**: ~35-40% invalid rate (due to intentional data quality issues)

**If > 50%**: Check validation logic, may need to adjust thresholds

### Producer Retries

**Normal**: Some retries are expected (5% network failure simulation)

**If excessive**: Check Kafka health, network connectivity

## Next Steps

After Topic 2, proceed to:
- **Topic 3**: Real-Time Processing with Spark Streaming (consumes from `validated_iot_data`)
- **Topic 5**: Data Quality & Validation (enhanced validation logic)

## Interview Preparation

See `docs/topic2_comprehensive_guide.md` for:
- Comprehensive interview Q&A
- Code explanations
- Design decisions
- What-if scenarios
- Data quality handling strategies

---

**Last Updated**: Topic 2 - Data Ingestion with Kafka

