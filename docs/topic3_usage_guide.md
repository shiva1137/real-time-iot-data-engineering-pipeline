# Topic 3: Real-Time Processing with Spark Streaming - Usage Guide

## Overview

This guide explains how to run the Spark Streaming job that processes real-time IoT data from Kafka, performs 5-minute window aggregations, and writes results to MongoDB (synced to PostgreSQL).

## Architecture

```
Kafka (validated_iot_data) → Spark Streaming → MongoDB (real_time_aggregates) → PostgreSQL Sync → PostgreSQL
```

## Prerequisites

1. Docker and Docker Compose installed
2. Python 3.11+
3. Apache Spark 3.5+ installed (or use Docker)
4. Kafka running (via docker-compose)
5. MongoDB running (via docker-compose)
6. PostgreSQL running (via docker-compose)

## Setup

### 1. Install Dependencies

```bash
cd spark_streaming
pip install -r requirements.txt
```

### 2. Set Environment Variables

Create `.env` file or set environment variables:

```bash
# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_INPUT_TOPIC=validated_iot_data

# MongoDB configuration
export MONGO_URI=mongodb://admin:password@localhost:27017/
export MONGO_DATABASE=iot_data
export MONGO_COLLECTION=real_time_aggregates

# PostgreSQL configuration
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=iot_analytics
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres

# Spark checkpoint location
export CHECKPOINT_LOCATION=/tmp/spark_streaming_checkpoint
```

## Running the System

### Option 1: Run Spark Streaming Job

**Terminal 1 - Start Kafka, MongoDB, PostgreSQL:**
```bash
cd docker
docker-compose up -d kafka mongodb postgresql
```

**Terminal 2 - Run Data Generator + Producer:**
```bash
cd data_generator
python producer.py
```

**Terminal 3 - Run Validation Consumer:**
```bash
cd data_quality
python validation_consumer.py
```

**Terminal 4 - Run Spark Streaming Job:**
```bash
cd spark_streaming
spark-submit streaming_job.py
```

**Terminal 5 - Run MongoDB to PostgreSQL Sync (every 5 minutes):**
```bash
cd spark_streaming
python mongo_to_postgres_sync.py
```

Or schedule with cron:
```bash
# Run every 5 minutes
*/5 * * * * cd /path/to/spark_streaming && python mongo_to_postgres_sync.py
```

## Configuration

### Spark Streaming Settings

- **Micro-batch interval**: 10 seconds
- **Window duration**: 5 minutes (tumbling windows)
- **Watermark**: 1 minute (handles late data)
- **Output mode**: UPDATE (shows latest window results)
- **Checkpoint location**: `/tmp/spark_streaming_checkpoint`

### Window Aggregations

Per sensor per 5-minute window:
- `avg_temperature` - Average temperature
- `max_temperature` - Maximum temperature
- `min_temperature` - Minimum temperature
- `avg_humidity` - Average humidity
- `total_energy_consumption` - Sum of energy consumption
- `count` - Number of readings in window

## How It Works

### 1. Spark Streaming Job

**Flow:**
1. Reads from Kafka `validated_iot_data` topic
2. Parses JSON messages into structured DataFrame
3. Groups by `sensor_id` and 5-minute tumbling windows
4. Calculates aggregations (avg, max, min, sum, count)
5. Applies watermark (1 minute) to handle late data
6. Writes to MongoDB using `foreachBatch`

**Watermarking:**
- Data within 1 minute of watermark: Updates window result
- Data beyond 1 minute: Dropped (too late)
- Prevents infinite state growth

**Output Mode: UPDATE**
- Shows latest window results (not historical logs)
- Perfect for dashboards showing current state
- Overwrites previous window results

### 2. MongoDB to PostgreSQL Sync

**Flow:**
1. Reads aggregated data from MongoDB
2. Creates/updates PostgreSQL table
3. UPSERTs records (INSERT ... ON CONFLICT UPDATE)
4. Last write wins (handles conflicts)
5. Creates indexes for fast queries

**Why Sync?**
- MongoDB: Optimized for high write throughput
- PostgreSQL: Optimized for SQL queries, dbt compatibility
- Separation of concerns: Write to MongoDB, read from PostgreSQL

## Monitoring

### Check Spark Streaming Status

```bash
# View Spark UI (if enabled)
# Default: http://localhost:4040

# Check checkpoint location
ls -la /tmp/spark_streaming_checkpoint
```

### Check MongoDB Data

```bash
# Connect to MongoDB
docker exec -it iot_mongodb mongosh -u admin -p password

# Use database
use iot_data

# Query aggregations
db.real_time_aggregates.find().sort({window_start: -1}).limit(10)

# Count records
db.real_time_aggregates.count()
```

### Check PostgreSQL Data

```bash
# Connect to PostgreSQL
docker exec -it iot_postgresql psql -U postgres -d iot_analytics

# Query aggregations
SELECT * FROM real_time_aggregates 
ORDER BY window_start DESC 
LIMIT 10;

# Count records
SELECT COUNT(*) FROM real_time_aggregates;
```

## Expected Output

### Spark Streaming Logs

```
INFO - Starting IoT Spark Streaming Job
INFO - Kafka topic: validated_iot_data
INFO - MongoDB collection: iot_data.real_time_aggregates
INFO - Window duration: 5 minutes
INFO - Watermark: 1 minute
INFO - Streaming query started
INFO - Batch 0: Wrote 15 records to MongoDB
INFO - Batch 1: Wrote 20 records to MongoDB
```

### MongoDB Document Example

```json
{
  "_id": ObjectId("..."),
  "sensor_id": "SENSOR_MUM_001",
  "window_start": ISODate("2024-01-15T10:00:00Z"),
  "window_end": ISODate("2024-01-15T10:05:00Z"),
  "avg_temperature": 28.5,
  "max_temperature": 32.1,
  "min_temperature": 25.2,
  "avg_humidity": 65.3,
  "total_energy_consumption": 12.5,
  "count": 30,
  "location": "Mumbai",
  "state": "Maharashtra",
  "device_type": "Temperature Sensor",
  "processed_at": ISODate("2024-01-15T10:05:10Z")
}
```

## Troubleshooting

### Spark Streaming Not Starting

**Error**: `KafkaException: Failed to construct kafka consumer`

**Solution**:
1. Check Kafka is running: `docker ps | grep kafka`
2. Check Kafka topic exists: `docker exec -it iot_kafka kafka-topics.sh --list --bootstrap-server localhost:9092`
3. Verify `validated_iot_data` topic exists

### No Data in MongoDB

**Possible Causes**:
1. Validation consumer not running (no data in `validated_iot_data`)
2. Spark Streaming job not running
3. Checkpoint location issues

**Solution**:
1. Verify validation consumer is running
2. Check Spark Streaming logs
3. Verify checkpoint location is writable

### Late Data Being Dropped

**Expected**: Data arriving > 1 minute late is dropped (by design)

**If too strict**: Increase watermark in `streaming_job.py`:
```python
WATERMARK_DELAY = "2 minutes"  # Increase from 1 minute
```

**Trade-off**: Larger watermark = more state memory usage

### Sync Job Failing

**Error**: `Failed to connect to PostgreSQL`

**Solution**:
1. Check PostgreSQL is running: `docker ps | grep postgresql`
2. Verify connection credentials in `.env`
3. Check network connectivity

## Performance Tuning

### Spark Streaming

**Increase parallelism:**
```python
.config("spark.sql.shuffle.partitions", "400")  # Increase from 200
```

**Adjust micro-batch interval:**
```python
MICRO_BATCH_INTERVAL = "5 seconds"  # Faster processing (more overhead)
```

**Optimize state store:**
- Use RocksDB (already configured)
- Increase state store memory if needed

### MongoDB

**Indexes:**
- Already created: `(sensor_id, window_start)` unique index
- Already created: `window_start` descending index

**Write performance:**
- MongoDB handles high write throughput well
- Consider sharding for very large scale

### PostgreSQL

**Indexes:**
- Already created: `window_start`, `sensor_id`, `location`
- Add more indexes based on query patterns

**Connection pooling:**
- Use connection pooler (PgBouncer) for production

## Next Steps

After Topic 3, proceed to:
- **Topic 4**: Batch Processing with PySpark (daily aggregations)
- **Topic 7**: FastAPI REST API (query aggregated data)
- **Topic 8**: Airflow Orchestration (schedule sync job)

## Interview Preparation

See `docs/topic3_comprehensive_guide.md` for:
- Comprehensive interview Q&A
- Code explanations (windowing, watermarking, state management)
- Design decisions
- What-if scenarios
- Performance optimization tips

---

**Last Updated**: Topic 3 - Real-Time Processing with Spark Streaming



