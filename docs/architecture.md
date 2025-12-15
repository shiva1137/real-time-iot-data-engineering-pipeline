# IoT Data Engineering Project - Architecture Documentation

## 📋 Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Folder Structure Explanation](#folder-structure-explanation)
3. [Data Flow](#data-flow)
4. [Technology Choices & Design Decisions](#technology-choices--design-decisions)
5. [Failure Handling & Resilience](#failure-handling--resilience)
6. [Scaling Strategy](#scaling-strategy)
7. [Interview Q&A](#interview-qa)

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│   IoT Sensor Data (Faker Generator)                         │
│   - Temperature, Humidity, Energy Consumption               │
│   - 100 devices, every 10 seconds, continuous               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │   Kafka Topic: raw_iot     │
        │   (Multi-partition)        │
        └────────────┬───────────────┘
                     │
        ┌────────────┴──────────────┐
        │                           │
        ▼                           ▼
┌──────────────────┐      ┌──────────────────┐
│ SPARK STREAMING  │      │ SPARK BATCH      │
│ (5-min windows)  │      │ (Daily job)      │
│ - Aggregations   │      │ - Features       │
│ - Late data      │      │ - Cleaning       │
│ - Deduplication  │      │ - Aggregations   │
└────────┬─────────┘      └────────┬─────────┘
         │                         │
         └────────────┬────────────┘
                      │
                      ▼
        ┌─────────────────────────┐
        │  Storage Layer          │
        │  - MongoDB (Write)      │
        │  - PostgreSQL (Read)    │
        │  - raw_iot              │
        │  - cleaned_iot          │
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  dbt Transformations    │
        │  - Staging              │
        │  - Intermediate         │
        │  - Marts (Analytics)    │
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  Analytics DB           │
        │  (PostgreSQL marts)     │
        │  (Read operations)      │
        └────────────┬────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
    ┌────────┐ ┌─────────┐ ┌──────────┐
    │ FastAPI│ │ Airflow │ │Monitoring│
    │ REST   │ │Orchestr.│ │  Logs    │
    │ API    │ │         │ │  Alerts  │
    └────────┘ └─────────┘ └──────────┘
```

---

## Folder Structure Explanation

### Why This Structure?

**Separation of Concerns**: Each module has a specific responsibility, making the codebase maintainable and scalable.

**Scalability**: Independent modules can be scaled, deployed, and tested separately.

**Team Collaboration**: Different teams can work on different modules without conflicts.

**Interview Readiness**: Demonstrates understanding of production-grade project organization.

### Detailed Breakdown

#### `data_generator/`
- **Purpose**: Generates realistic IoT sensor data using Faker library
- **Why Separate**: Can run independently, useful for testing and development
- **Key Files**:
  - `generator.py`: Main data generation logic
  - `requirements.txt`: Module-specific dependencies

#### `kafka/`
- **Purpose**: Kafka topic configuration and initialization scripts
- **Why Separate**: Infrastructure configuration separate from application code
- **Key Files**:
  - `topics_config.json`: Topic definitions (partitions, replication)
  - `init-topics.sh`: Script to create topics on startup

#### `spark_streaming/`
- **Purpose**: Real-time data processing with Spark Streaming
- **Why Separate**: Different processing paradigm (streaming vs batch)
- **Key Files**:
  - `streaming_job.py`: Main streaming job with window operations
  - `requirements.txt`: Spark and streaming dependencies

#### `spark_batch/`
- **Purpose**: Daily batch processing jobs
- **Why Separate**: Batch processing has different requirements (scheduling, resource allocation)
- **Key Files**:
  - `batch_job.py`: Daily aggregation and feature engineering
  - `requirements.txt`: Batch processing dependencies

#### `data_quality/`
- **Purpose**: Data validation and quality checks
- **Why Separate**: Reusable across streaming and batch jobs
- **Key Files**:
  - `validators.py`: Schema validation, range checks, anomaly detection

#### `dbt/`
- **Purpose**: SQL transformations using dbt (Data Build Tool)
- **Why Separate**: Follows dbt project structure conventions
- **Key Files**:
  - `models/staging/`: Raw data cleaning
  - `models/intermediate/`: Intermediate transformations
  - `models/marts/`: Final analytics tables
  - `dbt_project.yml`: dbt configuration

#### `api/`
- **Purpose**: FastAPI REST API for data access
- **Why Separate**: API layer separate from data processing
- **Key Files**:
  - `main.py`: FastAPI application
  - `models/`: Pydantic models for request/response
  - `routes/`: API endpoint handlers

#### `airflow/`
- **Purpose**: Workflow orchestration and scheduling
- **Why Separate**: Airflow requires specific directory structure
- **Key Files**:
  - `dags/`: Airflow DAG definitions
  - `plugins/`: Custom Airflow operators/plugins

#### `monitoring/`
- **Purpose**: Centralized logging and alerting
- **Why Separate**: Cross-cutting concern used by all modules
- **Key Files**:
  - `logging_config.py`: Centralized logging configuration
  - `alerts.py`: Alert handling logic

#### `docker/`
- **Purpose**: Docker configuration files
- **Why Separate**: Infrastructure as code, separate from application
- **Key Files**:
  - `docker-compose.yml`: Multi-container orchestration
  - `Dockerfile.*`: Individual service containerization

#### `docs/`
- **Purpose**: Project documentation
- **Why Separate**: Documentation separate from code

#### `scripts/`
- **Purpose**: Utility scripts for setup and maintenance
- **Why Separate**: Operational scripts separate from application code

#### `tests/`
- **Purpose**: Unit and integration tests
- **Why Separate**: Standard Python testing structure

---

## Data Flow

### End-to-End Data Flow

1. **Data Generation** (`data_generator/`)
   - Faker generates realistic IoT sensor data
   - 100 devices, every 10 seconds
   - Data: `{sensor_id, timestamp, temperature, humidity, energy_consumption}`

2. **Kafka Ingestion** (`kafka/`)
   - Producer publishes to `raw_iot` topic
   - 6 partitions for parallel processing
   - Data buffered in Kafka (7-day retention)

3. **Real-Time Processing** (`spark_streaming/`)
   - Spark Streaming consumes from Kafka
   - 5-minute tumbling windows
   - Aggregations: avg temperature, max humidity, total energy
   - Handles late-arriving data (watermarking)
   - Writes to MongoDB (write-optimized)

4. **Batch Processing** (`spark_batch/`)
   - Daily job reads from PostgreSQL (synced from MongoDB)
   - Data cleaning: nulls, duplicates
   - Feature engineering: 7-day rolling averages, anomaly flags
   - Writes cleaned data back to PostgreSQL

5. **Data Transformation** (`dbt/`)
   - Staging: Clean raw data
   - Intermediate: Business logic transformations
   - Marts: Final analytics tables for reporting

6. **API Layer** (`api/`)
   - FastAPI serves data from PostgreSQL marts
   - REST endpoints for queries
   - Real-time and historical data access

7. **Orchestration** (`airflow/`)
   - Schedules batch jobs
   - Monitors pipeline health
   - Handles dependencies between jobs

8. **Monitoring** (`monitoring/`)
   - Centralized logging
   - Alerts on failures
   - Performance metrics

---

## Technology Choices & Design Decisions

### 1. Why Kafka over RabbitMQ/Redis?

**Answer for Interview:**

"Kafka was chosen for several reasons:

1. **High Throughput**: Kafka can handle millions of messages per second, which is critical for our 864K readings/day requirement. RabbitMQ and Redis are better for lower-volume scenarios.

2. **Durability**: Kafka persists messages to disk with configurable retention (we use 7 days). This ensures no data loss even if consumers are down temporarily.

3. **Partitioning**: Kafka's partitioning model allows parallel processing across multiple consumers, enabling horizontal scaling. RabbitMQ's queue model doesn't support this as elegantly.

4. **Ordering Guarantees**: Kafka maintains message order within partitions, which is important for time-series IoT data.

5. **Consumer Groups**: Kafka's consumer group model allows multiple consumers to process different partitions in parallel, providing both scalability and fault tolerance.

6. **Stream Processing**: Kafka integrates seamlessly with Spark Streaming for real-time processing, which is a core requirement.

Redis is primarily a cache/database, not a message broker. RabbitMQ is better for request-response patterns and lower volumes."

### 2. Why MongoDB + PostgreSQL instead of just one?

**Answer for Interview:**

"We use a dual-database architecture for optimal performance:

**MongoDB (Write Operations)**:
- **High Write Throughput**: MongoDB excels at high-volume writes. Our streaming pipeline writes continuously, and MongoDB's document model handles this efficiently.
- **Schema Flexibility**: IoT data schema may evolve, and MongoDB's flexible schema accommodates this.
- **Horizontal Scaling**: MongoDB sharding allows us to scale writes horizontally as data volume grows.
- **Time-Series Optimization**: MongoDB can be optimized for time-series data patterns.

**PostgreSQL (Read Operations)**:
- **SQL Queries**: PostgreSQL's SQL support is essential for complex analytical queries, joins, and aggregations required by dbt and the API.
- **ACID Compliance**: For analytics and reporting, we need strong consistency guarantees.
- **Mature Ecosystem**: PostgreSQL integrates seamlessly with dbt, BI tools, and has excellent performance for read-heavy workloads.
- **Cost Efficiency**: Separating reads and writes allows us to optimize each database independently.

**Data Sync**: We sync data from MongoDB to PostgreSQL periodically (via batch jobs) to maintain consistency while optimizing for each use case.

This is a common pattern in data engineering: write-optimized storage for ingestion, read-optimized storage for analytics."

### 3. Why KRaft instead of Zookeeper?

**Answer for Interview:**

"Kafka KRaft (Kafka Raft) is the modern replacement for Zookeeper:

1. **Simpler Architecture**: KRaft eliminates the external Zookeeper dependency, reducing operational complexity. One less service to manage, monitor, and maintain.

2. **Better Performance**: KRaft provides better performance for metadata operations, especially in large clusters with many topics and partitions.

3. **Scalability**: KRaft can handle millions of partitions, which is important as we scale the number of topics and partitions.

4. **Future-Proof**: Zookeeper is being deprecated in favor of KRaft. Apache Kafka 4.0+ will require KRaft mode.

5. **Reduced Latency**: KRaft reduces metadata propagation latency, improving overall system responsiveness.

6. **Operational Simplicity**: No need to manage Zookeeper clusters, which simplifies deployment and reduces failure points.

For a new project, KRaft is the recommended approach."

### 4. How do you handle data flow?

**Answer for Interview:**

"Data flows through the system in multiple stages:

1. **Ingestion**: Data generator → Kafka `raw_iot` topic (buffering layer)

2. **Real-Time Processing**: Kafka → Spark Streaming → MongoDB
   - Spark Streaming processes 5-minute windows
   - Aggregations performed in real-time
   - Results written to MongoDB (write-optimized)

3. **Batch Processing**: PostgreSQL (synced from MongoDB) → Spark Batch → PostgreSQL
   - Daily batch job reads from PostgreSQL
   - Performs data cleaning and feature engineering
   - Writes cleaned/transformed data back

4. **Transformation**: PostgreSQL → dbt → PostgreSQL marts
   - dbt models transform data through staging → intermediate → marts
   - Creates analytics-ready tables

5. **Serving**: PostgreSQL marts → FastAPI → End users
   - API queries PostgreSQL for read operations
   - Provides REST endpoints for data access

**Key Design Principles**:
- **Separation of Concerns**: Each stage has a specific purpose
- **Fault Tolerance**: Kafka buffers data, allowing recovery from failures
- **Scalability**: Each stage can scale independently
- **Data Quality**: Validation at multiple stages (Kafka, Spark, dbt)"

---

## Failure Handling & Resilience

### What if Kafka goes down?

**Answer for Interview:**

"Kafka failures are handled through multiple mechanisms:

1. **Producer Retries**: 
   - Producer configured with `retries=3` and `acks=all`
   - Idempotent producer ensures no duplicate messages
   - Producer buffers messages locally and retries on failure

2. **Data Buffering**:
   - Producer has a buffer that holds messages until Kafka is available
   - Prevents data loss during temporary outages

3. **Dead Letter Queue (DLQ)**:
   - Failed messages after retries are sent to a DLQ topic
   - Can be reprocessed later after investigation

4. **Monitoring & Alerts**:
   - Health checks monitor Kafka availability
   - Alerts trigger when Kafka is down
   - Operations team notified immediately

5. **Graceful Degradation**:
   - Data generator can pause or reduce rate if Kafka is unavailable
   - Prevents overwhelming the system when it recovers

6. **Recovery**:
   - Once Kafka is back, producers automatically reconnect
   - Consumers resume from last committed offset
   - No data loss due to Kafka's durability"

### What if MongoDB fails?

**Answer for Interview:**

"MongoDB failures are handled as follows:

1. **Write Failures Logged**:
   - Spark Streaming job logs all write failures
   - Failed writes are stored in a failure log for reprocessing

2. **Retry Mechanism**:
   - Exponential backoff retry for transient failures
   - Configurable retry count (default: 3 attempts)

3. **Data Persistence in Kafka**:
   - Since data is consumed from Kafka, it remains available
   - Can reprocess from Kafka once MongoDB recovers
   - Kafka retention (7 days) ensures data availability

4. **Alert Triggered**:
   - Monitoring system detects MongoDB connection failures
   - Immediate alert to operations team
   - Dashboard shows write failure rate

5. **Graceful Handling**:
   - Spark Streaming continues processing but queues failed writes
   - Batch jobs can skip MongoDB-dependent steps
   - API read operations unaffected (reads from PostgreSQL)

6. **Recovery**:
   - Once MongoDB is back, Spark Streaming resumes writes
   - Can replay failed writes from Kafka if needed
   - Data consistency maintained through idempotent writes"

### What if PostgreSQL fails?

**Answer for Interview:**

"PostgreSQL failures are handled differently based on the operation:

**For Read Operations (API, dbt)**:
1. **Read Failures**: API returns 503 Service Unavailable
   - Clients can retry with exponential backoff
   - Caching layer (if implemented) can serve stale data temporarily

2. **dbt Transformations**: Jobs fail gracefully
   - Airflow marks DAG run as failed
   - Can retry once PostgreSQL is available
   - No data loss (source data in MongoDB)

**For Write Operations (Batch Jobs)**:
1. **Write Failures Logged**: All failed writes logged
2. **Retry on Recovery**: Batch jobs can be rerun
3. **Data Availability**: Source data remains in MongoDB

**Key Points**:
- **Writes Continue**: MongoDB writes are unaffected, so data ingestion continues
- **No Data Loss**: All data persists in MongoDB and Kafka
- **Read Impact Only**: Only read operations (API, analytics) are affected
- **Recovery**: Once PostgreSQL is back, all operations resume normally
- **Monitoring**: Alerts trigger immediately on PostgreSQL failures"

---

## Scaling Strategy

### How would you scale this pipeline?

**Answer for Interview:**

"Scaling strategy depends on the bottleneck:

**1. Horizontal Scaling - Kafka**:
- **More Partitions**: Increase partitions in topics (currently 6, can go to 20+)
- **More Brokers**: Add Kafka brokers to the cluster
- **Consumer Parallelism**: More Spark Streaming executors = more parallel consumers
- **Result**: Linear scaling of throughput

**2. Horizontal Scaling - Spark**:
- **More Executors**: Increase Spark executors for streaming and batch jobs
- **Distributed Mode**: Move from `local[*]` to Spark cluster (YARN/Kubernetes)
- **Dynamic Allocation**: Auto-scale executors based on load
- **Result**: Parallel processing of more partitions

**3. Database Scaling**:
- **MongoDB Sharding**: Shard collections by sensor_id or timestamp
- **PostgreSQL Read Replicas**: Add read replicas for API queries
- **Connection Pooling**: Optimize connection pools for high concurrency
- **Result**: Handle more reads/writes

**4. API Scaling**:
- **Multiple API Instances**: Run multiple FastAPI instances behind load balancer
- **Caching**: Redis cache for frequently accessed data
- **Result**: Handle more concurrent API requests

**5. Batch Job Optimization**:
- **Partitioning**: Process data in parallel partitions
- **Incremental Processing**: Process only new data, not full table scans
- **Result**: Faster batch job execution

**6. Infrastructure Scaling**:
- **Kubernetes**: Deploy all services on Kubernetes for auto-scaling
- **Auto-scaling Policies**: Scale based on CPU, memory, or queue depth
- **Result**: Automatic scaling based on load

**Monitoring for Scaling**:
- Track metrics: throughput, latency, queue depth, resource utilization
- Set alerts when approaching capacity limits
- Plan scaling before hitting bottlenecks"

---

## Interview Q&A

### Common Interview Questions

#### Q1: "Walk me through your architecture"

**Answer:**
"I built an end-to-end IoT data pipeline processing 864K readings per day from 100 sensors.

**Data Flow**:
1. Faker generator creates realistic IoT data (temperature, humidity, energy)
2. Kafka ingests data into `raw_iot` topic with 6 partitions
3. Spark Streaming processes in 5-minute windows, writes to MongoDB
4. Daily Spark batch jobs clean data and engineer features, write to PostgreSQL
5. dbt transforms data through staging → intermediate → marts
6. FastAPI serves analytics data via REST endpoints
7. Airflow orchestrates batch jobs and monitors pipeline health

**Key Design Decisions**:
- Kafka for high-throughput ingestion with durability
- MongoDB for writes, PostgreSQL for reads (optimized for each use case)
- Spark Streaming for real-time, Spark Batch for daily processing
- dbt for SQL-based transformations
- Docker Compose for local development and reproducibility

**Why This Architecture**:
- Scalable: Each component can scale independently
- Fault-tolerant: Kafka buffers data, retries on failures
- Maintainable: Clear separation of concerns
- Production-ready: Follows industry best practices"

#### Q2: "Explain Kafka partitioning"

**Answer:**
"Kafka topics are divided into partitions for parallel processing:

**Partitioning Strategy**:
- We use 6 partitions for `raw_iot` topic
- Partition key: `sensor_id % 6` (ensures same sensor always goes to same partition)
- This maintains ordering per sensor while enabling parallelism

**Benefits**:
1. **Parallelism**: Multiple consumers can process different partitions simultaneously
2. **Scalability**: Can increase partitions to scale throughput
3. **Ordering**: Messages within a partition maintain order
4. **Fault Tolerance**: Each partition can be replicated

**Consumer Groups**:
- Spark Streaming uses a consumer group
- Each executor processes different partitions
- If an executor fails, its partitions are reassigned to other executors
- Ensures exactly-once processing with proper offset management"

#### Q3: "How do you ensure data quality?"

**Answer:**
"Data quality is enforced at multiple stages:

**1. Schema Validation (Kafka Producer)**:
- Pydantic models validate data structure before publishing
- Ensures required fields are present

**2. Range Validation (Data Quality Module)**:
- Temperature: -50°C to 150°C
- Humidity: 0% to 100%
- Energy: 0 to 10,000 units
- Out-of-range values flagged and sent to DLQ

**3. Spark Processing**:
- Null handling: Fill or drop based on business rules
- Deduplication: Remove duplicate records based on (sensor_id, timestamp)
- Anomaly detection: Statistical methods (Z-score) to detect outliers

**4. dbt Tests**:
- Schema tests: Ensure data types are correct
- Not null tests: Critical fields must not be null
- Unique tests: Prevent duplicates
- Custom tests: Business logic validation

**5. Monitoring**:
- Track data quality metrics: null rate, duplicate rate, anomaly rate
- Alerts when quality thresholds are breached
- Dashboard for data quality trends"

#### Q4: "How do you handle late-arriving data?"

**Answer:**
"Late data is handled through Spark Streaming's watermarking:

**Watermarking Strategy**:
- Watermark = 10 minutes (allows data up to 10 minutes late)
- Window = 5-minute tumbling windows
- Late data within watermark is included in appropriate window
- Data beyond watermark is dropped (or sent to separate stream)

**Implementation**:
```python
# Pseudo-code
df.withWatermark("timestamp", "10 minutes") \
  .groupBy(window("timestamp", "5 minutes"), "sensor_id") \
  .agg(...)
```

**Why This Works**:
- IoT sensors may have network delays
- 10-minute watermark accommodates typical delays
- Data beyond 10 minutes is likely erroneous or from failed sensors
- Can be processed separately if needed

**Alternative Approaches**:
- Separate late data stream for analysis
- Reprocess windows when late data arrives (if critical)
- Alert on high late data rate (indicates sensor issues)"

#### Q5: "What's the difference between Spark Streaming and Batch?"

**Answer:**
"In our pipeline, we use both:

**Spark Streaming**:
- **Purpose**: Real-time aggregations (5-minute windows)
- **Input**: Kafka (streaming)
- **Output**: MongoDB (continuous writes)
- **Use Case**: Real-time dashboards, alerts, monitoring
- **Latency**: Sub-minute
- **Processing**: Micro-batches (every 60 seconds)

**Spark Batch**:
- **Purpose**: Daily data cleaning, feature engineering, historical aggregations
- **Input**: PostgreSQL (batch read)
- **Output**: PostgreSQL (batch write)
- **Use Case**: Analytics, reporting, ML features
- **Latency**: Hours (daily job)
- **Processing**: Full dataset processing

**Why Both?**:
- Streaming for real-time needs (low latency)
- Batch for comprehensive processing (data quality, complex features)
- Batch can correct errors found in streaming data
- Batch handles historical reprocessing"

---

## Summary

This architecture demonstrates:

✅ **Production-Grade Design**: Scalable, fault-tolerant, maintainable
✅ **Technology Understanding**: Justified choices for each component
✅ **Best Practices**: Separation of concerns, proper error handling
✅ **Interview Readiness**: Answers to common data engineering questions

**Key Takeaways**:
- Architecture supports 864K readings/day with room to scale
- Each component has a clear purpose and can scale independently
- Failure handling ensures data integrity and system resilience
- Design decisions are justified and interview-ready

---

**Last Updated**: Topic 1 - Project Setup & Architecture

