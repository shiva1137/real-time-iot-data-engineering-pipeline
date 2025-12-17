# Real-Time IoT Data Engineering Pipeline: Streaming Data Processing & Analytics

A production-grade, interview-ready real-time data pipeline for IoT sensor data processing. This project demonstrates end-to-end data engineering practices from ingestion to serving analytics via REST API.

## 📊 Project Overview

This project processes **100 IoT sensors** generating data every **10 seconds**, resulting in approximately **864,000 readings per day**. The pipeline handles real-time streaming, batch processing, data quality validation, transformations, and API services.

### Key Metrics
- **Devices**: 100 IoT sensors
- **Data Frequency**: Every 10 seconds
- **Daily Volume**: ~864,000 readings/day
- **Data Types**: Temperature, Humidity, Energy Consumption

## 🏗️ Architecture

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

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Git

### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd "IOT Data Engineering Project"
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start infrastructure services**
   ```bash
   cd docker
   docker-compose up -d
   ```

4. **Verify services are running**
   ```bash
   docker-compose ps
   ```

   You should see:
   - MongoDB: `localhost:27017`
   - PostgreSQL: `localhost:5432`
   - Kafka: `localhost:9092`
   - Kafka UI: `localhost:8080` (optional)

5. **Initialize Kafka topics**
   ```bash
   # Install dependencies
   cd kafka
   pip install -r requirements.txt
   
   # Initialize topics
   python init_topics.py
   
   # Or verify topics exist:
   docker exec -it iot_kafka kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

6. **Run data generator and producer** (Topic 2)
   ```bash
   cd ../data_generator
   pip install -r requirements.txt
   python producer.py
   ```

7. **Run validation consumer** (in separate terminal)
   ```bash
   cd ../data_quality
   pip install -r requirements.txt
   python validation_consumer.py
   ```

See [docs/topic2_usage_guide.md](docs/topic2_usage_guide.md) for detailed usage instructions.

## 📁 Project Structure

```
├── data_generator/          # Topic 2: Faker data generator + Kafka producer
│   ├── __init__.py
│   ├── generator.py         # India-based data generator with quality issues
│   ├── producer.py          # Idempotent Kafka producer
│   └── requirements.txt
│
├── kafka/                   # Topic 2: Kafka configs
│   ├── topics_config.json
│   ├── init_topics.py       # Topic initialization script (Python)
│   └── requirements.txt
│
├── spark_streaming/         # Topic 3: Real-time processing
│   ├── __init__.py
│   ├── streaming_job.py
│   └── requirements.txt
│
├── spark_batch/             # Topic 4: Daily batch jobs
│   ├── __init__.py
│   ├── batch_job.py
│   └── requirements.txt
│
├── data_quality/            # Topic 2 & 5: Validation logic
│   ├── __init__.py
│   ├── validators.py       # Validation functions (Topic 5)
│   ├── validation_consumer.py  # Kafka validation consumer (Topic 2)
│   └── requirements.txt
│
├── dbt/                     # Topic 6: dbt project
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── api/                     # Topic 7: FastAPI
│   ├── __init__.py
│   ├── main.py
│   ├── models/
│   ├── routes/
│   └── requirements.txt
│
├── airflow/                 # Topic 8: DAGs
│   ├── dags/
│   ├── logs/
│   └── plugins/
│
├── monitoring/              # Topic 9: Logs, alerts
│   ├── logging_config.py
│   └── alerts.py
│
├── docker/                  # Topic 10: Dockerfiles
│   ├── Dockerfile.generator
│   ├── Dockerfile.spark
│   ├── Dockerfile.api
│   └── docker-compose.yml
│
├── .github/                 # Topic 11: CI/CD
│   └── workflows/
│       └── ci.yml
│
├── docs/                    # Documentation
│   ├── architecture.md      # System architecture
│   ├── topic1_comprehensive_guide.md  # Topic 1: Complete guide
│   ├── topic2_comprehensive_guide.md  # Topic 2: Complete guide
│   └── topic2_usage_guide.md          # Topic 2: Usage guide
│
├── scripts/                 # Utility scripts
│   └── setup.py             # Project setup script (Python, uses stdlib only)
│
├── tests/                   # Test files
│   └── __init__.py
│
├── .gitignore
├── .env.example
├── requirements.txt
└── README.md
```

## 🔧 Technology Stack

- **Message Queue**: Apache Kafka (KRaft mode)
- **Streaming**: Apache Spark Streaming
- **Batch Processing**: Apache Spark (PySpark)
- **Write Database**: MongoDB
- **Read Database**: PostgreSQL
- **Transformations**: dbt (Data Build Tool)
- **API**: FastAPI
- **Orchestration**: Apache Airflow
- **Containerization**: Docker & Docker Compose

## 📚 Topics Covered

### ✅ Completed Topics

1. **✅ Project Setup & Architecture** - Complete
   - Complete folder structure following best practices
   - Docker Compose setup with MongoDB, PostgreSQL, Kafka (KRaft)
   - Comprehensive documentation and architecture diagrams
   - Git repository initialization
   - CI/CD pipeline setup

2. **✅ Data Ingestion with Kafka** - Complete
   - Kafka broker setup in KRaft mode (no Zookeeper)
   - India-based Faker data generator (100 sensors across major cities)
   - Comprehensive data quality issues (nulls, duplicates, late data, out-of-range, type mismatches, schema violations, formatting)
   - Idempotent Kafka producer (MVP - clean, focused implementation)
     - Schema validation (lightweight checks)
     - Retry logic with exponential backoff (5 retries max)
     - Partition hashing for ordering per sensor
     - Thread-safe statistics tracking
   - Validation consumer with comprehensive quality checks
   - Dead-letter queue (DLQ) for invalid data
   - Quality metrics tracking
   - Interview preparation documentation

### 🚧 In Progress / Upcoming Topics

3. **Real-Time Processing** - Upcoming
   - Spark Streaming, windowing, aggregations
   - Late data handling with watermarking
   - State management

4. **Batch Processing** - Upcoming
   - Daily jobs, feature engineering
   - Data cleaning and transformations

5. **Data Quality** - Upcoming
   - Validation, schema enforcement
   - Dead-letter queue (DLQ) pattern

6. **dbt Transformations** - Upcoming
   - Staging, intermediate, marts
   - SQL-based transformations

7. **FastAPI** - Upcoming
   - REST API for data access
   - Pydantic validation

8. **Airflow Orchestration** - Upcoming
   - DAGs, scheduling
   - Workflow management

9. **Monitoring & Logging** - Upcoming
   - Alerts, observability
   - Structured logging

10. **Docker** - Upcoming
    - Containerization
    - Multi-stage builds

11. **CI/CD** - Upcoming
    - GitHub Actions
    - Automated testing

12. **Production Deployment** - Upcoming
    - Best practices
    - Performance optimization

## 📊 Project Progress

**Overall Progress: 2/12 Topics (17%)**

- ✅ Topic 1: Project Setup & Architecture
- ✅ Topic 2: Data Ingestion with Kafka
- ⏳ Topic 3-12: In Development

## 🎯 Interview Preparation

This project is designed to answer common data engineering interview questions:

- **Architecture**: "Walk me through your data pipeline architecture"
- **Technology Choices**: "Why Kafka over RabbitMQ/Redis?"
- **Data Flow**: "How does data flow through your system?"
- **Scalability**: "How would you scale this pipeline?"
- **Failure Handling**: "What happens if Kafka/MongoDB/PostgreSQL fails?"

**Comprehensive Documentation:**
- [Topic 1 Comprehensive Guide](docs/topic1_comprehensive_guide.md) - Project setup & architecture
- [Topic 2 Comprehensive Guide](docs/topic2_comprehensive_guide.md) - Data ingestion with Kafka
- [Topic 2 Usage Guide](docs/topic2_usage_guide.md) - Practical setup & usage
- [Architecture Documentation](docs/architecture.md) - System design & decisions

## 🤝 Contributing

This is a learning project. Feel free to fork and experiment!

## 📝 License

This project is for educational purposes.

## 🔗 Useful Links

- Kafka UI: http://localhost:8080
- FastAPI Docs: http://localhost:8000/docs (when running)
- Airflow UI: http://localhost:8080/airflow (when running)

---

## 🎯 Project Status

**Current Status**: ✅ Topics 1-2 Complete | 🚧 Topics 3-12 In Development

**Last Updated**: January 2025

**Repository**: [GitHub - Real-Time IoT Data Engineering Pipeline](https://github.com/shiva1137/real-time-iot-data-engineering-pipeline)

---

## 📈 Learning Journey

This project is part of a structured learning path to master data engineering concepts through hands-on implementation. Each topic builds upon the previous one, creating a complete, production-ready pipeline.

**Key Achievements So Far:**
- ✅ Production-grade project structure
- ✅ Docker infrastructure setup
- ✅ Comprehensive architecture documentation
- ✅ Kafka broker in KRaft mode
- ✅ India-based data generator with comprehensive quality issues
- ✅ Idempotent Kafka producer with error handling
- ✅ Validation consumer with DLQ pattern
- ✅ Interview-ready explanations and Q&A

**Next Milestones:**
- 🎯 Topic 3: Build Spark Streaming pipeline (consumes from validated_iot_data)
- 🎯 Topic 4: Create batch processing jobs
- 🎯 Topic 5: Enhanced data quality validation

