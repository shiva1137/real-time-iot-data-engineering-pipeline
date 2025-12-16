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
   cd ../kafka
   bash init-topics.sh
   ```

6. **Run data generator** (Topic 2)
   ```bash
   cd ../data_generator
   pip install -r requirements.txt
   python generator.py
   ```

## 📁 Project Structure

```
├── data_generator/          # Topic 2: Faker data generator
│   ├── __init__.py
│   ├── generator.py
│   └── requirements.txt
│
├── kafka/                   # Topic 2: Kafka configs
│   ├── topics_config.json
│   └── init-topics.sh
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
├── data_quality/            # Topic 5: Validation logic
│   ├── __init__.py
│   ├── validators.py
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
│   └── architecture.md
│
├── scripts/                 # Utility scripts
│   └── setup.sh
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

### 🚧 In Progress / Upcoming Topics

2. **Data Ingestion with Kafka** - Upcoming
   - Producer implementation, topics, partitioning
   - Faker data generator for realistic IoT data
   - Error handling and retries

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

**Overall Progress: 1/12 Topics (8%)**

- ✅ Topic 1: Project Setup & Architecture
- ⏳ Topic 2-12: In Development

## 🎯 Interview Preparation

This project is designed to answer common data engineering interview questions:

- **Architecture**: "Walk me through your data pipeline architecture"
- **Technology Choices**: "Why Kafka over RabbitMQ/Redis?"
- **Data Flow**: "How does data flow through your system?"
- **Scalability**: "How would you scale this pipeline?"
- **Failure Handling**: "What happens if Kafka/MongoDB/PostgreSQL fails?"

See [docs/architecture.md](docs/architecture.md) for detailed explanations.

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

**Current Status**: ✅ Topic 1 Complete | 🚧 Topics 2-12 In Development

**Last Updated**: December 2025

**Repository**: [GitHub - Real-Time IoT Data Engineering Pipeline](https://github.com/shiva1137/real-time-iot-data-engineering-pipeline)

---

## 📈 Learning Journey

This project is part of a structured learning path to master data engineering concepts through hands-on implementation. Each topic builds upon the previous one, creating a complete, production-ready pipeline.

**Key Achievements So Far:**
- ✅ Production-grade project structure
- ✅ Docker infrastructure setup
- ✅ Comprehensive architecture documentation
- ✅ Interview-ready explanations and Q&A

**Next Milestones:**
- 🎯 Topic 2: Implement Kafka producer and data generator
- 🎯 Topic 3: Build Spark Streaming pipeline
- 🎯 Topic 4: Create batch processing jobs

