# IoT Data Pipeline – Architecture on AWS

This document maps the project to **AWS services** for **2026 AWS Data Engineer** roles. The same codebase runs locally (Docker) or on AWS with environment-driven configuration.

---

## High-Level AWS Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│   IoT Sensor Data (Faker / Producer)                                        │
│   → Local: Kafka | AWS: Amazon MSK or Kinesis Data Streams                   │
└────────────────────┬────────────────────────────────────────────────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  Amazon MSK (Kafka)          │  or  Kinesis Data Streams
        │  Topics: raw_iot, validated  │
        └────────────┬────────────────┘
                     │
        ┌────────────┴───────────────┐
        │                            │
        ▼                            ▼
┌──────────────────┐       ┌──────────────────┐
│ AWS Glue / EMR    │       │ AWS Glue / EMR    │
│ Spark Streaming   │       │ Spark Batch       │
│ → S3 + DocumentDB │       │ → S3 + RDS        │
└────────┬──────────┘       └────────┬──────────┘
         │                          │
         └────────────┬─────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  Amazon S3 (Data Lake)       │
        │  raw / processed / checkpoint│
        │  Amazon RDS (PostgreSQL)     │
        │  Amazon DocumentDB (optional)│
        └────────────┬────────────────┘
                     │
                     ▼
        ┌─────────────────────────────┐
        │  dbt (EC2/Glue/CI)           │
        │  → Redshift or RDS           │
        └────────────┬────────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
   FastAPI      Amazon MWAA    CloudWatch
   (ECS/EC2)    (Airflow)      Logs / Alerts
```

---

## Service Mapping (Local → AWS)

| Component        | Local (Docker)     | AWS Service                          |
|-----------------|-------------------|--------------------------------------|
| Kafka           | Confluent Kafka   | **Amazon MSK** (Managed Kafka)       |
| Spark Streaming | Spark on host     | **AWS Glue** (Spark) or **EMR**      |
| Spark Batch     | Spark on host     | **AWS Glue** batch job or **EMR**    |
| MongoDB         | Mongo container   | **Amazon DocumentDB** (MongoDB compat) |
| PostgreSQL      | Postgres container| **Amazon RDS for PostgreSQL**       |
| Checkpoint/path | Local `/tmp`      | **Amazon S3**                        |
| Airflow         | Docker/standalone | **Amazon MWAA**                      |
| API             | Docker/local      | **ECS**, **EC2**, or **Lambda**      |
| CI/CD           | GitHub Actions    | **ECR**, **Glue**, **MWAA** DAG sync |

---

## Data Flow on AWS

1. **Ingestion**: Producer → **MSK** `raw_iot_data` (or Kinesis). Optional: also land raw events in **S3** for data lake.
2. **Validation**: Validation consumer (EC2/ECS/Lambda) reads from MSK, writes to `validated_iot_data` and **DLQ**; invalid records can go to S3.
3. **Streaming**: **Glue/EMR** Spark job consumes from MSK `validated_iot_data`, writes aggregations to **DocumentDB** (or RDS) and/or **S3**; checkpoint on **S3**.
4. **Batch**: **Glue** or **EMR** batch job reads from RDS (or S3), cleans and aggregates, writes to **RDS** and **S3** `processed/`.
5. **Transform**: **dbt** runs on EC2/Glue/CI against **Redshift** or **RDS**, builds marts.
6. **Orchestration**: **MWAA** runs DAGs for validation, batch, and dbt; uses AWS connections (RDS, S3, Glue).
7. **Serving**: **FastAPI** on ECS/EC2 reads from **RDS**.

---

## Resume-Ready AWS Keywords

- **Amazon MSK**, **Amazon Kinesis**
- **Amazon S3** (data lake, checkpoints)
- **AWS Glue** (ETL, Spark)
- **Amazon EMR** (Spark)
- **Amazon RDS for PostgreSQL**
- **Amazon DocumentDB**
- **Amazon MWAA** (Airflow)
- **Amazon Redshift** (optional for dbt)
- **AWS Lambda**, **ECS**, **CloudWatch**

---

## Cost Considerations (India – ap-south-1)

- **MSK**: Pay per broker hour and storage; start with 1 broker for demos.
- **S3**: Low cost for landing and processed data; use lifecycle rules for older tiers.
- **Glue**: Pay per DPU hour for jobs; minimal for small datasets.
- **RDS**: db.t3.micro for dev/demo.
- **MWAA**: Per environment and worker; small environment for learning.

Use **AWS Pricing Calculator** and **Cost Explorer** before production sizing.

---

**Last Updated**: February 2026
