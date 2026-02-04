# AWS Deployment Guide – IoT Data Pipeline

Step-by-step guide to run this pipeline on **AWS** for **AWS Data Engineer** roles (2026). The same code works locally (Docker) and on AWS via environment variables.

---

## Prerequisites

- AWS account (e.g. **ap-south-1** for India)
- **AWS CLI** configured (`aws configure`)
- **Python 3.9+** (for scripts and dbt)
- Familiarity with AWS Glue, RDS, S3, and optionally MWAA

---

## 1. S3 Bucket (Data Lake)

Create one bucket (or separate buckets for raw/processed/checkpoints):

```bash
export AWS_REGION=ap-south-1
export S3_BUCKET=your-iot-pipeline-bucket

aws s3 mb s3://$S3_BUCKET --region $AWS_REGION
aws s3api put-bucket-versioning --bucket $S3_BUCKET --versioning-configuration Status=Enabled
```

Suggested prefixes:

- `s3://$S3_BUCKET/iot/raw/` – raw event landing (optional)
- `s3://$S3_BUCKET/iot/processed/` – batch output
- `s3://$S3_BUCKET/iot/checkpoints/streaming/` – Spark streaming checkpoint

Set in `.env`:

```env
USE_AWS=1
AWS_REGION=ap-south-1
S3_BUCKET_RAW=s3://$S3_BUCKET/iot/raw/
S3_BUCKET_PROCESSED=s3://$S3_BUCKET/iot/processed/
S3_CHECKPOINT_LOCATION=s3://$S3_BUCKET/iot/checkpoints/streaming/
```

---

## 2. Amazon RDS (PostgreSQL)

1. Create RDS PostgreSQL instance (e.g. **PostgreSQL 15**), same VPC as Glue/MWAA if used.
2. Note **endpoint**, **port**, **master user**, **password**.
3. Create database: `iot_analytics`.
4. In `.env`:

```env
POSTGRES_HOST=your-instance.xxxxx.ap-south-1.rds.amazonaws.com
POSTGRES_PORT=5432
POSTGRES_DB=iot_analytics
POSTGRES_USER=admin
POSTGRES_PASSWORD=your-secure-password
```

Run your schema migrations or dbt against this RDS (same as local Postgres).

---

## 3. Amazon MSK (Kafka)

1. Create **Amazon MSK** cluster (e.g. 1 broker for demo), same VPC.
2. Create topics: `raw_iot_data`, `validated_iot_data`, `dlq_iot_data` (e.g. via MSK Connect or a one-off EC2 in VPC with Kafka tools).
3. Get **bootstrap servers** from MSK console.
4. In `.env`:

```env
KAFKA_BOOTSTRAP_SERVERS=b-1.xxx.kafka.ap-south-1.amazonaws.com:9092,b-2.xxx.kafka.ap-south-1.amazonaws.com:9092
```

Producer and validation consumer use the same code; point them to this bootstrap list.

---

## 4. MongoDB on AWS (Optional)

- **Option A**: **Amazon DocumentDB** (MongoDB-compatible). Create cluster, get endpoint, set:

```env
MONGO_URI=mongodb://user:pass@your-cluster.cluster-xxxxx.ap-south-1.docdb.amazonaws.com:27017/?tls=true
MONGO_DATABASE=iot_data
```

- **Option B**: Keep MongoDB on EC2 or use Atlas; set `MONGO_URI` accordingly.

Streaming and batch jobs already use `MONGO_URI` from env.

---

## 5. AWS Glue (Spark Jobs)

**Batch job (daily):**

1. Package `spark_batch/` (and dependencies) into a ZIP or use Glue’s Python shell/Spark with job parameters.
2. Create **Glue Job** (Spark, Python 3), main script pointing to your `batch_job.py` or a thin wrapper.
3. Set job parameters / environment to pass:
   - `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, etc. (from Secrets Manager or job params).
   - `S3_BUCKET_PROCESSED` if you write batch output to S3.
4. Schedule with **EventBridge** or **MWAA** (recommended).

**Streaming job:**

- Glue Streaming is possible; for this project, **EMR** with Spark Streaming is often easier. Alternatively run the existing `streaming_job.py` on an **EMR** cluster with Kafka libraries, checkpoint path set to `S3_CHECKPOINT_LOCATION`.

---

## 6. Amazon MWAA (Airflow)

1. Create **MWAA** environment (small size for demo), same VPC as RDS/MSK.
2. Upload DAGs from `airflow/dags/` to the MWAA DAGs S3 bucket.
3. In MWAA, create **Connections** (e.g. `postgres_default`) for RDS; use **Variables** or **Secrets Manager** for passwords.
4. Update DAGs to use AWS connection IDs if you added them (see `airflow/dags/README_AWS.md` or comments in DAG files).
5. Schedule: ingestion/validation DAG (e.g. every 10 min), batch DAG (daily 02:00), transformation DAG (daily 03:00).

---

## 7. FastAPI on AWS

- **ECS (Fargate)**: Build Docker image from `api/` and `docker/Dockerfile.api`, push to **ECR**, run as Fargate service; env from ECS task def or Secrets Manager.
- **EC2**: Run container or Python with same env vars (RDS, etc.).
- Ensure security groups allow API → RDS (and optional DocumentDB).

---

## 8. Environment Summary (.env for AWS)

```env
USE_AWS=1
AWS_REGION=ap-south-1

# S3
S3_BUCKET_RAW=s3://your-bucket/iot/raw/
S3_BUCKET_PROCESSED=s3://your-bucket/iot/processed/
S3_CHECKPOINT_LOCATION=s3://your-bucket/iot/checkpoints/streaming/

# MSK
KAFKA_BOOTSTRAP_SERVERS=b-1.xxx.kafka.ap-south-1.amazonaws.com:9092

# RDS
POSTGRES_HOST=your-rds.region.rds.amazonaws.com
POSTGRES_PORT=5432
POSTGRES_DB=iot_analytics
POSTGRES_USER=admin
POSTGRES_PASSWORD=***

# DocumentDB (if used)
MONGO_URI=mongodb://user:pass@cluster.region.docdb.amazonaws.com:27017/?tls=true
MONGO_DATABASE=iot_data
```

---

## 9. Resume / Interview Talking Points

- “Pipeline runs on **Amazon MSK**, **S3**, **AWS Glue/EMR**, **RDS**, and **MWAA**; same code as local with env-driven config.”
- “Used **S3** for data lake (raw/processed) and Spark checkpoints.”
- “Orchestrated with **Amazon MWAA**; DAGs trigger Glue jobs and dbt against **Redshift/RDS**.”
- “Designed for 2026 AWS Data Engineer roles with clear AWS service ownership.”

---

**Last Updated**: February 2026
