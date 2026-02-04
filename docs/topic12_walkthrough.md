# Topic 12: Project Walkthrough & Implementation Summary

## What Was Added/Changed (Topics 3–12)

### Topic 3 – Spark Streaming
- **streaming_job.py**: Fixed trigger to use `Trigger.processingTime("10 seconds")`. Already had 5-min windows, 1-min watermark, UPDATE mode, checkpointing, MongoDB writes with retry and backpressure detection.
- **mongo_to_postgres_sync.py**: Already present; syncs `real_time_aggregates` to PostgreSQL every 5 minutes (run via cron or Airflow).

### Topic 4 – Batch Processing
- **batch_job.py**: Implemented from scratch. Reads `real_time_aggregates` from PostgreSQL for a given date, cleans (dedup, nulls, outlier flags), aggregates by sensor/hour, adds anomaly flag (outlier or 2×stddev), ensures `processed_daily` table, writes to PostgreSQL (append) and MongoDB (foreachPartition). Run: `spark-submit batch_job.py [--date YYYY-MM-DD]`.

### Topic 5 – Data Quality
- **validators.py**: New module with schema, type, range, format, freshness, completeness, and duplicate validation; single `validate_record()` API.
- **validation_consumer.py**: Refactored to import `validate_record` from `validators`; keeps metrics and DLQ routing. DLQ topic: `dlq_iot_data`.

### Topic 6 – dbt
- **sources.yml**: Source `raw.processed_daily` (public schema).
- **staging/stg_iot_readings.sql**: Clean and standardize from `processed_daily`.
- **intermediate/int_iot_with_features.sql**: Time features, `sensor_status`, `is_anomaly`, `is_daytime`, `is_weekend`.
- **marts**: `mart_iot_daily_summary`, `mart_iot_hourly_summary`, `mart_iot_location_stats`.
- **tests**: `staging/schema.yml` (not_null), `tests/temperature_range.sql` (custom -50 to 50).

### Topic 7 – FastAPI
- **api/main.py**: FastAPI app with CORS, lifespan (DB pool), request_id middleware, global exception handler.
- **api/database.py**: Connection pool (SimpleConnectionPool), `get_connection()`, `check_connection()`, `get_latest_data_timestamp()`.
- **api/models/schemas.py**: Pydantic models for sensors list, analytics response, health, error.
- **api/routes/sensors.py**: GET `/sensors` (list with optional location/limit/offset), GET `/sensors/{sensor_id}/analytics` (start_date, end_date, granularity).
- **api/routes/health.py**: GET `/health` (DB, latest data timestamp; optionally calls monitoring alerts).

### Topic 8 – Airflow
- **ingestion_validation_dag.py**: Every 10 min; check_kafka_health, count_messages, alert_if_low (< 50).
- **batch_processing_dag.py**: Daily 02:00; wait_for_data, run_spark_batch, validate_output.
- **transformation_dag.py**: Daily 03:00; dbt_run, dbt_test, generate_docs.

### Topic 9 – Monitoring
- **logging_config.py**: Structured JSON formatter (timestamp, level, service, task, message, context); console + optional file.
- **alerts.py**: Threshold alerts (data freshness > 5 min, quality < 95%, throughput, DLQ size, API error rate, DAG failure); severity HIGH/MEDIUM; emit to log.

### Topic 10 – Docker
- **Dockerfile.airflow**: Airflow 2.7 image, postgresql-client, requirements-airflow.txt (psycopg2, kafka-python).
- **.env.example**: Aligned with all components (MONGO_URI, POSTGRES_*, KAFKA_*, CHECKPOINT_LOCATION, etc.).

### Topic 11 – CI/CD
- **.github/workflows/ci.yml**: Added `security` job (pip-audit on requirements); updated actions to v4/v5.

### Topic 12 – Walkthrough
- This doc and README updated to 12/12 complete.

---

## How Failure Scenarios Are Handled

| Stage | Failure | Detection | Handling |
|-------|--------|-----------|----------|
| **Ingestion** | Kafka down | Producer retries (Topic 2); Airflow check_kafka_health | Retries, DLQ for send failures; alert on low message count |
| **Streaming** | MongoDB down | Write exception in foreachBatch | Tenacity retry (exponential backoff, long max duration); Spark checkpoint so no offset commit until write succeeds |
| **Streaming** | Spark crash | Checkpoint | Restart from checkpoint; reprocess from last committed offset |
| **Batch** | PostgreSQL empty | wait_for_data task | DAG fails; retry once after 30 min |
| **Batch** | Write failure | validate_output task | DAG fails; manual investigation |
| **Data quality** | >10% invalid | Metrics in consumer | Alerts (alerts.py) if quality < 95%; DLQ for invalid records |
| **dbt** | Test failure | dbt test | transformation_dag fails; no retry (real issue) |
| **API** | DB down | GET /health | Returns unhealthy; optional emit_alert HIGH |
| **API** | Stale data | Health + alerts | check_data_freshness in health route; alert if > 5 min |
| **Airflow** | DAG/task fail | Airflow UI + retries | Retries per DAG; alert_dag_failed in monitoring (call from callback if desired) |

Contracts are aligned: Kafka topics (`raw_iot_data`, `validated_iot_data`, `dlq_iot_data`), PostgreSQL tables (`real_time_aggregates`, `processed_daily`), and dbt source `raw.processed_daily` match the architecture.
