# Airflow DAGs – Amazon MWAA (AWS)

These DAGs run as-is on **Amazon Managed Workflows for Apache Airflow (MWAA)**.

## Deployment on MWAA

1. **Create MWAA environment** in AWS (e.g. ap-south-1). Use the same VPC as your RDS and MSK if they are in AWS.
2. **Upload DAGs**: Copy the contents of this `dags/` folder to the S3 bucket configured for MWAA DAGs (e.g. `s3://your-mwaa-bucket/dags/`).
3. **Environment variables**: MWAA does not support arbitrary env vars in the UI. Use one or both:
   - **Airflow Variables**: In MWAA console, set Variables such as `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`, etc. Then in DAG code, read via `Variable.get("POSTGRES_HOST")` and `os.environ["POSTGRES_HOST"] = Variable.get(...)` in a task, or pass to BashOperator env.
   - **Airflow Connections**: Create a connection (e.g. `postgres_default`) with RDS host, login, password, port. Use `PostgresHook` or pass connection id to operators that support it.
4. **DAGs behavior**:
   - `ingestion_validation_dag`: Uses `KAFKA_BOOTSTRAP_SERVERS` (set via Variable or MWAA environment config if available).
   - `batch_processing_dag`: Uses `POSTGRES_*` for wait_for_data and validate_output; Spark batch can be run via Glue or EMR (trigger from MWAA).
   - `transformation_dag`: Runs `dbt run` / `dbt test`; ensure the MWAA environment has dbt and `profiles.yml` pointing to RDS/Redshift (e.g. via Variable or mounted config).

## Triggering AWS Glue from MWAA

To run the batch job on **AWS Glue** instead of local Spark:

- Install `airflow.providers.amazon.aws` and use `GlueJobOperator` in a new task or replace the `run_spark_batch` BashOperator with:

  ```python
  from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
  run_glue = GlueJobOperator(
      task_id="run_glue_batch",
      job_name="your-glue-job-name",
      region_name="ap-south-1",
  )
  ```

- Ensure the Glue job is configured with the same logic as `spark_batch/batch_job.py` and has access to RDS and S3.

## Resume / Interview

- "Orchestrated pipelines with **Amazon MWAA**; DAGs for ingestion validation, batch processing, and dbt transformations."
- "Used Airflow Variables and Connections for RDS and Kafka (MSK) when running on AWS."
