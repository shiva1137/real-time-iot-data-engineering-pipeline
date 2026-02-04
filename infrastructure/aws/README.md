# AWS Infrastructure – IoT Data Pipeline

This folder documents **AWS infrastructure** for the IoT data pipeline so you can present it as an **AWS Data Engineer** project (2026 India roles).

## What to Create in AWS (Manual / IaC)

| Resource | Service | Purpose |
|----------|---------|--------|
| Bucket | **S3** | Data lake: raw, processed, checkpoints |
| Cluster | **Amazon MSK** | Kafka (raw_iot_data, validated_iot_data, dlq_iot_data) |
| Instance | **Amazon RDS** | PostgreSQL (iot_analytics) |
| Cluster (optional) | **DocumentDB** | MongoDB-compatible write store |
| Job | **AWS Glue** | Spark batch (same as spark_batch/batch_job.py) |
| Environment | **Amazon MWAA** | Airflow DAGs |
| Repository | **ECR** | Docker images (API, generator) |
| Cluster (optional) | **EMR** | Spark streaming if not using Glue streaming |

## Optional: CI/CD for AWS

To add **GitHub Actions** deploy to AWS:

1. **Secrets** (in repo Settings → Secrets): `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` (e.g. `ap-south-1`).
2. **Build and push to ECR** on push to `main`:
   - Configure OIDC (recommended) or static credentials.
   - `aws ecr get-login-password` → `docker build` → `docker push` to your ECR repo.
3. **Update Glue job**: Upload `spark_batch/` (and deps) to S3; point Glue job script to that path.
4. **Update MWAA**: Sync `airflow/dags/` to MWAA DAGs S3 bucket (e.g. `aws s3 sync airflow/dags/ s3://your-mwaa-bucket/dags/`).

Example (pseudo) for ECR push only:

```yaml
# .github/workflows/deploy-aws.yml (optional)
deploy-ecr:
  runs-on: ubuntu-latest
  if: github.ref == 'refs/heads/main'
  steps:
    - uses: actions/checkout@v4
    - uses: aws-actions/configure-aws-credentials@v4
      with:
        role-to-assume: arn:aws:iam::ACCOUNT:role/GitHubActionsRole
        aws-region: ap-south-1
    - name: Login to ECR
      uses: aws-actions/amazon-ecr-login@v2
    - name: Build and push API image
      run: |
        docker build -f docker/Dockerfile.api -t $ECR_REGISTRY/iot-api:latest .
        docker push $ECR_REGISTRY/iot-api:latest
```

## Terraform / CloudFormation

You can define the above resources in **Terraform** or **AWS CloudFormation** in this folder (e.g. `main.tf` or `template.yaml`). Minimum for a demo:

- S3 bucket
- RDS PostgreSQL (small instance)
- MSK cluster (1 broker)
- Security groups and VPC as needed

## Resume Keywords (from this project)

- **Amazon S3**, **Amazon MSK**, **Amazon RDS**, **Amazon DocumentDB**
- **AWS Glue**, **Amazon EMR**, **Amazon MWAA**
- **Amazon Redshift** (optional, for dbt)
- **ECR**, **ECS**, **CloudWatch**

See [docs/architecture_aws.md](../../docs/architecture_aws.md) and [docs/aws_deployment_guide.md](../../docs/aws_deployment_guide.md) for architecture and step-by-step deployment.
