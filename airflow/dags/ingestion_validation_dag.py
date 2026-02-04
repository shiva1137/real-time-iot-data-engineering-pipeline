"""
DAG 1: Ingestion & Validation (Topic 8)

Schedule: Every 10 minutes.
Tasks: check_kafka_health, count_messages, alert_if_low.
Retry: 2 times, 5 min interval. On failure: Alert.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "tags": ["ingestion", "validation"],
}


def check_kafka_health():
    """Verify Kafka is up."""
    import socket
    import os
    host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(":")[0]
    port = int(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(":")[1])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((host, port))
    sock.close()
    if result != 0:
        raise RuntimeError(f"Kafka not reachable at {host}:{port}")


def count_messages(ti):
    """Count messages in raw_iot_data (last 10 mins). Push count to XCom."""
    try:
        from kafka import KafkaConsumer
        import os
        import time
        consumer = KafkaConsumer(
            "raw_iot_data",
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            consumer_timeout_ms=5000,
        )
        consumer.poll(timeout_ms=2000)
        parts = consumer.assignment()
        if parts:
            total = sum(consumer.end_offsets(parts).values())
        else:
            total = 0
        consumer.close()
        ti.xcom_push(key="message_count", value=total)
        return total
    except Exception as e:
        ti.xcom_push(key="message_count", value=0)
        return 0


def alert_if_low(ti):
    """Alert if message count < 50 (expected ~100 in 10 min)."""
    count = ti.xcom_pull(task_ids="count_messages", key="message_count") or 0
    if count < 50:
        raise RuntimeError(f"Low message count: {count} (expected >= 50)")
    return count


with DAG(
    dag_id="ingestion_validation_dag",
    default_args=default_args,
    schedule_interval="*/10 * * * *",  # Every 10 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )
    t2 = PythonOperator(
        task_id="count_messages",
        python_callable=count_messages,
    )
    t3 = PythonOperator(
        task_id="alert_if_low",
        python_callable=alert_if_low,
    )
    t1 >> t2 >> t3
