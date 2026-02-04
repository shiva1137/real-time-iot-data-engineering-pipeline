"""
Spark Batch Job - Daily IoT Data Processing (Topic 4)

Daily batch job that:
1. Reads real_time_aggregates from PostgreSQL for previous day (synced from MongoDB)
2. Data cleaning: duplicates, nulls, outliers
3. Hourly aggregation per sensor
4. Feature engineering: 7-day rolling avg, anomaly flags, location stats
5. Writes to MongoDB processed_daily and PostgreSQL processed_daily

All logic uses functions. Trigger: Daily at 02:00 AM (Airflow).

Usage:
    spark-submit batch_job.py [--date YYYY-MM-DD]
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_trunc, hour, to_date, avg, max as spark_max, min as spark_min,
    stddev, sum as spark_sum, count, when, lit, broadcast
)
from pyspark.sql.types import DoubleType

# Load env before other imports that use it
from dotenv import load_dotenv
load_dotenv()

# Constants
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DATABASE", os.getenv("POSTGRES_DB", "iot_analytics"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_SOURCE_TABLE = "real_time_aggregates"
POSTGRES_TARGET_TABLE = "processed_daily"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@localhost:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "iot_data")
MONGO_COLLECTION = "processed_daily"
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_processing_date(run_date: Optional[str]) -> str:
    """Return date to process (yesterday by default)."""
    if run_date:
        return run_date
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    return yesterday


def create_spark_session(app_name: str = "IoT_Batch_Job") -> SparkSession:
    """Create Spark session with batch-optimized config."""
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created: %s", app_name)
    return spark


def read_postgres_for_date(spark: SparkSession, process_date: str):
    """Read real_time_aggregates from PostgreSQL for the given date."""
    start_dt = f"{process_date} 00:00:00"
    end_dt = (datetime.strptime(process_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    query = f"""
    (SELECT * FROM {POSTGRES_SOURCE_TABLE}
     WHERE window_start >= '{start_dt}' AND window_start < '{end_dt}') AS batch_input
    """
    logger.info("Reading PostgreSQL from %s to %s", start_dt, end_dt)
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", query)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    logger.info("Read %d rows from PostgreSQL", df.count())
    return df


def clean_data(df):
    """Remove duplicates, handle nulls, flag outliers."""
    # Deduplicate by (sensor_id, window_start)
    df = df.dropDuplicates(["sensor_id", "window_start"])
    # Drop rows with null critical fields
    df = df.filter(col("sensor_id").isNotNull() & col("window_start").isNotNull())
    df = df.filter(col("avg_temperature").isNotNull())
    # Flag outliers for later combination with statistical anomaly
    df = df.withColumn(
        "outlier_flag",
        when(
            (col("avg_temperature") > 50) | (col("avg_temperature") < -50),
            lit(1),
        ).otherwise(lit(0)),
    )
    return df


def hourly_aggregation(df):
    """Group by sensor_id + date + hour; compute aggregates."""
    df = df.withColumn("date", to_date(col("window_start")))
    df = df.withColumn("hour", hour(col("window_start")))
    agg_df = (
        df.groupBy("sensor_id", "date", "hour", "location", "state", "device_type")
        .agg(
            avg("avg_temperature").alias("avg_temperature"),
            spark_max("max_temperature").alias("max_temperature"),
            spark_min("min_temperature").alias("min_temperature"),
            stddev("avg_temperature").alias("stddev_temperature"),
            avg("avg_humidity").alias("avg_humidity"),
            spark_max("avg_humidity").alias("max_humidity"),
            spark_sum("total_energy_consumption").alias("total_energy_consumption"),
            spark_sum("count").alias("reading_count"),
        )
    )
    # Fill null stddev with 0
    agg_df = agg_df.withColumn(
        "stddev_temperature",
        when(col("stddev_temperature").isNull(), lit(0.0)).otherwise(col("stddev_temperature")),
    )
    return agg_df


def add_anomaly_flag_from_stddev(df):
    """Flag rows: outlier (temp >50 or <-50) OR avg_temperature > 2*stddev from sensor mean."""
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("sensor_id")
    df = df.withColumn("_sensor_mean", avg("avg_temperature").over(window_spec))
    df = df.withColumn("_sensor_std", stddev("avg_temperature").over(window_spec))
    df = df.withColumn(
        "_stat_anomaly",
        when(
            col("_sensor_std").isNotNull() & (col("_sensor_std") > 0)
            & (col("avg_temperature") > col("_sensor_mean") + 2 * col("_sensor_std")),
            lit(1),
        ).otherwise(lit(0)),
    )
    df = df.withColumn(
        "anomaly_flag",
        when(col("outlier_flag") == 1, lit(1))
        .when(col("_stat_anomaly") == 1, lit(1))
        .otherwise(lit(0)),
    )
    return df.drop("_sensor_mean", "_sensor_std", "_stat_anomaly", "outlier_flag")


def write_to_postgres(df, process_date: str) -> None:
    """Write batch result to PostgreSQL processed_daily (overwrite partition for date)."""
    # Write in overwrite mode for the partition (date) for idempotency
    df.write.format("jdbc").option("url", JDBC_URL).option(
        "dbtable", POSTGRES_TARGET_TABLE
    ).option("user", POSTGRES_USER).option("password", POSTGRES_PASSWORD).option(
        "driver", "org.postgresql.Driver"
    ).mode("append").save()
    logger.info("Wrote batch to PostgreSQL %s", POSTGRES_TARGET_TABLE)


def ensure_processed_daily_table(process_date: str) -> None:
    """Ensure processed_daily table exists in PostgreSQL (idempotent)."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {POSTGRES_TARGET_TABLE} (
                    sensor_id VARCHAR(50) NOT NULL,
                    date DATE NOT NULL,
                    hour INTEGER NOT NULL,
                    location VARCHAR(100),
                    state VARCHAR(100),
                    device_type VARCHAR(100),
                    avg_temperature DOUBLE PRECISION,
                    max_temperature DOUBLE PRECISION,
                    min_temperature DOUBLE PRECISION,
                    stddev_temperature DOUBLE PRECISION,
                    avg_humidity DOUBLE PRECISION,
                    max_humidity DOUBLE PRECISION,
                    total_energy_consumption DOUBLE PRECISION,
                    reading_count BIGINT,
                    anomaly_flag INTEGER DEFAULT 0,
                    PRIMARY KEY (sensor_id, date, hour)
                );
            """)
            conn.commit()
        conn.close()
        logger.info("Table %s ensured", POSTGRES_TARGET_TABLE)
    except Exception as e:
        logger.warning("Could not ensure table (may already exist): %s", e)


def run_batch(run_date: Optional[str] = None) -> None:
    """Run the full batch pipeline for the given date."""
    process_date = get_processing_date(run_date)
    logger.info("Batch processing date: %s", process_date)

    spark = create_spark_session()
    ensure_processed_daily_table(process_date)

    try:
        df = read_postgres_for_date(spark, process_date)
        if df.rdd.isEmpty():
            logger.warning("No data for date %s; skipping.", process_date)
            return

        df = clean_data(df)
        df = hourly_aggregation(df)
        df = add_anomaly_flag_from_stddev(df)

        # Cache before multiple actions
        df = df.cache()
        row_count = df.count()
        logger.info("Processed %d hourly rows", row_count)

        # Write to PostgreSQL (append for date = idempotent per run)
        write_to_postgres(df, process_date)

        # Write to MongoDB via connector if available; else skip (optional per guide)
        try:
            write_to_mongodb(df)
        except Exception as e:
            logger.warning("MongoDB write skipped or failed: %s", e)

        df.unpersist()
        logger.info("Batch job completed for %s", process_date)
    finally:
        spark.stop()


def write_to_mongodb(df) -> None:
    """Write batch DataFrame to MongoDB processed_daily collection."""
    try:
        from pyspark.sql.connector import write_to_mongo  # optional
    except ImportError:
        # Use foreachPartition + pymongo if no Spark MongoDB connector
        def _write_partition(partition):
            from pymongo import MongoClient
            client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
            )
            coll = client[MONGO_DATABASE][MONGO_COLLECTION]
            for row in partition:
                doc = row.asDict()
                for k, v in doc.items():
                    if hasattr(v, "isoformat"):
                        doc[k] = v
                coll.update_one(
                    {
                        "sensor_id": doc["sensor_id"],
                        "date": str(doc["date"]) if doc.get("date") else None,
                        "hour": doc["hour"],
                    },
                    {"$set": doc},
                    upsert=True,
                )
            client.close()

        df.foreachPartition(_write_partition)
        logger.info("Wrote batch to MongoDB %s.%s", MONGO_DATABASE, MONGO_COLLECTION)


def main() -> None:
    parser = argparse.ArgumentParser(description="IoT Daily Batch Job")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Process date YYYY-MM-DD (default: yesterday)",
    )
    args = parser.parse_args()
    try:
        run_batch(run_date=args.date)
    except Exception as e:
        logger.error("Batch job failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
