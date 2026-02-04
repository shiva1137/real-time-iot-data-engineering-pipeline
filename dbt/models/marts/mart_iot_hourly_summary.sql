-- Mart: 1 row per sensor per hour (from intermediate)
{{ config(materialized='table') }}
select
    sensor_id,
    reading_date,
    hour,
    location,
    state,
    device_type,
    avg_temperature,
    max_temp,
    min_temp,
    avg_humidity,
    total_energy_consumption,
    reading_count,
    is_anomaly,
    sensor_status,
    is_daytime,
    is_weekend
from {{ ref('int_iot_with_features') }}
