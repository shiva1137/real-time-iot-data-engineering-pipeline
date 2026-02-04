-- Mart: 1 row per sensor per day
{{ config(materialized='table') }}
select
    sensor_id,
    reading_date as date,
    location,
    state,
    device_type,
    avg(avg_temperature) as avg_temp,
    max(max_temp) as max_temp,
    min(min_temp) as min_temp,
    sum(reading_count) as count,
    sum(case when is_anomaly then 1 else 0 end) as anomaly_count
from {{ ref('int_iot_with_features') }}
group by sensor_id, reading_date, location, state, device_type
