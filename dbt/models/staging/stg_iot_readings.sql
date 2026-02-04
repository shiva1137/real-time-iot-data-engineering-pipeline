-- Staging: clean processed_daily, rename and standardize types
-- Grain: 1 row per sensor per date per hour
{{ config(materialized='view') }}
select
    sensor_id,
    date::date as reading_date,
    hour,
    location,
    state,
    device_type,
    avg_temperature,
    max_temperature as max_temp,
    min_temperature as min_temp,
    stddev_temperature,
    avg_humidity,
    max_humidity,
    total_energy_consumption,
    reading_count,
    anomaly_flag,
    current_timestamp as _loaded_at,
    row_number() over (order by sensor_id, date, hour) as _row_number
from {{ source('raw', 'processed_daily') }}
where sensor_id is not null
  and date is not null
  and avg_temperature is not null
