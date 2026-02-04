-- Intermediate: time features and sensor_status from staging
-- Grain: 1 row per sensor per date per hour
{{ config(materialized='view') }}
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
    stddev_temperature,
    avg_humidity,
    max_humidity,
    total_energy_consumption,
    reading_count,
    anomaly_flag,
    _loaded_at,
    hour as hour_of_day,
    extract(dow from reading_date) as day_of_week,
    extract(week from reading_date) as week_of_year,
    extract(month from reading_date) as month_of_year,
    case when hour >= 7 and hour < 18 then true else false end as is_daytime,
    case when extract(dow from reading_date) in (0, 6) then true else false end as is_weekend,
    case when anomaly_flag = 1 then true else false end as is_anomaly,
    case
        when anomaly_flag = 1 then 'Faulty'
        when stddev_temperature > 5 then 'Recalibration_needed'
        else 'Working'
    end as sensor_status
from {{ ref('stg_iot_readings') }}
