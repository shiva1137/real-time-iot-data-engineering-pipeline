-- Mart: 1 row per location per day (aggregate across sensors)
{{ config(materialized='table') }}
select
    location,
    state,
    reading_date as date,
    count(distinct sensor_id) as sensor_count,
    avg(avg_temperature) as avg_temp,
    max(max_temp) as max_temp,
    min(min_temp) as min_temp,
    sum(total_energy_consumption) as total_energy,
    sum(reading_count) as total_readings
from {{ ref('int_iot_with_features') }}
group by location, state, reading_date
