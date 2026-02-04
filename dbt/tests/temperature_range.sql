-- Custom test: temperature between -50 and 50 (Learning Guide Topic 6)
select sensor_id, reading_date, hour, avg_temperature
from {{ ref('stg_iot_readings') }}
where avg_temperature < -50 or avg_temperature > 50
