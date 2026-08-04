

-- {{config(materialized='incremental',
-- unique_key='id')}}
-- with mart_meteorologic_data as (
-- SELECT d.id, d.data_hora, d.temperature, d.felt_temperature, d.wind_speed, d.precipitation_probability
--     FROM {{ source('stg', 'stg_tests_meteo_blue_data') }}
--     UNION ALL
-- SELECT 
-- o.id, o.data_hora, o.apparent_temperature, o.precipitation, o.cloud_cover, o.visibility, o.wind_speed_10m, o.soil_temperature_0cm
--     FROM {{ source('stg', 'stg_tests_open_meteo_data') }}

--     {%if is_incremental() %}
--     where raw.date_time > (select coalesce(max(data_hora))from {{this}})
--     {% endif %}
-- )
-- select * from mart_meteorologic_data
