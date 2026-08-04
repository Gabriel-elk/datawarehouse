{{ config(materialized='incremental',
unique_key='id') }}

with meteo_blue as (

SELECT concat('meteo_blue_', cast(d.id as varchar)) as id,
d.data_hora,
cast(d.temperature as float) d.temperature,
cast (d.wind_speed as float) d.wind_speed,
cast (null as float) as d.precipitation_probability,
d.precipitation,
d.cloud_cover,
d.visibility,
'METEO_BLUE' as source_name
    FROM {{ source('stg', 'stg_tests_meteo_blue_data') }} d
    {%if is_incremental()%}
    where d.data_hora > (
        SELECT coalesce(
            max(data_hora),
            cast('1900-01-01' as timestamp)
        )
        from {{this}}
        where source_name = 'METEO_BLUE'
    )
    {% endif %}
),
open_meteo as (
SELECT
concat('open_meteo_', cast(o.id as varchar)) as id,
o.data_hora,
 cast (null as float) o.apparent_temperature,
  o.precipitation,
   o.cloud_cover,
   o.visibility,
    cast (null as float) o.wind_speed_10m, 
    cast (null as float) o.soil_temperature_0cm,
    'OPEN_METEO' as source_name
    FROM {{ source('stg', 'stg_tests_open_meteo_data') }} o
       {%if is_incremental()%}
    where o.data_hora > (
        SELECT coalesce(
            max(data_hora),
            cast('1900-01-01' as timestamp)
        )
        from {{this}}
        where source_name = 'OPEN_METEO'
    )
    {% endif %}
),
mart_meteorologic_data as (
    SELECT * FROM meteo_blue
    UNION ALL
    SELECT * FROM open_meteo
)

SELECT * FROM mart_meteorologic_data