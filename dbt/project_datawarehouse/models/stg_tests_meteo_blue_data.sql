{{ config(materialized='incremental',
unique_key='id') }}

with staging_meteo_blue_data as (

select
 	id,
    cast(time as timestamp) as data_hora,
    cast(temperature as float) as temperature,
    coalesce(precipitation, 0) as precipitation,
    precipitation_probability,
    convective_precipitation,
    snowfraction as snow_fraction,
    windspeed as wind_speed,
    rainspot,
    felttemperature as felt_temperature,
    isdaylight as is_day_light,
    uvindex as uv_index,
    cast(relativehumidity as float) as relative_humidity,
    sealevelpressure as sea_level_pressure,
    cast(winddirection as float) as wind_direction
from {{ source('raw', 'raw_meteo_blue_data') }}
        {% if is_incremental() %}
where cast(time as timestamp) > (select coalesce(max(data_hora), cast('1900-01-01' as timestamp)) from {{ this }})
        {% endif %}
)
select *
from staging_meteo_blue_data