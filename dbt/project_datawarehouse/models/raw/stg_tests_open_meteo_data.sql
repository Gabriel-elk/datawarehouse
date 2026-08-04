{{ config(materialized='incremental',
unique_key='id')}}

with staging_open_meteo_data as (
    select 
    id,
    date_time as data_hora,
    temperature_2m,
    cast(cloud_cover as int),
    cast(relative_humidity_2m as int),
    dew_point_2m,
    apparent_temperature,
    cast(precipitation_probability as int),
    precipitation,
    rain,
    evapotranspiration,
    visibility,
    wind_speed_10m,
    wind_speed_80m,
    wind_speed_120m,
    wind_speed_180m,
    cast(wind_direction_10m as int),
    cast(wind_direction_80m as int),
    cast(wind_direction_120m as int),
    cast(wind_direction_180m as int),
    temperature_80m,
    temperature_120m,
    temperature_180m,
    soil_temperature_0cm,
    uv_index,
    is_day,
    sunshine_duration
    from {{source('raw', 'raw_open_meteo')}} as raw
    {%if is_incremental() %}
    where raw.date_time > (select coalesce(max(data_hora))from {{this}})
    {% endif %}
)
select * from staging_open_meteo_data