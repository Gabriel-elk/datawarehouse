{{ config(materialized='incremental',
unique_key='id') }}

with staging_meteo_blue_campo as (
select
        id,
    cast(time as timestamp) as data_hora,
    delta_t_min,
    delta_t_mean,
    temperature_instant,
    temperature_min,
    temperature_mean,
    temperature_max,
    predictability,
    predictability_class,
    skintemperature_min,
    skintemperature_mean,
    skintemperature_max,
    uvindex as uv_index,
    indexto1hvalues_start,
    indexto1hvalues_end,
    soiltemperature_0to10cm_min,
    soiltemperature_0to10cm_max,
    convective_precipitation,
    precipitation_probability,
    precipitation_hours,
    potentialevapotranspiration,
    referenceevapotranspiration_fap,
    evapotranspiration,
    sealevelpressure_min as sea_level_pressure_min,
    sealevelpressure_mean as sea_level_pressure_mean,
    cast(winddirection as float) as wind_direction,
    windspeed_min as wind_speed_min,
    windspeed_max as wind_speed_max,
    windspeed_mean as wind_speed_mean,
    cast(relativehumidity_min as float) as relative_humidity_min,
    cast(relativehumidity_max as float) as relative_humidity_max,
    cast(relativehumidity_mean as float) as relative_humidity_mean,
    coalesce(precipitation, 0) as precipitation,
    humiditygreater90_hours as humidity_greater_90_hours,
    felttemperature_min as felt_temperature_min,
    felttemperature_mean as felt_temperature_mean
from {{ source('raw', 'raw_meteo_blue_campo') }}
        {% if is_incremental() %}
where time > (select max(time) from {{ this }})
        {% endif %}
)
select *
from staging_meteo_blue_campo