
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='staging_meteo_blue_data') }}

with source_data as (

select
    cast(time as timestamp) as data_hora,
    cast(temperature as float) as temperatura,
    coalesce(precipitation, 0) as precipitation,
    precipitation_probability,
    convective_probability,
    snowfraction as snow_fraction,
    windspeed as wind_speed,
    temperature,
    rainspot,
    felttemperature as felt_temperature,
    case extract(HOUR from time)




from {{ source('raw', 'raw_meteo_blue_data') }}

)

select *
from source_data
