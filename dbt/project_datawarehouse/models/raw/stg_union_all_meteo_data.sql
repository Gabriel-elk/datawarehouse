{{
    config(
        materialized = 'incremental',
        unique_key = 'id'
    )
}}

with meteo_blue as (

    select
        concat('meteo_blue_', d.id) as id,
        d.data_hora,
        d.temperature,
        d.wind_speed,
        cast(null as float) as apparent_temperature,
        d.precipitation_probability,
        d.precipitation,
        cast(null as float) as cloud_cover,
        cast(null as float) as visibility,
        cast(null as float) as soil_temperature_0cm,
        'METEO_BLUE' as source_name

    from {{ source('staging', 'stg_tests_meteo_blue_data') }} d

    {% if is_incremental() %}

        where d.data_hora > (
            select coalesce(
                max(data_hora),
                cast('1900-01-01' as timestamp)
            )
            from {{ this }}
            where source_name = 'METEO_BLUE'
        )

    {% endif %}

),

open_meteo as (

    select
        concat('open_meteo_', o.id) as id,
        o.data_hora,
        cast(null as float) as temperature,
        o.wind_speed_10m as wind_speed,
        o.apparent_temperature,
        cast(null as float) as precipitation_probability,
        o.precipitation,
        o.cloud_cover,
        o.visibility,
        o.soil_temperature_0cm,
        'OPEN_METEO' as source_name

    from {{ source('staging', 'stg_tests_open_meteo_data') }} o

    {% if is_incremental() %}

        where o.data_hora > (
            select coalesce(
                max(data_hora),
                cast('1900-01-01' as timestamp)
            )
            from {{ this }}
            where source_name = 'OPEN_METEO'
        )

    {% endif %}

),

mart_meteorologic_data as (

    select * from meteo_blue

    union all

    select * from open_meteo

)

select *
from mart_meteorologic_data

-- Campo existe na fonte:
-- d.temperature

-- Campo existe, mas tem outro nome:
-- o.wind_speed_10m as wind_speed

-- Campo não existe naquela fonte:
-- cast(null as float) as soil_temperature_0cm