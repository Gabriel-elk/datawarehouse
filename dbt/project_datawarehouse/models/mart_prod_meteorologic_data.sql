SELECT d.data_hora, d.temperature, d.felt_temperature, d.wind_speed, d.precipitation_probability,
o.apparent_temperature, o.precipitation, o.cloud_cover, o.visibility, o.wind_speed_10m, o.soil_temperature_0cm
from staging.stg_tests_meteo_blue_data as d
join staging.stg_tests_open_meteo_data as o on o.data_hora = d.data_hora