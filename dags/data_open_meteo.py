from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
@dag(
    schedule=None,
    start_date=datetime(2026,1,1),
    catchup=False
    )
def data_collection():

    def retry_config():
        s=requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502,503,504])
        s.mount('http://', HTTPAdapter(max_retries=retries))
        s.mount('https://', HTTPAdapter(max_retries=retries))
        return s
    @task
    def extract():
        baseURL = "https://api.open-meteo.com/v1/forecast"
        params = {
        "latitude": -24.9637641,
        "longitude": -53.4230072,
        "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation_probability", "apparent_temperature", "precipitation", "rain", "pressure_msl", "surface_pressure", "cloud_cover", "visibility", "evapotranspiration", "vapour_pressure_deficit", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_speed_10m", "wind_direction_80m", "wind_gusts_10m", "temperature_80m", "showers", "weather_code", "soil_temperature_0cm", "wind_direction_120m", "wind_direction_180m", "temperature_180m", "temperature_120m"],
    }
        session = retry_config()
        response = session.get(baseURL, params=params, timeout=30)
        data = response.json()
        return data
    @task
    def ajustar_dados(data):
        df = pd.DataFrame(data['hourly'])
        print(df)

    ajustar_dados(extract())
data_collection()