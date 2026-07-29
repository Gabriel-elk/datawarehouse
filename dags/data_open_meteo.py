from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
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
        latitude = '-24.940921',
        longitude = '-53.471636',
        params = {
        "latitude": -24.940921,
        "longitude": -53.471636,
        "hourly": ["temperature_2m", "cloud_cover", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "weather_code", "visibility", "evapotranspiration", "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m", "temperature_80m", "temperature_120m", "temperature_180m", "soil_temperature_0cm", "uv_index", "is_day", "sunshine_duration"],
    }
        session = retry_config()
        response = session.get(baseURL, params=params, timeout=30)
        data = response.json()
        return data
    @task
    def inserirDados(data):
        hook = PostgresHook(postgres_conn_id="dw_postgres")
        engine = hook.get_sqlalchemy_engine()
        df = pd.DataFrame(data['hourly'])
        df = df.rename(columns={"time" : "date_time"})
        df["date_time"] = pd.to_datetime(df["date_time"])
        contagem = df.to_sql(
            name="raw_open_meteo",
            schema="raw",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",)
        print(f"Tabela Criada com Sucesso foram : {contagem} Registros inseridos")
    #execussão
    inserirDados(extract())
#função principal
data_collection()