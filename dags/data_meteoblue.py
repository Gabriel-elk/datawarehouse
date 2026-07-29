from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter, Retry

load_dotenv()
MY_API_METEOBLUE = os.getenv("MY_API_METEOBLUE")
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
        baseURL = "https://my.meteoblue.com/packages/basic-1h,agro-day,basic-day?lat=-24.940921&lon=-53.471636&tz=UTC&apikey=MY_API_METEOBLUE"
        session = retry_config()
        response = session.get(baseURL, timeout=30)
        data = response.json()
        print(data)
        return data
    @task
    def adjust_data(data):
        
        df_data = pd.DataFrame(data['data_1h'])   
        df_snowfraction = pd.DataFrame(data['snowfraction'])          
        df_windspeed = pd.DataFrame(data['windspeed'])
        df_temperature = pd.DataFrame(data['temperature'])
        df_precipitation_probability = pd.DataFrame(data['precipitation_probability'])          
        df_convective_precipitation = pd.DataFrame(data['convective_precipitation'])
        df_rainspot = pd.DataFrame(data['rainspot'])
        df_pictocode = pd.DataFrame(data['pictocode'])          
        df_felttemperature = pd.DataFrame(data['felttemperature'])
        df_precipitation = pd.DataFrame(data['precipitation'])
        df_isdaylight = pd.DataFrame(data['isdaylight'])          
        #ajustar
        df_felttemperature = pd.DataFrame(data['felttemperature'])
        df_precipitation = pd.DataFrame(data['precipitation'])
        
        df_final = pd.concat([df_antigo, df_novo], ignore_index=True) 
        
        
    #execussão
    def insert_database(data):
        #Inserir no banco
        
        hook = PostgresHook(postgres_conn_id="dw_postgres")
        engine = hook.get_sqlalchemy_engine()
        contagem = data.to_sql(
                    name="raw_meteo_blue",
                    schema="raw",
                    con=engine,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",)
        print(f"Tabela Criada com Sucesso foram : {contagem} Registros inseridos")
        

    insert_database(adjust_data(extract()))
#função principal
data_collection()