from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter, Retry

@dag(
    schedule=None,
    start_date=datetime(2026,1,1),
    catchup=False
    )
def data_meteoblue():
    def retry_config():
        s=requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502,503,504])
        s.mount('http://', HTTPAdapter(max_retries=retries))
        s.mount('https://', HTTPAdapter(max_retries=retries))
        return s
    @task
    def extract():
        load_dotenv(dotenv_path = '/opt/airflow/dags/.env')
        MY_API_METEOBLUE = os.getenv("MY_API_METEOBLUE")
        if MY_API_METEOBLUE:
            print("valor recebido:", MY_API_METEOBLUE)
        else:
            print("valor inválido ou não encontrado")
        baseURL = f"https://my.meteoblue.com/packages/basic-1h,agro-day,basic-day?lat=-24.940921&lon=-53.471636&tz=UTC&apikey={MY_API_METEOBLUE}"
        session = retry_config()
        response = session.get(baseURL, timeout=30)
        data = response.json()
        print(data)
        return data
    @task(multiple_outputs=True)
    def adjust_data(data):
        df_data = pd.DataFrame(data['data_1h'])
        #essa vai ser de dados climáticos   
        df_data_acontecido = pd.DataFrame(df_data['time'])   
        df_snowfraction = pd.DataFrame(df_data['snowfraction'])          
        df_windspeed = pd.DataFrame(df_data['windspeed'])
        df_temperature = pd.DataFrame(df_data['temperature'])
        df_precipitation_probability = pd.DataFrame(df_data['precipitation_probability'])          
        df_convective_precipitation = pd.DataFrame(df_data['convective_precipitation'])
        df_rainspot = pd.DataFrame(df_data['rainspot'])
        df_pictocode = pd.DataFrame(df_data['pictocode'])          
        df_felttemperature = pd.DataFrame(df_data['felttemperature'])
        df_precipitation = pd.DataFrame(df_data['precipitation'])
        df_isdaylight = pd.DataFrame(df_data['isdaylight'])          
        df_uvindex = pd.DataFrame(df_data['uvindex'])
        df_relativehumidity = pd.DataFrame(df_data['relativehumidity'])
        df_sealevelpressure = pd.DataFrame(df_data['sealevelpressure'])
        df_winddirection = pd.DataFrame(df_data['winddirection'])
        df_uvindex = pd.DataFrame(df_data['uvindex'])
        df_relativehumidity = pd.DataFrame(df_data['relativehumidity'])
        
        #put everything together 
        
        df_data_final = pd.concat([
                    df_data_acontecido,
                    df_snowfraction,
                    df_windspeed,
                    df_temperature,
                    df_precipitation_probability,
                    df_convective_precipitation,
                    df_rainspot,
                    df_pictocode,
                    df_felttemperature,
                    df_precipitation,
                    df_isdaylight,
                    df_uvindex,
                    df_relativehumidity,
                    df_sealevelpressure,
                    df_winddirection
                ], axis=1)

        
        #separação dos df para tabelas diferentes essa vai ser de dados campo
        df_campo = pd.DataFrame(data['data_day'])
        df_time = pd.DataFrame(df_campo['time'])
        df_delta_t_min = pd.DataFrame(df_campo['delta_t_min'])
        df_delta_t_mean = pd.DataFrame(df_campo['delta_t_mean'])
        df_temperature_instant = pd.DataFrame(df_campo['temperature_instant'])
        df_temperature_min = pd.DataFrame(df_campo['temperature_min'])
        df_temperature_mean = pd.DataFrame(df_campo['temperature_mean'])
        df_temperature_max = pd.DataFrame(df_campo['temperature_max'])
        df_predictability = pd.DataFrame(df_campo['predictability'])
        df_predictability_class = pd.DataFrame(df_campo['predictability_class'])
        df_skintemperature_min = pd.DataFrame(df_campo['skintemperature_min'])
        df_skintemperature_mean = pd.DataFrame(df_campo['skintemperature_mean'])
        df_skintemperature_max = pd.DataFrame(df_campo['skintemperature_max'])          
        df_uvindex = pd.DataFrame(df_campo['uvindex'])
        df_indexto1hvalues_start = pd.DataFrame(df_campo['indexto1hvalues_start'])          
        df_indexto1hvalues_end = pd.DataFrame(df_campo['indexto1hvalues_end'])          
        df_soiltemperature_0to10cm_min = pd.DataFrame(df_campo['soiltemperature_0to10cm_min'])
        df_soiltemperature_0to10cm_mean = pd.DataFrame(df_campo['soiltemperature_0to10cm_mean'])
        df_soilmoisture_0to10cm_max = pd.DataFrame(df_campo['soilmoisture_0to10cm_max'])
        df_convective_precipitation = pd.DataFrame(df_campo['convective_precipitation'])          
        df_precipitation_probability = pd.DataFrame(df_campo['precipitation_probability'])
        df_precipitation_hours = pd.DataFrame(df_campo['precipitation_hours'])
        df_precipitation = pd.DataFrame(df_campo['precipitation'])
        df_potentialevapotranspiration = pd.DataFrame(df_campo['potentialevapotranspiration'])
        df_referenceevapotranspiration_fao = pd.DataFrame(df_campo['referenceevapotranspiration_fao'])          
        df_evapotranspiration = pd.DataFrame(df_campo['evapotranspiration'])
        df_sealevelpressure_min = pd.DataFrame(df_campo['sealevelpressure_min'])
        df_sealevelpressure_mean = pd.DataFrame(df_campo['sealevelpressure_mean'])
        df_winddirection = pd.DataFrame(df_campo['winddirection'])
        df_felttemperature_mean = pd.DataFrame(df_campo['felttemperature_mean'])
        df_windspeed_min = pd.DataFrame(df_campo['windspeed_min'])
        df_windspeed_max = pd.DataFrame(df_campo['windspeed_max'])
        df_windspeed_mean = pd.DataFrame(df_campo['windspeed_mean'])
        df_pictocode = pd.DataFrame(df_campo['pictocode'])
        df_relativehumidity_min = pd.DataFrame(df_campo['relativehumidity_min'])
        df_relativehumidity_mean = pd.DataFrame(df_campo['relativehumidity_mean'])
        df_relativehumidity_max = pd.DataFrame(df_campo['relativehumidity_max'])
        df_humiditygreater90_hours = pd.DataFrame(df_campo['humiditygreater90_hours'])
        df_felttemperature_min = pd.DataFrame(df_campo['felttemperature_min'])
        df_felttemperature_max = pd.DataFrame(df_campo['felttemperature_max'])
        #criar a tabela df_campo_final
        df_campo_final = pd.concat([
            df_time,
            df_delta_t_min,
            df_delta_t_mean,
            df_temperature_instant,
            df_temperature_min,
            df_temperature_mean,
            df_temperature_max,
            df_predictability,
            df_predictability_class,
            df_skintemperature_min,
            df_skintemperature_mean,
            df_skintemperature_max,
            df_uvindex,
            df_indexto1hvalues_start,
            df_indexto1hvalues_end,
            df_soiltemperature_0to10cm_min,
            df_soiltemperature_0to10cm_mean,
            df_soilmoisture_0to10cm_max,
            df_convective_precipitation,
            df_precipitation_probability,
            df_precipitation_hours,
            df_precipitation,
            df_potentialevapotranspiration,
            df_referenceevapotranspiration_fao,
            df_evapotranspiration,
            df_sealevelpressure_min,
            df_sealevelpressure_mean,
            df_winddirection,
            df_felttemperature_mean,
            df_windspeed_min,
            df_windspeed_max,
            df_windspeed_mean,
            df_pictocode,
            df_relativehumidity_min,
            df_relativehumidity_mean,
            df_relativehumidity_max,
            df_humiditygreater90_hours,
            df_felttemperature_min,
            df_felttemperature_max
        ], axis=1)
    
        """i know about this ( df_data_final = df_data.copy() df_campo_final = df_campo.copy()
        but i cannot control the collumns order and cannot amout the important data)
        """
    
        return {"df_data_final": df_data_final, "df_campo_final": df_campo_final}
        
    @task
    #execussão
    def insert_database(dataset1, dataset2):
        #Inserir no banco
        hook = PostgresHook(postgres_conn_id="dw_postgres")
        engine = hook.get_sqlalchemy_engine()
        contagem = dataset1.to_sql(
                    name="raw_meteo_blue_data",
                    schema="raw",
                    con=engine,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",)
        print(f"Tabela Criada com Sucesso foram : {contagem} Registros inseridos")
        contagem2 = dataset2.to_sql(
                    name="raw_meteo_blue_campo",
                    schema="raw",
                    con=engine,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",)
        print(f"Tabela Criada com Sucesso foram : {contagem2} Registros inseridos")

    values = adjust_data(extract())
    insert_database(values["df_data_final"], values["df_campo_final"])
#função principal
data_meteoblue()
