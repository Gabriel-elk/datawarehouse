# from airflow.decorators import dag, task
# from airflow import DAG
# from datetime import datetime
# from airflow.operators.bash import BashOperator
# with DAG(
#     dag_id="operator_meteoblue_data_DBT",
#     start_date=datetime(2026,1,1),
#     schedule=None,
#     catchup=False,
#     max_active_tasks=1,
#     max_active_runs=1
# ) as dag:
#     BashOperator(
#         task_id="dbt_run",
#         bash_command="cd /opt/airflow/dbt/project_datawarehouse && dbt run --select stg_tests_meteo_blue_data --target prod"
#     )
