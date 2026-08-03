from airflow.decorators import dag, task
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtDag
project_config = ProjectConfig("/opt/airflow/dbt/project_datawarehouse")

#caminho do arquivo (tive dúvidas)
profile_config = ProfileConfig(
    profile_name="project_datawarehouse",   # o nome que está no teu profiles.yml (nome do projeto no caminho cat /home/airflow/.dbt/profiles.yml)
    target_name="dev",                    # dev ou prod
    profiles_yml_filepath="/home/airflow/.dbt/profiles.yml"
)

execution_config = ExecutionConfig(dbt_executable_path="/home/airflow/dbt_venv/bin/dbt")

meu_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    schedule='0 */8 * * *',
    start_date=datetime(2026,1,1),
    catchup=False,
    dag_id="dbt_cosmos_meteoblue",
)
