FROM apache/airflow:2.10.5

RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    requests \
    python-dotenv \
    apache-airflow-providers-postgres \
    apache-airflow-providers-mysql \
    apache-airflow-providers-oracle \
    apache-airflow-providers-microsoft-mssql \
    psycopg2-binary \
    pandas \
    astronomer-cosmos \
    --constraint "/home/airflow/constraints.txt"

# grupo 2: dbt num venv isolado
USER airflow
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.11.11 dbt-postgres==1.10.0