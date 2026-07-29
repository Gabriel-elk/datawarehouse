#docker compose up -d postgres-airflow postgres (bancos primeiro, espera uns 10s)
#docker compose up airflow-init (sem -d, pra ver ele migrar e criar o user; ele roda e para sozinho, é normal)
#docker compose up -d (agora sim o resto)
#dbt-core e apache airflow se o dbt for ionstalado com pip_aditiona_requirements (ele dá error por conflito de pacotes tipo jinja2 pyyaml e etc)
# --upgrade pip setuptools wheel (atualizar as ferramentas pip para subir o dbt core e o dbt popstgres eles pedem isso para não dar erro de versão)

 --- o ajuste que foi feito no dockerfile fez com que eu tivesse que alterar um path dentro do inspec dos arquivos de configuração do airfow init no docker eu localizei ele
 #docker compose run --rm airflow-webserver airflow users create --username admin --password admin --firstname Air --lastname Flow --role Admin --email admin@example.com (CASO DE ERRO NA CRIAÇÃO DE USUÁRIO)