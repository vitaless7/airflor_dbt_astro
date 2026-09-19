import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# 1. Caminho para o diretório onde está o seu projeto dbt dentro do container
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/felipe_dw")

# 2. Configuração do perfil do Postgres no Cosmos
profile_config = ProfileConfig(
    profile_name="felipe_dw",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dw",
        profile_args={"schema": "public"},
    ),
)

# 3. Definição da DAG usando Cosmos
dbt_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        # Caminho para o dbt instalado na venv dentro do Docker
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
    ),
    dag_id="dbt_felipe_dw_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
)