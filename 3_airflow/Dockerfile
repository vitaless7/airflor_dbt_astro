FROM astrocrpublic.azurecr.io/runtime:3.3-7

RUN python -m venv /usr/local/airflow/dbt_venv

# 2. Instala o dbt-core e dbt-postgres dentro da venv criada
RUN /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-postgres
