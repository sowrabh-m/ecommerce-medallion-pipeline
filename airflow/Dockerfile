FROM apache/airflow:3.0.2
RUN pip install --no-cache-dir --timeout=120 --retries=5 apache-airflow-providers-amazon apache-airflow-providers-snowflake
USER root
RUN python -m venv /opt/dbt_venv && chown -R airflow: /opt/dbt_venv
USER airflow
RUN /opt/dbt_venv/bin/pip install --no-cache-dir --timeout=120 --retries=5 dbt-core dbt-snowflake
