# syntax=docker/dockerfile:1.7
FROM apache/airflow:3.3.2-python3.11
# The pipeline is installed into Airflow's own Python, so DAG files import its jobs and run them as Python tasks.
COPY --chown=airflow pipelines/pyproject.toml /opt/pipelines/pyproject.toml
COPY --chown=airflow pipelines/src /opt/pipelines/src
RUN python -m pip install --no-cache-dir "apache-airflow==3.3.2" \
    apache-airflow-providers-fab==3.9.0 apache-airflow-providers-standard==1.19.0 /opt/pipelines \
    && python -m pip check
WORKDIR /opt/project
