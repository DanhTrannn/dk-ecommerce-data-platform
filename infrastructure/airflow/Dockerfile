# syntax=docker/dockerfile:1.7
FROM apache/airflow:3.3.2-python3.11
# The pipeline is installed into Airflow's own Python, so DAG files import its jobs and run them as Python tasks.
COPY --chown=airflow airflow/pyproject.toml /opt/pipeline/pyproject.toml
COPY --chown=airflow airflow/lakehouse /opt/pipeline/lakehouse
RUN python -m pip install --no-cache-dir "apache-airflow==3.3.2" \
    apache-airflow-providers-fab==3.9.0 apache-airflow-providers-standard==1.19.0 /opt/pipeline \
    && python -m pip check
WORKDIR /opt/project
