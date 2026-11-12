# syntax=docker/dockerfile:1.7
FROM python:3.11.13-slim AS pipeline-runtime
COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /bin/uv
ENV UV_PYTHON_DOWNLOADS=never UV_PROJECT_ENVIRONMENT=/opt/pipeline-venv UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY generator/pyproject.toml generator/pyproject.toml
COPY services/platform-app/pyproject.toml services/platform-app/pyproject.toml
COPY pipelines/pyproject.toml pipelines/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-workspace --package batch-pipeline
COPY pipelines/src pipelines/src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --package batch-pipeline

FROM apache/airflow:3.3.2-python3.11
USER root
COPY --from=pipeline-runtime /opt/pipeline-venv /opt/pipeline-venv
USER airflow
RUN python -m pip install --no-cache-dir "apache-airflow==3.3.2" \
    apache-airflow-providers-fab==3.9.0 apache-airflow-providers-standard==1.19.0
ENV PIPELINE_PYTHON=/opt/pipeline-venv/bin/python
WORKDIR /opt/project
