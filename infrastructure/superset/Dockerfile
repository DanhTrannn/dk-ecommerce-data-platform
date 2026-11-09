# syntax=docker/dockerfile:1.7
FROM apache/superset:6.1.0
USER root
COPY infrastructure/superset/requirements.lock /app/project/requirements.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --python /app/.venv/bin/python --require-hashes -r /app/project/requirements.lock
COPY infrastructure/superset/*.py /app/project/
ENV SUPERSET_CONFIG_PATH=/app/project/superset_config.py \
    PYTHONPATH=/app/project
USER superset
