# syntax=docker/dockerfile:1.7
# Optional migration-pilot runtime; production Airflow/Spark images are unchanged.
FROM python:3.11.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /bin/uv
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy POLARS_MAX_THREADS=2 PYTHONPATH=/app/pipelines/src PATH="/app/.venv/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY generator/pyproject.toml generator/pyproject.toml
COPY services/platform-app/pyproject.toml services/platform-app/pyproject.toml
COPY pipelines/pyproject.toml pipelines/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-workspace --package batch-pipeline --extra polars
COPY pipelines/src pipelines/src
COPY pipelines/config pipelines/config
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --package batch-pipeline --extra polars
USER 185:185
ENTRYPOINT ["python"]
