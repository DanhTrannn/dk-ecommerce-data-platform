# syntax=docker/dockerfile:1.7
FROM python:3.11.13-slim AS pipeline-runtime
COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /bin/uv
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/pipeline-venv POLARS_MAX_THREADS=2 \
    PYTHONPATH=/app/pipelines/src PATH="/opt/pipeline-venv/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY generator/pyproject.toml generator/pyproject.toml
COPY services/platform-app/pyproject.toml services/platform-app/pyproject.toml
COPY pipelines/pyproject.toml pipelines/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-workspace --package batch-pipeline
COPY pipelines/src pipelines/src
COPY pipelines/dbt pipelines/dbt
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --package batch-pipeline
USER 185:185
ENTRYPOINT ["python"]
