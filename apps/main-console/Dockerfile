# syntax=docker/dockerfile:1.7
FROM python:3.11.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH" \
    GENERATOR_CONSOLE_REPO_ROOT=/app \
    GENERATOR_CONFIGS_DIR=/app/generator/configs \
    GENERATOR_PROFILE_PATH=/app/generator/profiles/uci-online-retail-uk-v1.json \
    GENERATOR_CONSOLE_DATA_DIR=/data/generator/jobs

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./
COPY services/ecommerce-api/pyproject.toml services/ecommerce-api/pyproject.toml
COPY generator/pyproject.toml generator/pyproject.toml
COPY pipelines/pyproject.toml pipelines/pyproject.toml
COPY apps/main-console/pyproject.toml apps/main-console/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package generator-console

COPY apps/main-console/src apps/main-console/src
COPY generator/src generator/src
COPY generator/configs generator/configs
COPY generator/profiles generator/profiles
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable \
        --reinstall-package generator-console \
        --reinstall-package data-generator \
        --package generator-console

EXPOSE 8501
CMD ["generator-console", "--server.address=0.0.0.0", "--server.port=8501"]
