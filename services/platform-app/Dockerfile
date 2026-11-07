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
COPY generator/pyproject.toml generator/pyproject.toml
COPY pipelines/pyproject.toml pipelines/pyproject.toml
COPY services/platform-app/pyproject.toml services/platform-app/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package platform-app

COPY services/platform-app/src services/platform-app/src
COPY database database
COPY services/platform-app/start.sh /app/start.sh
COPY infrastructure/polaris/rbac.yml /app/infrastructure/polaris/rbac.yml
COPY generator/src generator/src
COPY generator/configs generator/configs
COPY generator/profiles generator/profiles
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable \
        --reinstall-package platform-app \
        --reinstall-package data-generator \
        --package platform-app

EXPOSE 8000 8501
CMD ["bash", "/app/start.sh"]
