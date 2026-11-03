# syntax=docker/dockerfile:1.7
FROM cgr.dev/chainguard/minio-client:latest-dev AS client
FROM python:3.11-slim
ARG BOTO3_VERSION=1.35.99
RUN pip install --no-cache-dir "boto3==${BOTO3_VERSION}"
COPY --from=client /usr/bin/mc /usr/local/bin/mc
RUN useradd --create-home --uid 65532 minio-init
USER minio-init
