# syntax=docker/dockerfile:1.7
FROM curlimages/curl:8.17.0 AS pipeline-jars
ARG ICEBERG_VERSION=1.10.1
ARG POSTGRES_JDBC_VERSION=42.7.14
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG MAVEN_MIRROR=https://repo1.maven.org/maven2
RUN mkdir -p /tmp/jars \
    && curl --fail --location --retry 5 --retry-all-errors --retry-delay 5 \
      -C - --connect-timeout 30 \
      "${MAVEN_MIRROR}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      --output /tmp/jars/iceberg-spark-runtime.jar \
    && curl --fail --location --retry 5 --retry-all-errors --retry-delay 5 \
      -C - --connect-timeout 30 \
      "${MAVEN_MIRROR}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      --output /tmp/jars/iceberg-aws-bundle.jar \
    && curl --fail --location --retry 5 --retry-all-errors --retry-delay 5 \
      -C - --connect-timeout 30 \
      "${MAVEN_MIRROR}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      --output /tmp/jars/postgresql.jar \
    && curl --fail --location --retry 5 --retry-all-errors --retry-delay 5 \
      -C - --connect-timeout 30 \
      "${MAVEN_MIRROR}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      --output /tmp/jars/hadoop-aws.jar \
    && curl --fail --location --retry 5 --retry-all-errors --retry-delay 5 \
      -C - --connect-timeout 30 \
      "${MAVEN_MIRROR}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      --output /tmp/jars/aws-java-sdk-bundle.jar

FROM apache/airflow:3.3.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
COPY --from=pipeline-jars /tmp/jars/*.jar /opt/spark/jars/

WORKDIR /opt/project
USER airflow
RUN python -m pip install --no-cache-dir "apache-airflow==3.3.2" pyspark==3.5.9 pandas==2.2.3 \
        apache-airflow-providers-apache-spark==5.6.0 \
        apache-airflow-providers-fab==3.9.0 \
        apache-airflow-providers-standard==1.19.0 \
    && cp /opt/spark/jars/*.jar "$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')/"
