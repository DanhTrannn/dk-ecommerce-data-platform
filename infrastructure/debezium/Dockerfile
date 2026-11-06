# syntax=docker/dockerfile:1.7
FROM curlimages/curl:8.17.0 AS plugin
ARG DEBEZIUM_VERSION=3.0.8.Final

RUN mkdir -p /tmp/debezium-postgres \
    && curl --fail --location --retry 5 \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
      --output /tmp/debezium-postgres.tar.gz \
    && tar -xzf /tmp/debezium-postgres.tar.gz -C /tmp/debezium-postgres

FROM apache/kafka:3.8.0

COPY --from=plugin /tmp/debezium-postgres /opt/kafka/plugins/debezium-postgres
COPY infrastructure/debezium/connect-distributed.properties /opt/kafka/config/connect-distributed.properties

EXPOSE 8083

CMD ["/opt/kafka/bin/connect-distributed.sh", "/opt/kafka/config/connect-distributed.properties"]
