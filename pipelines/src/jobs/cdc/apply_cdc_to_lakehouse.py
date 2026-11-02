"""Persistent Spark micro-batches: Bronze CDC -> Silver -> published Gold.

The publication contains transport offsets and exact Iceberg snapshot IDs.
Readers only use completed publications. A failed batch is replayed; per-key
watermarks make Silver mutations idempotent, including physical deletes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from lakehouse.config import load_config
from lakehouse.oltp import gold
from lakehouse.oltp.cdc import apply_table, curate_rows, ensure_cdc_state, source_columns
from lakehouse.oltp.gold_ddl import GOLD_TABLE_DDL, ensure_oltp_gold_tables
from lakehouse.oltp.quality import QualityCheckResult, persist_quality_results
from lakehouse.oltp.silver_ddl import ensure_silver_tables
from lakehouse.spark import spark_session
from lakehouse.writer import oltp_writer_lock

log = logging.getLogger("lakehouse.cdc.apply")
BRONZE = "lakehouse.bronze.oltp_cdc_events"
PUBLICATIONS = "lakehouse.system.analytics_publications"
HEARTBEAT = "lakehouse.system.cdc_pipeline_state"
SOURCE_CURSOR_KEY = "__mysql_binlog_cursor__"
EVENT_IDENTITY = [
    "topic",
    "kafka_partition",
    "kafka_offset",
    "transaction_id",
    "binlog_file",
    "binlog_position",
    "binlog_row",
    "operation",
    "raw_payload",
]
BOOTSTRAP_CUTOFF = datetime.now(timezone.utc).replace(tzinfo=None)


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def ensure_tables(spark):
    ensure_silver_tables(spark)
    if "shipping_region" not in spark.table("lakehouse.silver.silver_orders").columns:
        spark.sql("ALTER TABLE lakehouse.silver.silver_orders ADD COLUMN shipping_region STRING")
    ensure_oltp_gold_tables(spark)
    ensure_cdc_state(spark)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {PUBLICATIONS} (
        run_id STRING, published_at TIMESTAMP, source_event_at TIMESTAMP,
        snapshots_json STRING, offsets_json STRING, changed_tables_json STRING
    ) USING iceberg TBLPROPERTIES ('format-version'='2')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {HEARTBEAT} (
        consumer_id STRING, status STRING, checked_at TIMESTAMP,
        pending_events BIGINT, pending_since TIMESTAMP, detail STRING
    ) USING iceberg TBLPROPERTIES ('format-version'='2')""")


def heartbeat(spark, status, pending=0, pending_since=None, detail=""):
    since = f"TIMESTAMP {quote(pending_since)}" if pending_since else "CAST(NULL AS TIMESTAMP)"
    spark.sql(f"""MERGE INTO {HEARTBEAT} t USING (
        SELECT 'oltp-cdc' consumer_id, {quote(status)} status, current_timestamp() checked_at,
        CAST({int(pending)} AS BIGINT) pending_events, {since} pending_since,
        {quote(detail[:400])} detail
    ) s ON t.consumer_id=s.consumer_id
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def latest_publication(spark):
    rows = spark.sql(f"SELECT * FROM {PUBLICATIONS} ORDER BY published_at DESC LIMIT 1").collect()
    return rows[0].asDict() if rows else None


def verify_bootstrap(spark, cfg):
    """Legacy Bronze predates transaction-END capture; require full source parity.

    This one-time read is source reconciliation, never an API fallback. A live
    source change during comparison fails closed and is retried next batch.
    """
    from urllib.parse import unquote, urlsplit

    url = urlsplit(os.environ["MYSQL_ECOMMERCE_READER_URL"])
    jdbc = f"jdbc:mysql://{url.hostname}:{url.port or 3306}{url.path}?serverTimezone=UTC"
    for table in cfg.tables:
        source = (
            spark.read.format("jdbc")
            .option("url", jdbc)
            .option("dbtable", table.name)
            .option("user", unquote(url.username or ""))
            .option("password", unquote(url.password or ""))
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )
        source = curate_rows(source, table)
        target = spark.table(f"lakehouse.silver.{table.silver_table}")
        fields = [f for f in target.schema.fields if not f.name.startswith("_")]
        source = source.select(
            *[
                F.col(f.name).cast(f.dataType).alias(f.name)
                if f.name in source.columns
                else F.lit(None).cast(f.dataType).alias(f.name)
                for f in fields
            ]
        )
        names = [f.name for f in fields]
        if (
            source.exceptAll(target.select(*names)).limit(1).count()
            or target.select(*names).exceptAll(source).limit(1).count()
        ):
            raise ValueError(f"Bootstrap source parity failed for {table.name}; no publication/checkpoint advanced")
        log.info("Bootstrap source parity: %s", table.name)


def refresh_gold(spark, changed, run_id, initial=False):
    """Rebuild affected tables, preserving daily inventory history separately.

    Whole-table replacement deliberately removes groups that disappeared after
    deletes. This is table-level incremental refresh, not a partition optimizer.
    """

    def silver(name):
        return spark.table(f"lakehouse.silver.silver_{name}")

    def fact(name):
        return spark.table(f"lakehouse.gold.{name}")

    today = datetime.now(timezone.utc).date().isoformat()
    definitions = [
        ("dim_date", set(), lambda: gold.build_dim_date(spark)),
        (
            "dim_product",
            {"products", "categories"},
            lambda: gold.build_dim_product(silver("products"), silver("categories"), run_id),
        ),
        (
            "dim_variant",
            {"product_variants", "products"},
            lambda: gold.build_dim_variant(silver("product_variants"), silver("products"), run_id),
        ),
        ("dim_store", {"stores", "cities"}, lambda: gold.build_dim_store(silver("stores"), silver("cities"), run_id)),
        (
            "dim_delivery_staff",
            {"delivery_staff", "cities"},
            lambda: gold.build_dim_delivery_staff(silver("delivery_staff"), silver("cities"), run_id),
        ),
        (
            "dim_customer",
            {"customers", "cities"},
            lambda: gold.build_dim_customer(silver("customers"), silver("cities"), run_id),
        ),
        (
            "fact_order",
            {"orders", "order_items", "product_variants"},
            lambda: gold.build_fact_order(silver("orders"), silver("order_items"), run_id, silver("product_variants")),
        ),
        (
            "fact_order_item",
            {"orders", "order_items", "product_variants"},
            lambda: gold.build_fact_order_item(
                silver("order_items"), silver("orders"), run_id, silver("product_variants")
            ),
        ),
        ("fact_shipment", {"shipments"}, lambda: gold.build_fact_shipment(silver("shipments"), run_id)),
        (
            "fact_return_exchange",
            {"return_requests", "return_items"},
            lambda: gold.build_fact_return_exchange(silver("return_requests"), silver("return_items"), run_id),
        ),
        (
            "fact_inventory_daily_snapshot",
            {"inventory", "store_inventory", "product_variants"},
            lambda: gold.build_fact_inventory_daily_snapshot(
                silver("inventory"), silver("store_inventory"), silver("product_variants"), today, run_id
            ),
        ),
        (
            "fact_inventory_current",
            {"inventory", "store_inventory", "product_variants"},
            lambda: gold.build_fact_inventory_daily_snapshot(
                silver("inventory"), silver("store_inventory"), silver("product_variants"), today, run_id
            ),
        ),
        (
            "mart_sales_daily",
            {"fact_order", "fact_order_item", "dim_product"},
            lambda: gold.build_mart_sales_daily(
                fact("fact_order"), fact("fact_order_item"), fact("dim_product"), run_id
            ),
        ),
        (
            "mart_logistics_performance",
            {"fact_shipment", "dim_delivery_staff"},
            lambda: gold.build_mart_logistics_performance(fact("fact_shipment"), fact("dim_delivery_staff"), run_id),
        ),
        (
            "mart_product_returns",
            {"fact_return_exchange", "dim_variant"},
            lambda: gold.build_mart_product_returns(fact("fact_return_exchange"), fact("dim_variant"), run_id),
        ),
        (
            "mart_inventory_health",
            {"fact_inventory_daily_snapshot", "dim_product"},
            lambda: gold.build_mart_inventory_health(
                fact("fact_inventory_daily_snapshot"), fact("dim_product"), run_id
            ),
        ),
    ]
    refreshed = set()
    for name, dependencies, build in definitions:
        if not initial and not dependencies.intersection(changed | refreshed):
            continue
        target = f"lakehouse.gold.{name}"
        df = build().select(*spark.table(target).columns)
        if name == "fact_inventory_daily_snapshot":
            # Replacing today's partition also clears it when all balances were deleted.
            df.writeTo(target).overwrite(F.col("snapshot_date") == F.lit(today).cast("date"))
        else:
            df.writeTo(target).overwrite(F.lit(True))
        refreshed.add(name)
        log.info("Refreshed %s", target)
    return refreshed


def validate_publication(spark, cfg, columns, changed, refreshed, run_id):
    results = []
    # Validate real references from the source DDL against the current typed
    # state. Credentials are excluded from this analytical catalogue.
    import re

    ddl = columns
    for table in cfg.tables:
        body = ddl[table.name]
        for child, parent, pk in re.findall(r"FOREIGN KEY \(`([^`]+)`\) REFERENCES `([^`]+)` \(`([^`]+)`\)", body):
            if cfg.table(parent) is None:
                continue
            # Curated Silver intentionally excludes operational audit fields.
            if child not in spark.table(f"lakehouse.silver.{table.silver_table}").columns:
                continue
            rows = (
                spark.sql(f"""SELECT count(*) n FROM lakehouse.silver.{table.silver_table} c
                LEFT JOIN lakehouse.silver.{cfg.table(parent).silver_table} p ON c.`{child}`=p.`{pk}`
                WHERE c.`{child}` IS NOT NULL AND p.`{pk}` IS NULL""")
                .first()
                .n
            )
            results.append(
                QualityCheckResult(
                    f"fk_{table.name}_{child}",
                    table.silver_table,
                    str(rows),
                    "0",
                    rows == 0,
                    "CDC reference integrity",
                    failed_rows=rows,
                )
            )
    if "fact_order" in refreshed:
        row = spark.sql("""SELECT
            (SELECT count(*) FROM lakehouse.silver.silver_orders) source_count,
            (SELECT count(*) FROM lakehouse.gold.fact_order) target_count,
            (SELECT count(DISTINCT order_id) FROM lakehouse.gold.fact_order) unique_count""").first()
        passed = row.source_count == row.target_count == row.unique_count
        results.append(
            QualityCheckResult(
                "order_count_and_key",
                "fact_order",
                str(row.target_count),
                str(row.source_count),
                passed,
                "Source count and Gold unique order key",
            )
        )
    if "fact_inventory_current" in refreshed:
        row = spark.sql("""SELECT count(*) n, count(DISTINCT (location_type, location_id, variant_key)) keys,
            count_if(on_hand_quantity < 0) negatives FROM lakehouse.gold.fact_inventory_current""").first()
        results.append(
            QualityCheckResult(
                "current_inventory_keys",
                "fact_inventory_current",
                str(row.n),
                str(row.keys),
                row.n == row.keys and row.negatives == 0,
                "One current balance per location/variant",
            )
        )
    if results:
        persist_quality_results(
            spark,
            results,
            run_id=run_id,
            batch_date=datetime.now(timezone.utc).date().isoformat(),
            phase="cdc_publication",
        )
    if any(not result.passed for result in results):
        raise ValueError("CDC publication failed quality checks; previous publication remains active")


def _with_source_order(events):
    return (
        events.withColumn("_file_number", F.regexp_extract("binlog_file", r"(\d+)$", 1).cast("long"))
        .withColumn("_source_position", F.coalesce(F.col("binlog_position"), F.lit(0)))
        .withColumn("_source_row", F.coalesce(F.col("binlog_row"), F.lit(0)))
        .withColumn("_source_order", F.struct("_file_number", "_source_position", "_source_row"))
        .withColumn("_source_uuid", F.element_at(F.split(F.col("transaction_id"), ":"), 1))
    )


def _source_checkpoint(events):
    ordered = (
        _with_source_order(events).filter(F.col("transaction_id").isNotNull()).orderBy(F.col("_source_order").desc())
    )
    row = ordered.select("_file_number", "_source_position", "_source_row", "_source_uuid").first()
    if row is None or row._file_number is None:
        return None
    return {
        "file_number": int(row._file_number),
        "position": int(row._source_position),
        "row": int(row._source_row),
        "source_uuid": row._source_uuid,
    }


def process_once(spark, cfg, types, ddl):
    publication = latest_publication(spark)
    offsets = json.loads(publication["offsets_json"]) if publication else {}
    prior_cursor = offsets.get(SOURCE_CURSOR_KEY)
    all_events = spark.table(BRONZE).dropDuplicates(EVENT_IDENTITY)
    source_rows = all_events.filter(F.col("operation").isin("r", "c", "u", "d"))
    if publication and prior_cursor is None:
        # Migrate publications made before source-position cursors existed.
        prior_cursor = _source_checkpoint(
            source_rows.filter(F.col("event_ts") <= F.lit(publication["source_event_at"]))
        )

    predicate = F.lit(False)
    for topic, partition in [
        (r.topic, r.kafka_partition) for r in all_events.select("topic", "kafka_partition").distinct().collect()
    ]:
        key = f"{topic}:{partition}"
        predicate = predicate | (
            (F.col("topic") == topic)
            & (F.col("kafka_partition") == partition)
            & (F.col("kafka_offset") > offsets.get(key, -1))
        )
    transport_batch = all_events.filter(predicate)

    if prior_cursor:
        cursor = F.struct(
            F.lit(int(prior_cursor["file_number"])).cast("long").alias("_file_number"),
            F.lit(int(prior_cursor["position"])).cast("long").alias("_source_position"),
            F.lit(int(prior_cursor["row"])).cast("int").alias("_source_row"),
        )
        source_ordered = _with_source_order(source_rows)
        source_after = source_ordered.filter(
            (F.col("_source_order") > cursor) | (F.col("event_ts") > F.lit(publication["source_event_at"]))
        )
        source_candidates = (
            source_ordered.join(transport_batch.select(*EVENT_IDENTITY), EVENT_IDENTITY, "inner")
            .unionByName(source_after.select(*source_ordered.columns))
            .dropDuplicates(EVENT_IDENTITY)
        )
        # A changed GTID UUID or a backwards binlog position means the source
        # lineage reset. Never silently mix epochs or advance a stale offset.
        latest = _source_checkpoint(source_candidates)
        source_uuids = {
            row._source_uuid
            for row in _with_source_order(source_candidates)
            .filter(F.col("_source_uuid").isNotNull())
            .select("_source_uuid")
            .distinct()
            .collect()
        }
        if prior_cursor.get("source_uuid") and any(uuid != prior_cursor["source_uuid"] for uuid in source_uuids):
            raise ValueError("MySQL GTID source UUID changed; stop writers and perform a controlled CDC rebootstrap")
        if latest and latest["file_number"] < prior_cursor["file_number"]:
            raise ValueError("MySQL binlog file moved backwards; stop writers and perform a controlled CDC rebootstrap")
    else:
        source_candidates = source_rows.join(transport_batch.select(*EVENT_IDENTITY), EVENT_IDENTITY, "inner")

    transaction_ids = source_candidates.filter(F.col("transaction_id").isNotNull()).select("transaction_id").distinct()
    transaction_metadata = all_events.filter(F.col("operation").isin("BEGIN", "END")).join(
        transaction_ids, "transaction_id", "left_semi"
    )
    batch = (
        transport_batch.unionByName(source_candidates, allowMissingColumns=True)
        .unionByName(transaction_metadata, allowMissingColumns=True)
        .dropDuplicates(EVENT_IDENTITY)
        .persist()
    )
    try:
        pending = batch.count()
        if not pending and publication:
            heartbeat(spark, "idle")
            return
        data = batch.filter(F.col("operation").isin("r", "c", "u", "d"))
        transactions = data.filter(F.col("transaction_id").isNotNull()).select("transaction_id").distinct()
        # END may land in a later Iceberg snapshot. Do not acknowledge a partial
        # MySQL transaction; retry the entire bounded batch on the next tick.
        if transactions.limit(1).count():
            counts = (
                all_events.filter(F.col("operation").isin("c", "u", "d"))
                .join(transactions, "transaction_id")
                .groupBy("transaction_id")
                .agg(F.countDistinct(F.get_json_object("raw_payload", "$.transaction.total_order")).alias("seen"))
            )
            ends = (
                all_events.filter(F.col("operation") == "END")
                .select(
                    "transaction_id", F.get_json_object("raw_payload", "$.event_count").cast("long").alias("expected")
                )
                .groupBy("transaction_id")
                .agg(F.max("expected").alias("expected"))
            )
            incomplete = counts.join(ends, "transaction_id", "left").filter(
                F.col("expected").isNull() | (F.col("seen") < F.col("expected"))
            )
            # Before END capture was introduced, old Kafka metadata may have
            # expired. Only the initial, fully source-verified bootstrap may
            # accept such history. New/in-flight transactions always wait.
            incomplete = incomplete.join(
                data.groupBy("transaction_id").agg(F.max("event_ts").alias("last_event")), "transaction_id"
            )
            blocking = incomplete if publication else incomplete.filter(F.col("last_event") >= F.lit(BOOTSTRAP_CUTOFF))
            if blocking.limit(1).count():
                since = data.agg(F.min("event_ts")).first()[0]
                heartbeat(
                    spark, "waiting_transaction", pending, since, "Waiting for transaction END and all captured rows"
                )
                return
        if publication is None:
            snapshots = all_events.filter(F.col("operation") == "r")
            if (
                snapshots.limit(1).count()
                and not snapshots.filter(F.get_json_object("raw_payload", "$.source.snapshot") == "last")
                .limit(1)
                .count()
            ):
                heartbeat(
                    spark, "waiting_snapshot", pending, detail="Waiting for the initial snapshot completion marker"
                )
                return
        changed = {row.table_name for row in data.select("table_name").distinct().collect()}
        unknown = changed - {table.name for table in cfg.tables}
        if unknown:
            raise ValueError(f"CDC table outside allowlist: {sorted(unknown)}")
        run_id = "cdc-" + uuid.uuid4().hex
        heartbeat(spark, "applying", pending, data.agg(F.min("event_ts")).first()[0] if changed else None)
        for table in cfg.tables:
            if table.name in changed:
                count = apply_table(
                    spark, table, data.filter(F.col("table_name") == table.name), types[table.name], run_id
                )
                log.info("Silver %s: applied %s keys", table.name, count)
        if publication is None:
            verify_bootstrap(spark, cfg)
        refreshed = refresh_gold(spark, changed, run_id, initial=publication is None)
        validate_publication(spark, cfg, ddl, changed, refreshed, run_id)
        snapshots = json.loads(publication["snapshots_json"]) if publication else {}
        targets = {f"lakehouse.silver.{t.silver_table}" for t in cfg.tables if publication is None or t.name in changed}
        targets.update(f"lakehouse.gold.{name}" for name in (GOLD_TABLE_DDL if publication is None else refreshed))
        for target in sorted(targets):
            rows = spark.sql(
                f"SELECT snapshot_id FROM {target}.snapshots ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
            ).collect()
            snapshots[target] = rows[0].snapshot_id if rows else None
        for row in batch.groupBy("topic", "kafka_partition").agg(F.max("kafka_offset").alias("offset")).collect():
            key = f"{row.topic}:{row.kafka_partition}"
            offsets[key] = max(int(offsets.get(key, -1)), int(row.offset))
        current_cursor = _source_checkpoint(data) if changed else None
        if current_cursor:
            if prior_cursor and current_cursor["source_uuid"] != prior_cursor.get("source_uuid"):
                current_cursor["source_uuid"] = prior_cursor.get("source_uuid")
                if (current_cursor["file_number"], current_cursor["position"], current_cursor["row"]) < (
                    prior_cursor["file_number"],
                    prior_cursor["position"],
                    prior_cursor["row"],
                ):
                    current_cursor = prior_cursor
            offsets[SOURCE_CURSOR_KEY] = current_cursor
        source_at = data.agg(F.max("event_ts")).first()[0] if changed else None
        if publication and publication["source_event_at"] and source_at:
            source_at = max(source_at, publication["source_event_at"])
        elif not changed and publication:
            source_at = publication["source_event_at"]
        source_sql = f"TIMESTAMP {quote(source_at)}" if source_at else "CAST(NULL AS TIMESTAMP)"
        spark.sql(f"""INSERT INTO {PUBLICATIONS} SELECT {quote(run_id)}, current_timestamp(), {source_sql},
            {quote(json.dumps(snapshots))}, {quote(json.dumps(offsets))}, {quote(json.dumps(sorted(changed)))}""")
        heartbeat(spark, "idle")
        log.info(
            "Published %s: %s changed source tables, %s refreshed Gold tables", run_id, len(changed), len(refreshed)
        )
    finally:
        batch.unpersist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="/opt/project/pipelines/config/default.yml")
    parser.add_argument("--schema-path", default="/opt/project/database/schema.sql")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if os.environ.get("LAKEHOUSE_OLTP_MODE", "cdc") != "cdc":
        return
    logging.basicConfig(level=logging.INFO)
    cfg = load_config(args.config_path)
    types = source_columns(args.schema_path)
    import re

    ddl = dict(re.findall(r"CREATE TABLE `([^`]+)` \(\n(.*?)\n\) ENGINE=", open(args.schema_path).read(), re.S))
    spark = spark_session("oltp_cdc_materializer")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", os.environ.get("CDC_SHUFFLE_PARTITIONS", "2"))
    interval = max(5, int(os.environ.get("CDC_APPLY_INTERVAL_SECONDS", "30")))
    try:
        with oltp_writer_lock():
            ensure_tables(spark)
        while True:
            started = time.monotonic()
            try:
                with oltp_writer_lock():
                    process_once(spark, cfg, types, ddl)
            except Exception:
                log.exception("CDC application failed; publication checkpoint retained")
                heartbeat(spark, "failed", detail="CDC application failed; inspect driver logs and quality audit")
                if args.once:
                    raise
            if args.once:
                return
            time.sleep(max(1, interval - (time.monotonic() - started)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
