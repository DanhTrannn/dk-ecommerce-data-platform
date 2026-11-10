"""Actual Spark-created Iceberg files and cross-engine verification in the private fixture."""

import json
import sys

from lakehouse.utils.spark import spark_session

spark = spark_session("polars_compatibility")
spark.sparkContext.setLogLevel("ERROR")
try:
    name = "lakehouse.polars_pilot_acceptance.spark_probe"
    if sys.argv[1] == "create":
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.polars_pilot_acceptance")
        spark.sql(f"""CREATE TABLE {name} (
            id BIGINT, public_id BINARY, amount DECIMAL(18,2), event_at TIMESTAMP, note STRING, active BOOLEAN
        ) USING iceberg PARTITIONED BY (days(event_at)) TBLPROPERTIES (
            'format-version'='2', 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read'
        )""")
        # One physical file in one partition forces a row-level positional delete.
        spark.sql("""SELECT CAST(id AS BIGINT) id, unhex(public_id) public_id,
            CAST(amount AS DECIMAL(18,2)) amount, CAST(event_at AS TIMESTAMP) event_at, note, active
            FROM VALUES
            (1, '00112233445566778899aabbccddeeff', 12.34, '2026-01-01 01:02:03.123456', CAST(NULL AS STRING), true),
            (2, CAST(NULL AS STRING), -5.25, '2026-01-01 02:00:00', 'Tiếng Việt', false)
            AS source(id,public_id,amount,event_at,note,active)
        """).coalesce(1).writeTo(name).append()
        spark.sql(f"DELETE FROM {name} WHERE id=2")
        result = {
            "spark_probe_rows": spark.table(name).count(),
            "delete_files": spark.sql(f"SELECT count(*) n FROM {name}.files WHERE content<>0").first().n,
        }
        assert result["delete_files"] > 0
    else:
        rows = spark.table("lakehouse.polars_pilot_acceptance.python_probe").collect()
        assert len(rows) == 1 and rows[0].id == 9 and str(rows[0].amount) == "99.50"
        assert rows[0].public_id == bytes.fromhex("00112233445566778899aabbccddeeff")
        assert rows[0].active is False and rows[0].note == "Tiếng Việt"
        assert spark.table("lakehouse.polars_pilot_acceptance.silver_categories").count() > 0
        result = {"spark_reads_python": True}
    print("STACK_RESULT " + json.dumps(result), flush=True)
finally:
    spark.stop()
