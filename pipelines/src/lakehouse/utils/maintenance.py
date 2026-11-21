"""Native Iceberg maintenance with log-publication-aware retention and bounded orphan scope."""

import json
import os
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import boto3

from lakehouse.analytics.dbt import project_directory
from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.oltp.sources import SOURCE_TABLES
from lakehouse.utils.postgres import LOGS_LOCK, writer_lock

TABLE_PROFILES = {
    "streaming": [
        "landing.access_logs",
        "bronze.web_events",
        "silver.silver_logs",
        "gold.fact_web_events",
        "gold.mart_hourly_route_metrics",
        "gold.mart_daily_product_demand",
        "gold.mart_marketing_funnel_daily",
    ],
    "oltp": [
        *(f"bronze.{t.name}" for t in SOURCE_TABLES),
        *(f"silver.{t.name}" for t in SOURCE_TABLES),
        # dbt builds one Gold table per OLTP model and per seed.
        *(f"gold.{path.stem}" for path in sorted((project_directory() / "models" / "oltp").glob("*.sql"))),
        *(f"gold.{path.stem}" for path in sorted((project_directory() / "seeds").glob("*.csv"))),
    ],
}
TABLE_PROFILES["all"] = list(dict.fromkeys(TABLE_PROFILES["streaming"] + TABLE_PROFILES["oltp"]))


def protected_snapshots(catalog):
    protected = {}
    key = ("system", "logs_analytics_publications")
    if not catalog.table_exists(key):
        return protected
    table = catalog.load_table(key)
    for row in read_snapshot(table, capture(table)).to_dicts():
        for field in ("snapshots_json", "source_snapshots_json"):
            for physical, snapshot in json.loads(row.get(field) or "{}").items():
                if snapshot is not None:
                    if type(snapshot) is not int:
                        raise ValueError("Invalid publication snapshot reference")
                    protected.setdefault(physical, set()).add(snapshot)
    return protected


def expiry_candidates(table, physical, protected, *, retain, cutoff):
    snapshots = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms, reverse=True)
    keep = {s.snapshot_id for s in snapshots[:retain]}
    keep.update(protected.get(physical, set()))
    keep.update(ref.snapshot_id for ref in table.metadata.refs.values())
    return [
        s.snapshot_id
        for s in snapshots
        if s.snapshot_id not in keep and s.timestamp_ms < int(cutoff.timestamp() * 1000)
    ]


def compact(table, target_size):
    files = table.inspect.files()
    if len(files) < 2 and (not len(files) or not any(files["content"].to_pylist())):
        return {"rewritten": False}
    # The transaction starts before the read; concurrent append changes main and fails its snapshot assertion.
    with table.transaction() as transaction:
        data = read_snapshot(table, capture(table)).to_arrow()
        transaction.set_properties(
            {"write.target-file-size-bytes": str(target_size), "commit.manifest-merge.enabled": "true"}
        )
        transaction.overwrite(data)
    return {"rewritten": True, "rows": len(data)}


def orphan_files(table, *, cutoff, delete=False):
    location = urlsplit(table.location())
    bucket = os.environ["MINIO_LAKEHOUSE_BUCKET"]
    if location.scheme not in {"s3", "s3a"} or location.netloc != bucket:
        raise ValueError("Orphan cleanup is limited to the configured lakehouse bucket")
    prefix = location.path.strip("/") + "/data/"
    if prefix == "data/" or ".." in prefix.split("/"):
        raise ValueError("Refuse an unscoped orphan scan")
    live = {urlsplit(path).path.lstrip("/") for path in table.inspect.all_files()["file_path"].to_pylist()}
    # The credentials Polaris vended for this table only reach its own location, so the scan cannot leave it.
    vended = table.io.properties
    client = boto3.client(
        "s3",
        endpoint_url=vended["s3.endpoint"],
        aws_access_key_id=vended["s3.access-key-id"],
        aws_secret_access_key=vended["s3.secret-access-key"],
        aws_session_token=vended["s3.session-token"],
        region_name=vended.get("client.region", "us-east-1"),
    )
    candidates = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        candidates.extend(
            item["Key"]
            for item in page.get("Contents", [])
            if item["Key"] not in live and item["LastModified"] < cutoff
        )
    before = table.metadata_location
    table.refresh()
    if table.metadata_location != before:
        raise RuntimeError("Table changed during orphan scan; retry with writers stopped")
    if delete:
        for start in range(0, len(candidates), 1000):
            response = client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": key} for key in candidates[start : start + 1000]]}
            )
            if response.get("Errors"):
                raise RuntimeError("Object storage rejected orphan deletion")
    return {
        "orphan_candidates": len(candidates),
        "deleted": len(candidates) if delete else 0,
        "scope": "data-files-only",
    }


def maintenance_lock_key(profile):
    """OLTP tables are only written by the OLTP DAG, which runs its maintenance last; a separate key keeps the
    hourly log job, which holds the logs key, from failing on it."""
    return "lakehouse:oltp" if profile == "oltp" else LOGS_LOCK


def maintain_tables(
    catalog,
    connection,
    *,
    profile=None,
    tables=None,
    action="all",
    retain_snapshots=20,
    target_file_size_bytes=128 * 1024 * 1024,
    older_than_days=3,
    delete_orphans=False,
    writers_stopped=False,
):
    if retain_snapshots < 1 or older_than_days < 1 or target_file_size_bytes < 1:
        raise ValueError("Maintenance retention and target size must be positive")
    if delete_orphans and not writers_stopped:
        raise ValueError("Physical orphan deletion requires --writers-stopped; schedules only report candidates")
    cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
    with writer_lock(connection, maintenance_lock_key(profile)):
        protected = protected_snapshots(catalog)
        names = tables.split(",") if tables else TABLE_PROFILES[profile or "all"]
        result = {}
        for name in names:
            parts = name.strip().split(".")
            if len(parts) == 3 and parts[0] == "lakehouse":
                parts = parts[1:]
            if len(parts) != 2 or ".".join(parts) not in TABLE_PROFILES["all"]:
                raise ValueError("Maintenance table must belong to the configured whitelist")
            key = (parts[0], parts[1])
            physical = "lakehouse." + ".".join(key)
            if not catalog.table_exists(key):
                result[physical] = {"status": "not-created"}
                continue
            table = catalog.load_table(key)
            outcome = {}
            if action in {"all", "compact"}:
                outcome.update(compact(table, target_file_size_bytes))
            if action in {"all", "expire"}:
                expired = expiry_candidates(table, physical, protected, retain=retain_snapshots, cutoff=cutoff)
                if expired:
                    table.maintenance.expire_snapshots().by_ids(expired).commit()
                outcome["expired_snapshots"] = len(expired)
            if action in {"all", "orphan"}:
                outcome.update(orphan_files(table, cutoff=cutoff, delete=delete_orphans))
            result[physical] = outcome
        return result
