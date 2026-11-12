"""Native Iceberg maintenance with publication-aware retention and bounded orphan scope."""

import json
import os
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import boto3
from pyiceberg.expressions import LessThan

from lakehouse.oltp.gold.schema import TABLES
from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.postgres import namespace_lock

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
        *(f"silver.{t.silver_table}" for t in SOURCE_TABLES),
        *(f"gold.{name}" for name in TABLES),
    ],
}
TABLE_PROFILES["all"] = list(dict.fromkeys(TABLE_PROFILES["streaming"] + TABLE_PROFILES["oltp"]))


def protected_snapshots(catalog, prefix):
    protected = {}
    for name in ("analytics_publications", "logs_analytics_publications"):
        key = (ns(prefix, "system"), name)
        if not catalog.table_exists(key):
            continue
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
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_BATCH_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_BATCH_SECRET_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
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


def maintain_tables(catalog, connection, args):
    if (
        args.retain_snapshots < 1
        or args.older_than_days < 1
        or args.quarantine_retention_days < 1
        or args.target_file_size_bytes < 1
    ):
        raise ValueError("Maintenance retention and target size must be positive")
    if args.delete_orphans and not args.writers_stopped:
        raise ValueError("Physical orphan deletion requires --writers-stopped; schedules only report candidates")
    prefix = args.namespace_prefix
    cutoff = datetime.now(UTC) - timedelta(days=args.older_than_days)
    with namespace_lock(connection, prefix):
        protected = protected_snapshots(catalog, prefix)
        if args.action == "quarantine":
            key = (ns(prefix, "quarantine"), "silver_oltp_violations")
            if catalog.table_exists(key):
                catalog.load_table(key).delete(
                    LessThan("_quarantined_at", datetime.now(UTC) - timedelta(days=args.quarantine_retention_days))
                )
            return {"quarantine_cleanup": True}
        names = args.tables.split(",") if args.tables else TABLE_PROFILES[args.profile or "all"]
        result = {}
        for name in names:
            parts = name.strip().split(".")
            if len(parts) == 3 and parts[0] == "lakehouse":
                parts = parts[1:]
            if len(parts) != 2 or ".".join(parts) not in TABLE_PROFILES["all"]:
                raise ValueError("Maintenance table must belong to the configured whitelist")
            key = (ns(prefix, parts[0]), parts[1])
            physical = "lakehouse." + ".".join(key)
            if not catalog.table_exists(key):
                result[physical] = {"status": "not-created"}
                continue
            table = catalog.load_table(key)
            outcome = {}
            if args.action in {"all", "compact"}:
                outcome.update(compact(table, args.target_file_size_bytes))
            if args.action in {"all", "expire"}:
                expired = expiry_candidates(table, physical, protected, retain=args.retain_snapshots, cutoff=cutoff)
                if expired:
                    table.maintenance.expire_snapshots().by_ids(expired).commit()
                outcome["expired_snapshots"] = len(expired)
            if args.action in {"all", "orphan"}:
                outcome.update(orphan_files(table, cutoff=cutoff, delete=args.delete_orphans))
            result[physical] = outcome
        return result
