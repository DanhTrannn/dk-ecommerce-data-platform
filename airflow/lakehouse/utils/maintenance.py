import os
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import boto3

RETAIN_SNAPSHOTS = 20
TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024
# Younger snapshots and files stay, since a reader or a write in progress may still use them.
MIN_AGE = timedelta(days=3)


def compact(table):
    files = table.inspect.files()
    if len(files) < 2 and (not len(files) or not any(files["content"].to_pylist())):
        return {"rewritten": False}
    # The transaction starts before the read; a concurrent commit changes main and fails its snapshot assertion.
    with table.transaction() as transaction:
        data = table.scan().to_arrow()
        transaction.set_properties(
            {"write.target-file-size-bytes": str(TARGET_FILE_SIZE_BYTES), "commit.manifest-merge.enabled": "true"}
        )
        transaction.overwrite(data)
    return {"rewritten": True, "rows": len(data)}


def expire(table):
    snapshots = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms, reverse=True)
    keep = {s.snapshot_id for s in snapshots[:RETAIN_SNAPSHOTS]}
    keep.update(ref.snapshot_id for ref in table.metadata.refs.values())
    cutoff = (datetime.now(UTC) - MIN_AGE).timestamp() * 1000
    expired = [s.snapshot_id for s in snapshots if s.snapshot_id not in keep and s.timestamp_ms < cutoff]
    if expired:
        table.maintenance.expire_snapshots().by_ids(expired).commit()
    return {"expired_snapshots": len(expired)}


def orphan_files(table):
    """Count data files no snapshot references; deleting them is left to an operator with writers stopped."""
    location = urlsplit(table.location())
    bucket = os.environ["MINIO_LAKEHOUSE_BUCKET"]
    if location.scheme not in {"s3", "s3a"} or location.netloc != bucket:
        raise ValueError("Orphan scan is limited to the configured lakehouse bucket")
    prefix = location.path.strip("/") + "/data/"
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
    cutoff = datetime.now(UTC) - MIN_AGE
    orphans = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        orphans += sum(1 for item in page.get("Contents", []) if item["Key"] not in live and item["LastModified"] < cutoff)
    return {"orphan_candidates": orphans}


ACTIONS = {"compact": compact, "expire": expire, "orphan": orphan_files}


def maintain_tables(catalog, tables, action):
    return {".".join(key): ACTIONS[action](catalog.load_table(key)) for key in tables}
