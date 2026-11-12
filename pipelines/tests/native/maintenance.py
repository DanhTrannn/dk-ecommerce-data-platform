"""Exercise native maintenance against real files while preserving published history."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from lakehouse.utils.maintenance import maintain_tables, protected_snapshots, expiry_candidates
from lakehouse.utils.snapshots import capture, read_snapshot


def exercise(catalog, connection):
    args = SimpleNamespace(
        retain_snapshots=1, older_than_days=1, quarantine_retention_days=30,
        target_file_size_bytes=134217728, delete_orphans=False, writers_stopped=False,
        namespace_prefix="serving", profile="all", tables="gold.mart_order_daily,gold.fact_web_events",
        action="all",
    )
    names = [("gold", "mart_order_daily"), ("gold", "fact_web_events")]
    before = {name: read_snapshot(catalog.load_table(name), capture(catalog.load_table(name))).height for name in names}
    result = maintain_tables(catalog, connection, args)
    protected = protected_snapshots(catalog, "serving")
    checked = 0
    for physical, snapshots in protected.items():
        table = catalog.load_table(tuple(physical.split(".")[1:]))
        for snapshot in snapshots:
            assert table.snapshot_by_id(snapshot) is not None, physical
            # Reading verifies data files as well as retained metadata.
            read_snapshot(table, snapshot)
            checked += 1
        candidates = expiry_candidates(table, physical, protected, retain=1, cutoff=datetime.now(UTC) + timedelta(days=1))
        assert not snapshots.intersection(candidates), physical
    for name in names:
        table = catalog.load_table(name)
        assert read_snapshot(table, capture(table)).height == before[name]
    args.delete_orphans = True
    try:
        maintain_tables(catalog, connection, args)
    except ValueError as error:
        assert "writers-stopped" in str(error)
    else:
        raise AssertionError("Unsafe orphan deletion accepted")
    return dict(tables=result, protected_snapshots_read=checked, unsafe_delete_denied=True)
