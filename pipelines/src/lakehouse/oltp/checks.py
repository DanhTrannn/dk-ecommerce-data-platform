"""Checks on a run's Bronze rows; any failure stops the run before Silver changes."""

from collections import Counter

import polars as pl

from lakehouse.oltp.bronze import run_rows
from lakehouse.oltp.rules import violation_counts
from lakehouse.oltp.sources import FOREIGN_KEYS
from lakehouse.utils.postgres import identifier

MONEY_TOTALS = {"orders": "total_vnd", "payments": "amount_vnd", "refunds": "amount_vnd"}
# Ledger movements exclude the opening stock, so a position's ledger sum equals on_hand - opening_on_hand.
SOURCE_STOCK = """
    SELECT NULL::bigint AS store_id, variant_id, on_hand - opening_on_hand FROM "public"."inventory"
    UNION ALL
    SELECT store_id, variant_id, on_hand - opening_on_hand FROM "public"."store_inventory"
"""


class CheckFailed(RuntimeError):
    """A pre-merge check failed; Silver and Gold keep their previous state."""


def source_reconciliation(connection, specs):
    """Row counts, money totals and stock balances read from PostgreSQL in the ingest transaction."""
    with connection.cursor() as cursor:
        rows = {}
        for table in specs:
            cursor.execute(f'SELECT count(*) FROM "public".{identifier(table.name)}')
            rows[table.name] = cursor.fetchone()[0]
        totals = {}
        for name, column in MONEY_TOTALS.items():
            cursor.execute(f'SELECT coalesce(sum({identifier(column)}), 0) FROM "public".{identifier(name)}')
            totals[name] = int(cursor.fetchone()[0])
        cursor.execute(SOURCE_STOCK)
        ledger = {(store_id, variant_id): balance for store_id, variant_id, balance in cursor.fetchall()}
    return {"rows": rows, "totals": totals, "ledger": ledger}


def read_run(catalog, prefix, table, run_id, columns):
    reader = run_rows(catalog, prefix, table, run_id, columns)
    return pl.from_arrow(reader.read_all()) if reader is not None else pl.DataFrame(schema={name: pl.Int64 for name in columns})


def row_rule_failures(catalog, prefix, table, run_id):
    counts = Counter()
    reader = run_rows(catalog, prefix, table, run_id)
    for batch in reader or ():
        counts.update(violation_counts(pl.from_arrow(batch), table))
    return [f"{table.name}: {label} = {count}" for label, count in sorted(counts.items())]


def full_load_failures(catalog, prefix, specs, run_id, source):
    """Whole-table checks; valid only when the run holds every source row.

    An incremental run only holds rows whose cursor fell in its interval, so a child row may point at a
    parent updated after the interval end. PostgreSQL enforces these keys, so in a full load a miss means
    rows were lost on the way to the lakehouse.
    """
    loaded = {table.name for table in specs}
    parents = {parent for child, _, parent in FOREIGN_KEYS if child in loaded and parent in loaded}
    failures, keys = [], {}
    for table in specs:
        columns = [table.pk, *([MONEY_TOTALS[table.name]] if table.name in MONEY_TOTALS else [])]
        frame = read_run(catalog, prefix, table, run_id, columns)
        duplicates = frame.height - frame[table.pk].n_unique()
        if duplicates:
            failures.append(f"{table.name}: duplicate_primary_key = {duplicates}")
        if frame.height != source["rows"][table.name]:
            failures.append(f"{table.name}: source_rows {source['rows'][table.name]} != {frame.height}")
        if table.name in MONEY_TOTALS:
            column = MONEY_TOTALS[table.name]
            total = int(frame[column].sum() or 0)
            if total != source["totals"][table.name]:
                failures.append(f"{table.name}: source_{column} {source['totals'][table.name]} != {total}")
        if table.name in parents:
            keys[table.name] = frame[table.pk]
    for table in specs:
        references = [(column, parent) for child, column, parent in FOREIGN_KEYS if child == table.name and parent in loaded]
        if not references:
            continue
        frame = read_run(catalog, prefix, table, run_id, [column for column, _ in references])
        for column, parent in references:
            missing = frame[column].drop_nulls().is_in(keys[parent].implode()).not_().sum()
            if missing:
                failures.append(f"{table.name}: {column} -> {parent} = {missing}")
    for table in specs:
        if table.name == "inventory_transactions":
            failures += ledger_failures(catalog, prefix, table, run_id, source["ledger"])
    return failures


def ledger_failures(catalog, prefix, ledger, run_id, source_stock):
    movements = read_run(catalog, prefix, ledger, run_id, ["store_id", "variant_id", "quantity_delta"])
    balances = {
        (row["store_id"], row["variant_id"]): row["quantity_delta"]
        for row in movements.group_by("store_id", "variant_id").agg(pl.col("quantity_delta").sum()).iter_rows(named=True)
    }
    mismatched = sum(balances.get(key, 0) != source_stock.get(key, 0) for key in balances.keys() | source_stock.keys())
    return [f"inventory_transactions: ledger_balance_mismatch = {mismatched}"] if mismatched else []


def check_run(catalog, prefix, specs, run_id, source=None):
    """Raise CheckFailed listing every failed check; `source` is given only for full loads."""
    failures = []
    for table in specs:
        failures += row_rule_failures(catalog, prefix, table, run_id)
    if source is not None:
        failures += full_load_failures(catalog, prefix, specs, run_id, source)
    if failures:
        raise CheckFailed("Pre-merge checks failed:\n" + "\n".join(failures))
    print(f"CHECKS passed tables={len(specs)} full={source is not None}", flush=True)
