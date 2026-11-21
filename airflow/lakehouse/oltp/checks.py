"""Checks on a run's Bronze rows; any failure stops the run before Silver changes."""

from collections import Counter
from datetime import UTC, datetime

import polars as pl

from lakehouse.oltp.bronze import bronze_rows
from lakehouse.oltp.rules import violation_counts
from lakehouse.oltp.sources import FOREIGN_KEYS, TABLES_BY_NAME
from lakehouse.utils.postgres import identifier

MONEY_TOTALS = {"orders": "total_vnd", "payments": "amount_vnd", "refunds": "amount_vnd"}
# Ledger movements exclude the opening stock, so a position's ledger sum up to the cutoff equals
# on_hand - opening_on_hand minus the movements recorded since the cutoff; one query keeps both sides at one moment.
SOURCE_STOCK = """
    WITH later AS (
        SELECT store_id, variant_id, sum(quantity_delta) AS moved
        FROM "public"."inventory_transactions" WHERE "created_at" >= %(cutoff)s
        GROUP BY store_id, variant_id
    ), stock AS (
        SELECT NULL::bigint AS store_id, variant_id, on_hand - opening_on_hand AS balance FROM "public"."inventory"
        UNION ALL
        SELECT store_id, variant_id, on_hand - opening_on_hand FROM "public"."store_inventory"
    )
    SELECT s.store_id, s.variant_id, s.balance - coalesce(l.moved, 0)
    FROM stock s LEFT JOIN later l ON l.variant_id = s.variant_id AND l.store_id IS NOT DISTINCT FROM s.store_id
"""


class CheckFailed(RuntimeError):
    """A pre-merge check failed; Silver and Gold keep their previous state."""


def source_reconciliation(connection, table, created_before):
    """Row count, money total and (for the ledger) stock balances of one table, up to the run's cutoff."""
    cutoff = datetime.fromisoformat(created_before.replace("Z", "+00:00")).astimezone(UTC)
    total = f"sum({identifier(MONEY_TOTALS[table.name])})" if table.name in MONEY_TOTALS else "NULL"
    with connection.cursor() as cursor:
        cursor.execute(
            f'SELECT count(*), {total} FROM "public".{identifier(table.name)} WHERE "created_at" < %(cutoff)s',
            {"cutoff": cutoff},
        )
        rows, amount = cursor.fetchone()
        ledger = None
        if table.name == "inventory_transactions":
            cursor.execute(SOURCE_STOCK, {"cutoff": cutoff})
            ledger = {(store_id, variant_id): balance for store_id, variant_id, balance in cursor.fetchall()}
    return {"rows": rows, "total": None if table.name not in MONEY_TOTALS else int(amount or 0), "ledger": ledger}


def read_bronze(catalog, table, columns):
    return pl.from_arrow(bronze_rows(catalog, table, columns).read_all())


def row_rule_failures(catalog, table):
    counts = Counter()
    for batch in bronze_rows(catalog, table):
        counts.update(violation_counts(pl.from_arrow(batch), table))
    return [f"{table.name}: {label} = {count}" for label, count in sorted(counts.items())]


def full_load_failures(catalog, table, source):
    """Whole-table checks; valid only when the run holds every source row created before the cutoff."""
    columns = [table.pk, *([MONEY_TOTALS[table.name]] if table.name in MONEY_TOTALS else [])]
    frame = read_bronze(catalog, table, columns)
    failures = []
    duplicates = frame.height - frame[table.pk].n_unique()
    if duplicates:
        failures.append(f"{table.name}: duplicate_primary_key = {duplicates}")
    if frame.height != source["rows"]:
        failures.append(f"{table.name}: source_rows {source['rows']} != {frame.height}")
    if table.name in MONEY_TOTALS:
        column = MONEY_TOTALS[table.name]
        total = int(frame[column].sum() or 0)
        if total != source["total"]:
            failures.append(f"{table.name}: source_{column} {source['total']} != {total}")
    if source["ledger"] is not None:
        failures += ledger_failures(catalog, table, source["ledger"])
    return failures


def foreign_key_failures(catalog):
    """A full load holds every row created before the cutoff, and a child is never created before its parent,
    so a miss means rows were lost on the way to the lakehouse. Incremental runs skip this check: an order
    created in the interval but updated after it is read by the next run."""
    keys = {}
    for name in {parent for _, _, parent in FOREIGN_KEYS}:
        table = TABLES_BY_NAME[name]
        keys[name] = read_bronze(catalog, table, [table.pk])[table.pk]
    failures = []
    for child in dict.fromkeys(child for child, _, _ in FOREIGN_KEYS):
        references = [(column, parent) for name, column, parent in FOREIGN_KEYS if name == child]
        frame = read_bronze(catalog, TABLES_BY_NAME[child], [column for column, _ in references])
        for column, parent in references:
            missing = frame[column].drop_nulls().is_in(keys[parent].implode()).not_().sum()
            if missing:
                failures.append(f"{child}: {column} -> {parent} = {missing}")
    return failures


def ledger_failures(catalog, ledger, source_stock):
    movements = read_bronze(catalog, ledger, ["store_id", "variant_id", "quantity_delta"])
    balances = {
        (row["store_id"], row["variant_id"]): row["quantity_delta"]
        for row in movements.group_by("store_id", "variant_id").agg(pl.col("quantity_delta").sum()).iter_rows(named=True)
    }
    mismatched = sum(balances.get(key, 0) != source_stock.get(key, 0) for key in balances.keys() | source_stock.keys())
    return [f"inventory_transactions: ledger_balance_mismatch = {mismatched}"] if mismatched else []


def check_table(catalog, table, source):
    """Raise CheckFailed listing every failed check of one table; `source` is given only for full loads."""
    failures = row_rule_failures(catalog, table)
    if source is not None:
        failures += full_load_failures(catalog, table, source)
    if failures:
        raise CheckFailed("Pre-merge checks failed:\n" + "\n".join(failures))
    print(f"CHECKS table={table.name} passed full={source is not None}", flush=True)


def check_foreign_keys(catalog):
    failures = foreign_key_failures(catalog)
    if failures:
        raise CheckFailed("Foreign key checks failed:\n" + "\n".join(failures))
    print("CHECKS foreign_keys passed", flush=True)
