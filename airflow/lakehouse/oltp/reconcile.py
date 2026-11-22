from datetime import UTC, datetime

import polars as pl

from lakehouse.oltp.staging import staging_rows
from lakehouse.utils.postgres import identifier

MONEY_TOTALS = {"orders": "total_vnd", "payments": "amount_vnd", "refunds": "amount_vnd"}
# Ledger movements exclude the opening stock, so a position's ledger sum up to the cutoff equals
# on_hand - opening_on_hand minus the movements since the cutoff. One query reads both sides at one moment.
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


class ReconciliationFailed(RuntimeError):
    """The extract does not match the source."""


def source_reconciliation(connection, table, created_before):
    """Return the row count, money total and, for the ledger, stock balances created before the cutoff."""
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


def read_extract(catalog, table, columns):
    return pl.from_arrow(staging_rows(catalog, table, columns).read_all())


def ledger_failures(catalog, ledger, source_stock):
    movements = read_extract(catalog, ledger, ["store_id", "variant_id", "quantity_delta"])
    balances = {
        (row["store_id"], row["variant_id"]): row["quantity_delta"]
        for row in movements.group_by("store_id", "variant_id").agg(pl.col("quantity_delta").sum()).iter_rows(named=True)
    }
    mismatched = sum(balances.get(key, 0) != source_stock.get(key, 0) for key in balances.keys() | source_stock.keys())
    return [f"inventory_transactions: ledger_balance_mismatch = {mismatched}"] if mismatched else []


def reconcile_table(catalog, table, source):
    columns = [table.pk, *([MONEY_TOTALS[table.name]] if table.name in MONEY_TOTALS else [])]
    frame = read_extract(catalog, table, columns)
    failures = []
    if frame.height != source["rows"]:
        failures.append(f"{table.name}: source_rows {source['rows']} != {frame.height}")
    if table.name in MONEY_TOTALS:
        column = MONEY_TOTALS[table.name]
        total = int(frame[column].sum() or 0)
        if total != source["total"]:
            failures.append(f"{table.name}: source_{column} {source['total']} != {total}")
    if source["ledger"] is not None:
        failures += ledger_failures(catalog, table, source["ledger"])
    if failures:
        raise ReconciliationFailed("Extract does not match the source:\n" + "\n".join(failures))
    print(f"RECONCILED table={table.name} rows={frame.height}", flush=True)
