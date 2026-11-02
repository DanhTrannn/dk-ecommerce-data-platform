"""Explicit, repeatable upgrade for existing local databases; never recreates tables.

Run with: docker compose exec ecommerce-api python -m app.db.upgrade_payment_timestamp
Pause source writes and pipeline writers before upgrading and refreshing payments.
"""

from sqlalchemy import inspect, text


def upgrade(engine):
    with engine.begin() as connection:
        columns = {column["name"] for column in inspect(connection).get_columns("payments")}
        definition = "DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
        if "updated_at" not in columns:
            connection.execute(text(f"ALTER TABLE payments ADD COLUMN updated_at {definition}"))
        else:
            connection.execute(text("UPDATE payments SET updated_at=CURRENT_TIMESTAMP(6) WHERE updated_at IS NULL"))
            connection.execute(text(f"ALTER TABLE payments MODIFY COLUMN updated_at {definition}"))
        indexes = {index["name"] for index in inspect(connection).get_indexes("payments")}
        if "ix_payments_updated_at_payment_id" not in indexes:
            connection.execute(
                text("CREATE INDEX ix_payments_updated_at_payment_id ON payments(updated_at,payment_id)")
            )


if __name__ == "__main__":
    from app.db.session import engine

    upgrade(engine)
    print("Payment timestamp/index upgrade complete. Refresh existing Silver payments before publishing Gold.")
