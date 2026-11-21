# Database Seed Assets

Explicit reference-data initialization for the PostgreSQL source. Schema must already exist; API startup never runs these scripts.

- `seed_catalog.py`: categories, products/variants, opening inventory (written as an `adjustment` ledger row, with `opening_on_hand = 0`), welcome coupon and bootstrap admin configured by `API_BOOTSTRAP_ADMIN_*`.
- `__init__.py`: package marker.

From the repository root, `make seed` executes `database.seeds.seed_catalog` with the locked `platform-app` environment. Configure host-reachable `API_DATABASE_URL` and `API_SECRET_KEY` in `.env`; see [database bootstrap](../README.md). Alternatively run each module with `docker compose exec -T platform-app python -m database.seeds.<module>` in the rebuilt container.

The catalog seed owns one transaction. Reruns fill missing records individually, including partially seeded databases, and preserve existing product details, balances, stock versions and admin credentials. Existing non-admin use of the configured admin email is rejected. Change bootstrap credentials before first use; rerunning seed does not rotate an existing password. They do not create the schema, reset data, assign staff to stores, or simulate transactions.

No seed creates cities or stores; the current `data-generator` workspace package creates them during `datagen generate`. Existing locations are preserved by seed reruns. Historical transactions are loaded by the implemented OLTP generator; `datagen logs` writes matching access-log Parquet files, without a Lakehouse loader yet. See [generator](../../generator/README.md). Compose runs only the bootstrap-admin helper before the API; it does not run the full catalog seed.
