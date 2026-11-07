# Database Seed Assets

Explicit reference-data initialization for the PostgreSQL source. Schema must already exist; API startup never runs these scripts.

- `seed_catalog.py`: categories, products/variants, opening inventory, welcome coupon and bootstrap admin configured by `API_BOOTSTRAP_ADMIN_*`.
- `__init__.py`: package marker.

From the repository root, `make seed` executes `database.seeds.seed_catalog` with the locked `platform-app` environment. Configure host-reachable `API_DATABASE_URL` and `API_SECRET_KEY` in `.env`; see [database bootstrap/import](../README.md). Alternatively run each module with `docker compose exec -T platform-app python -m database.seeds.<module>` in the rebuilt container.

The catalog seed owns one transaction. Reruns fill missing records individually, including partially seeded databases, and preserve existing product details, balances, stock versions and admin credentials. Existing non-admin use of the configured admin email is rejected. Change bootstrap credentials before first use; rerunning seed does not rotate an existing password. Seeds can run before or after generated history; they do not create the schema, reset data, assign staff to stores, or simulate transactions.

The former city/store seed has moved to [generator locations](../../generator/README.md#cities-and-stores). Generator 0.10 SQL creates or reuses selected city/store codes; `make seed` no longer supplies location data. Existing database locations are preserved.

Use the [generator](../../generator/README.md) for historical transactions and access logs. Removing scripts does not delete records from an existing database.
