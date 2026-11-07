# Database Seed Assets

Explicit reference-data initialization for the PostgreSQL source. Schema must already exist; API startup never runs these scripts.

- `seed_catalog.py`: categories, products/variants, opening inventory, welcome coupon and bootstrap admin configured by `API_BOOTSTRAP_ADMIN_*`.
- `0010_multi_city_stores.py`: five cities and six stores, matched by case-insensitive business codes.
- `__init__.py`: package marker.

From the repository root, `make seed` executes both modules with the locked `platform-app` environment. Configure host-reachable `API_DATABASE_URL` and `API_SECRET_KEY` in `.env`; see [database bootstrap/import](../README.md). Alternatively run each module with `docker compose exec -T platform-app python -m database.seeds.<module>` in the rebuilt container.

Each module owns one transaction. Reruns fill missing records individually, including partially seeded databases, and preserve existing product details, balances, stock versions and admin credentials. Existing non-admin use of the configured admin email is rejected. Change bootstrap credentials before first use; rerunning seed does not rotate an existing password. Seeds can run before or after generated history; they do not create the schema, reset data, assign staff to stores, or simulate transactions.

Use the [generator](../../generator/README.md) for historical transactions and access logs. Removing scripts does not delete records from an existing database.
