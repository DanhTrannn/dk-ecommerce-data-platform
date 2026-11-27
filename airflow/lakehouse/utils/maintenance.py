import os

import trino

CATALOG = "lakehouse_etl"
MAINTAINER = "lakehouse_maintainer"
# Trino's default minimum retention; younger snapshots and files stay, since a reader or a write may still use them.
RETENTION = "7d"
STATEMENTS = {
    "optimize": "ALTER TABLE {table} EXECUTE optimize",
    "expire": f"ALTER TABLE {{table}} EXECUTE expire_snapshots(retention_threshold => '{RETENTION}')",
    "orphan": f"ALTER TABLE {{table}} EXECUTE remove_orphan_files(retention_threshold => '{RETENTION}')",
}


def statement(action, table):
    schema, name = table
    return STATEMENTS[action].format(table=f"{CATALOG}.{schema}.{name}")


def maintain_tables(tables, action):
    """Run one Trino maintenance statement per (schema, table), as the account allowed only table procedures."""
    connection = trino.dbapi.connect(
        host=os.environ["DBT_TRINO_HOST"],
        port=int(os.environ.get("DBT_TRINO_PORT", "8443")),
        user=MAINTAINER,
        auth=trino.auth.BasicAuthentication(MAINTAINER, os.environ["TRINO_MAINTAINER_PASSWORD"]),
        http_scheme="https",
        verify=os.environ["TRINO_CA_FILE"],
        catalog=CATALOG,
    )
    result = {}
    try:
        cursor = connection.cursor()
        for table in tables:
            cursor.execute(statement(action, table))
            cursor.fetchall()
            result[".".join(table)] = "done"
    finally:
        connection.close()
    return result
