"""Exercise TLS identities and SQL authorization without exposing credentials."""

import json
import os

import requests
from trino.auth import BasicAuthentication
from trino.dbapi import connect
from trino.exceptions import TrinoUserError


def denied(user, password, sql):
    connection = connect(
        host="trino", port=8443, http_scheme="https", user=user,
        auth=BasicAuthentication(user, password), verify="/run/trino-ca/ca.pem",
    )
    try:
        connection.cursor().execute(sql).fetchall()
    except TrinoUserError as error:
        assert "denied" in str(error).lower(), error.error_name
        return True
    finally:
        connection.close()
    raise AssertionError("Statement unexpectedly permitted")


password = os.environ["DBT_TRINO_PASSWORD"]
assert denied("dbt_writer", password, "DELETE FROM lakehouse_etl.silver.silver_orders WHERE false")
response = requests.post(
    "https://trino:8443/v1/statement", data="SELECT 1", timeout=10,
    auth=("dbt_writer", password), headers={"X-Trino-User": "analytics_reader"},
    verify="/run/trino-ca/ca.pem",
)
assert response.status_code == 403, response.status_code
response = requests.post(
    "http://trino:8080/v1/statement", data="SELECT 1", timeout=10,
    auth=("dbt_writer", password), headers={"X-Trino-User": "dbt_writer"},
)
assert response.status_code in {401, 403}, response.status_code
print("STACK_RESULT " + json.dumps(dict(writer_silver_delete_denied=True, impersonation_denied=True, http_auth_denied=True)))
