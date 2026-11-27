import os
import unittest
from unittest.mock import patch

from platform_app.provisioning import trino

PASSWORDS = {"TRINO_READER_PASSWORD": "r", "DBT_TRINO_PASSWORD": "d", "TRINO_MAINTAINER_PASSWORD": "m"}


class PasswordFileTests(unittest.TestCase):
    def write(self, env):
        with (
            patch.dict(os.environ, env, clear=True),
            patch.object(trino, "atomic_private") as written,
            patch.object(trino, "certificates"),
            patch.object(trino.os, "chown"),
            patch("builtins.print"),
        ):
            trino.main()
        return written.call_args.args[1]

    def test_writes_one_user_per_client(self):
        users = [line.split(":")[0] for line in self.write(PASSWORDS).splitlines()]
        self.assertEqual(users, ["analytics_reader", "dbt_writer", "lakehouse_maintainer"])

    def test_refuses_a_missing_maintainer_password(self):
        with self.assertRaises((KeyError, ValueError)):
            self.write({**PASSWORDS, "TRINO_MAINTAINER_PASSWORD": ""})


if __name__ == "__main__":
    unittest.main()
