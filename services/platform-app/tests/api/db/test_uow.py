"""Retry policy and transaction ownership without a live database."""

import importlib
import os
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import OperationalError


class TransactionPolicyTests(unittest.TestCase):
    def setUp(self):
        from platform_app.api.core.config import get_settings

        environment = patch.dict(
            os.environ,
            {
                "API_DATABASE_URL": "postgresql+psycopg2://fixture@localhost/unused",
                "API_SECRET_KEY": uuid.uuid4().hex,
            },
        )
        environment.start()
        self.addCleanup(environment.stop)
        get_settings.cache_clear()
        self.addCleanup(get_settings.cache_clear)
        self.uow = importlib.import_module("platform_app.api.db.uow")
        for attribute, value in (
            ("get_settings", lambda: SimpleNamespace(checkout_max_retries=3)),
            ("time", MagicMock()),
        ):
            binding = patch.object(self.uow, attribute, value)
            binding.start()
            self.addCleanup(binding.stop)

    def error(self, state):
        original = Exception("Fixture PostgreSQL error")
        original.pgcode = state
        return OperationalError(None, None, original)

    def test_postgres_retry_states_exclude_mysql_and_connection_failures(self):
        for state in ("40P01", "40001", "55P03"):
            self.assertTrue(self.uow._is_transient(self.error(state)))
        for state in ("23505", "08006", "57014", 1213, 1205, None):
            self.assertFalse(self.uow._is_transient(self.error(state)))

    def test_retry_exhaustion_rolls_back_and_closes_every_session(self):
        sessions = [MagicMock() for _ in range(3)]
        work = MagicMock(side_effect=self.error("40P01"))
        with patch.object(self.uow, "SessionLocal", side_effect=sessions):
            from platform_app.api.core.errors import AppError

            with self.assertRaises(AppError) as failure:
                self.uow.run_in_transaction(work)
        self.assertEqual(failure.exception.status_code, 409)
        self.assertEqual(work.call_count, 3)
        for session in sessions:
            session.rollback.assert_called_once()
            session.close.assert_called_once()
            session.commit.assert_not_called()

    def test_unknown_commit_outcome_is_not_replayed(self):
        session = MagicMock()
        session.commit.side_effect = self.error("08006")
        work = MagicMock(return_value="fixture")
        with patch.object(self.uow, "SessionLocal", return_value=session):
            with self.assertRaises(OperationalError):
                self.uow.run_in_transaction(work)
        work.assert_called_once()
        session.rollback.assert_called_once()
        session.close.assert_called_once()
