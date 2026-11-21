import unittest
from types import SimpleNamespace
from unittest.mock import patch

from lakehouse.oltp import source
from lakehouse.oltp.source import source_chunks
from lakehouse.oltp.sources import TABLES_BY_NAME

CATEGORIES = TABLES_BY_NAME["categories"]
BIGINT, TEXT, TIMESTAMPTZ = 20, 25, 1184


def column(name, code):
    return SimpleNamespace(name=name, type_code=code, precision=None, scale=None)


class NamedCursor:
    """Fake named cursor whose description is set only after the first fetch."""

    def __init__(self, rows, columns):
        self.rows, self.columns, self.description = list(rows), columns, None
        self.queries, self.fetches = [], []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query):
        self.queries.append(query)

    def fetchmany(self, size):
        self.description = self.columns
        self.fetches.append(size)
        rows, self.rows = self.rows[:size], self.rows[size:]
        return rows


class Connection:
    def __init__(self, cursor):
        self.named, self.names = cursor, []

    def cursor(self, name=None):
        self.names.append(name)
        return self.named


def categories(count):
    columns = [column(name, BIGINT if name.endswith("_id") else TIMESTAMPTZ if name.endswith("_at") else TEXT)
               for name in CATEGORIES.columns]
    values = {BIGINT: 1, TEXT: "x", TIMESTAMPTZ: None}
    return [tuple(values[c.type_code] for c in columns) for _ in range(count)], columns


class SourceChunkTests(unittest.TestCase):
    def read(self, count, chunk_rows=3, **window):
        rows, columns = categories(count)
        cursor = NamedCursor(rows, columns)
        connection = Connection(cursor)
        with patch.object(source, "CHUNK_ROWS", chunk_rows):
            chunks = list(source_chunks(connection, CATEGORIES, **window))
        return chunks, cursor, connection

    def test_one_query_on_a_named_cursor_split_into_chunks(self):
        chunks, cursor, connection = self.read(7, created_before="2026-11-21T00:00:00Z")
        self.assertEqual([chunk.num_rows for chunk in chunks], [3, 3, 1])
        self.assertEqual(len(cursor.queries), 1)
        self.assertNotIn("LIMIT", cursor.queries[0])
        self.assertIn('"created_at" < TIMESTAMPTZ', cursor.queries[0])
        self.assertEqual(connection.names, ["source_categories"])

    def test_empty_result_yields_one_typed_empty_chunk(self):
        chunks, _, _ = self.read(0)
        self.assertEqual([chunk.num_rows for chunk in chunks], [0])
        self.assertEqual(chunks[0].column_names, list(CATEGORIES.columns))

    def test_exact_multiple_ends_with_an_empty_chunk(self):
        chunks, _, _ = self.read(6)
        self.assertEqual([chunk.num_rows for chunk in chunks], [3, 3, 0])

    def test_half_open_interval_is_refused(self):
        with self.assertRaisesRegex(ValueError, "Both interval bounds"):
            self.read(1, start="2026-11-20T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
