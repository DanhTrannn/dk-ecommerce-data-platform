"""Gold parity fingerprints: integer money exact, DOUBLE fields compared at 1e-6."""

from datetime import date, datetime

from batch_contracts import fingerprint


def gold_fingerprint(rows, columns):
    normalized = []
    for row in rows:
        row = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        normalized.append(
            {
                key: round(value, 6)
                if isinstance(value, float)
                else value.isoformat()
                if isinstance(value, date) and not isinstance(value, datetime)
                else value
                for key, value in row.items()
            }
        )
    return fingerprint(normalized, columns)
