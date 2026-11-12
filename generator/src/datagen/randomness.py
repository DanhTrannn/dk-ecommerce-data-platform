"""Seeded random streams: one independent stream per (stream id, batch index).

A batch draws only from its own stream, so its data never depends on which batches ran before it.
"""

import numpy as np
import polars as pl

REFERENCE = 0
ONLINE = 1
POS = 2
INVENTORY = 3


def stream(seed: int, stream_id: int, batch_index: int = 0) -> np.random.Generator:
    return np.random.default_rng(np.random.SeedSequence(seed, spawn_key=(stream_id, batch_index)))


def uuid4_strings(rng: np.random.Generator, count: int) -> pl.Series:
    """Version-4 UUIDs built from the stream's bytes instead of the OS random source."""
    raw = rng.integers(0, 256, size=(count, 16), dtype=np.uint8)
    raw[:, 6] = (raw[:, 6] & 0x0F) | 0x40  # version 4
    raw[:, 8] = (raw[:, 8] & 0x3F) | 0x80  # RFC 4122 variant
    hexes = [row.tobytes().hex() for row in raw]
    return pl.Series([f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}" for h in hexes], dtype=pl.String)
