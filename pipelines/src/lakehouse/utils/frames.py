"""Small native SQL helper for independent quality expectations."""

import polars as pl


def query(frames, sql):
    return pl.SQLContext(frames=frames, eager=True).execute(sql)
