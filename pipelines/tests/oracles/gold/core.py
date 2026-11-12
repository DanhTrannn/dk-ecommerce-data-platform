"""Small native SQL/expression helpers with explicit Spark-compatible semantics."""

import polars as pl


def query(frames, sql):
    return pl.SQLContext(frames=frames, eager=True).execute(sql)


def round_money_rate(frame, *columns):
    return frame.with_columns([pl.col(name).round(2, mode="half_away_from_zero") for name in columns])


def metadata(frame, run_id, observed):
    return frame.with_columns(pl.lit(observed).alias("_gold_ingested_at"), pl.lit(run_id).alias("_source_run_id"))
