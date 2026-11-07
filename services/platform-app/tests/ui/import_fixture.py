"""Create completed generation jobs in temporary test roots, without a worker."""

import os
from pathlib import Path

import yaml
from generator import __version__
from generator.config import config_from_dict
from generator.runner import run
from generator.sql_export import export_sql

from platform_app.ui.artifacts import collect_artifacts, write_checksums
from platform_app.ui.job_store import JobStore

ROOT = Path(__file__).resolve().parents[4]


def generated_job(directory: Path):
    raw = yaml.safe_load((ROOT / "generator/configs/small.yml").read_text())
    raw["scale"] = dict(customers=10, products=2, variants=8, orders=30)
    config = config_from_dict(raw)
    store = JobStore(directory, launcher=lambda _: os.getpid())
    job = store.create_job(
        raw,
        {"output": "sql"},
        preset_name="fixture",
        config_yaml=yaml.safe_dump(raw),
        generator_version=__version__,
        dataset_identity=config.logical_identity,
    )
    store.mark_started(job.job_id, os.getpid())
    run(config, job.directory)
    path = job.directory / "outputs/sql" / f"{config.scenario_id}-{config.logical_identity}.sql"
    export_sql(config, path)
    for stage in ("validate", "sql"):
        store.complete_stage(job.job_id, stage, "succeeded")
    write_checksums(job.directory)
    store.set_artifacts(job.job_id, collect_artifacts(job.directory))
    store.finish_job(job.job_id, "succeeded")
    store.release_active(job.job_id)
    return store, store.get_job(job.job_id), path
