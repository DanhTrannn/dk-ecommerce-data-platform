"""Execute version-locked dbt models while callers hold the native writer lock."""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from pyiceberg.transforms import IdentityTransform

from lakehouse.oltp.polars.writer import PROPERTIES
from lakehouse.utils.namespaces import namespace as ns


def project_directory():
    configured = os.environ.get("DBT_PROJECT_DIR")
    if configured:
        return Path(configured)
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "dbt" / "dbt_project.yml"
        if candidate.is_file():
            return candidate.parent
    raise ValueError("Set DBT_PROJECT_DIR to the checked-in analytical project")


def ensure_target(catalog, prefix, name, schema, ddl=None):
    catalog.create_namespace_if_not_exists(ns(prefix, "gold"))
    table = catalog.create_table_if_not_exists((ns(prefix, "gold"), name), schema=schema, properties=PROPERTIES)
    if set(schema.names) - set(table.schema().column_names):
        with table.update_schema() as update:
            update.union_by_name(schema)
    partition = re.search(r"PARTITIONED BY\s*\(([a-z_]+)\)", ddl or "")
    field = (
        partition[1]
        if partition
        else "metric_date"
        if name.startswith("mart_") and "metric_date" in schema.names
        else None
    )
    if field and not table.spec().fields:
        with table.update_spec() as update:
            update.add_field(field, IdentityTransform(), field)
    return table


def run_models(prefix, run_id, observed, inputs, *, group, silver_schema=None, events_schema=None):
    variables = dict(
        run_id=run_id,
        as_of=observed.isoformat(),
        inputs=inputs,
        silver_schema=silver_schema or ns(prefix, "silver"),
        events_schema=events_schema or ns(prefix, "gold"),
        gold_schema=ns(prefix, "gold"),
    )
    environment = {
        **os.environ,
        "DBT_GOLD_SCHEMA": ns(prefix, "gold"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DBT_PARTIAL_PARSE": "false",
    }
    project = project_directory()
    executable = str(Path(sys.executable).with_name("dbt"))
    with tempfile.TemporaryDirectory(prefix="retail-dbt-") as work:
        command = [
            executable,
            "build",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--target-path",
            work + "/target",
            "--log-path",
            work + "/logs",
            "--no-use-colors",
            "--vars",
            json.dumps(variables),
            "--select",
            "tag:" + group,
        ]
        process = subprocess.run(command, env=environment, check=False)
        artifact_directory = os.environ.get("DBT_ARTIFACTS_DIR")
        if artifact_directory:
            destination = Path(artifact_directory) / run_id / group
            destination.mkdir(parents=True, exist_ok=True)
            for name in ("manifest.json", "run_results.json"):
                source = Path(work) / "target" / name
                if source.is_file():
                    shutil.copyfile(source, destination / name)
        process.check_returncode()
        result = json.loads(Path(work + "/target/run_results.json").read_text())
        return {
            "executed": len(result["results"]),
            "statuses": {item["unique_id"]: str(item["status"]) for item in result["results"]},
        }
