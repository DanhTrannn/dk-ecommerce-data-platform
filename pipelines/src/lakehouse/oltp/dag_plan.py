"""What the OLTP DAG runs: the Gold task graph and the command and environment of each task.

Airflow imports this module without the pipeline runtime, so it uses the standard library only.
"""

import re
from pathlib import Path

REF = re.compile(r"""ref\(\s*['"]([a-z0-9_]+)['"]\s*\)""")
GOLD_TESTS = "path:tests/oltp"  # OLTP singular tests; tests/ also holds the log pipeline's
POOL = "lakehouse_oltp"
POOL_SLOTS = 4  # parallel OLTP tasks; each one loads a table's batches in the Airflow container's memory
DBT_PROJECT = "/opt/project/pipelines/dbt"
# Accepted prefixes, as lakehouse.utils.namespaces checks them; the DAG param enforces it before any shell sees it.
NAMESPACE_PATTERN = r"^(serving|polars_batch_[a-z0-9_]{1,40})$"
# Fixed for the whole run: Airflow resets dag_run.start_date when a task is cleared, but not run_after,
# so every table of one run keeps the same full-load cutoff and Gold day after a retry.
RUN_STARTED_AT = "{{ dag_run.run_after.isoformat() }}"
# Namespace of a layer for the run's prefix, as lakehouse.utils.namespaces.namespace builds it.
LAYER_SCHEMA = "{{{{ '{layer}' if params.namespace_prefix == 'serving' else params.namespace_prefix ~ '_{layer}' }}}}"


def gold_dependencies(project: Path) -> dict[str, tuple[str, ...]]:
    """Each OLTP model and seed, with the models and seeds its SQL refers to through ref()."""
    graph = {path.stem: () for path in sorted((project / "seeds").glob("*.csv"))}
    for path in sorted((project / "models" / "oltp").glob("*.sql")):
        graph[path.stem] = tuple(dict.fromkeys(REF.findall(path.read_text())))
    return graph


def command(module: str, *extra: str) -> str:
    """Bash call of a per-table CLI; the interval is passed only in incremental runs."""
    words = [
        '"$PIPELINE_PYTHON" -m jobs.oltp.' + module,
        "--namespace-prefix '{{ params.namespace_prefix }}'",
        "--run-id '{{ run_id }}'",
        "--mode '{{ params.mode }}'",
        f"--run-started-at '{RUN_STARTED_AT}'",
        "{% if params.mode == 'incremental' %}"
        "--interval-start '{{ data_interval_start.isoformat() if data_interval_start else \"\" }}' "
        "--interval-end '{{ data_interval_end.isoformat() if data_interval_end else \"\" }}'"
        "{% endif %}",
        *extra,
    ]
    return " ".join(words)


def dbt_command(verb: str, select: str) -> str:
    """The dbt call of a Gold task. The echo prints the run context (no secrets) so the call can be repeated by hand."""
    context = 'echo "DBT_GOLD_SCHEMA=$DBT_GOLD_SCHEMA DBT_SILVER_SCHEMA=$DBT_SILVER_SCHEMA DBT_AS_OF=$DBT_AS_OF"'
    # Singular tests join several models; in a model's task the others may not be built yet, so they run once in
    # the final Gold task. Generic tests (unique, not_null) of the model still run with it.
    exclude = f" --exclude {GOLD_TESTS}" if verb == "build" else ""
    return f"{context} && dbt {verb} --select {select}{exclude}"


def dbt_env(node: str) -> dict[str, str]:
    """Environment of a Gold task; the dbt folder is mounted read-only and Gold tasks run in parallel."""
    work = "/tmp/dbt/{{ run_id }}/" + node
    return {
        "DBT_PROJECT_DIR": DBT_PROJECT,
        "DBT_PROFILES_DIR": DBT_PROJECT,
        "DBT_TARGET_PATH": work,
        "DBT_LOG_PATH": work,
        "DBT_GOLD_SCHEMA": LAYER_SCHEMA.format(layer="gold"),
        "DBT_SILVER_SCHEMA": LAYER_SCHEMA.format(layer="silver"),
        "DBT_AS_OF": RUN_STARTED_AT,
        "DBT_USE_COLORS": "false",
        "DBT_PARTIAL_PARSE": "false",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
