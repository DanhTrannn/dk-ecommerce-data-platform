"""What the OLTP DAGs run: the run context of the Python tasks, the Gold task graph and the dbt commands.

Airflow imports this module without the pipeline runtime, so it uses the standard library only.
"""

import re
from pathlib import Path

REF = re.compile(r"""ref\(\s*['"]([a-z0-9_]+)['"]\s*\)""")
GOLD_TESTS = "path:tests/oltp"  # OLTP singular tests; tests/ also holds the log pipeline's
POOL = "lakehouse_oltp"
POOL_SLOTS = 4  # parallel OLTP tasks; each one loads a table's batches in the Airflow container's memory
DBT_PROJECT = "/opt/project/pipelines/dbt"
# Fixed for the whole run: Airflow resets dag_run.start_date when a task is cleared, but not run_after,
# so every table of one run keeps the same full-load cutoff and Gold day after a retry.
RUN_STARTED_AT = "{{ dag_run.run_after.isoformat() }}"
# The `run` argument of the jobs.oltp.tasks calls, as Airflow renders it for each task of the init DAG.
FULL_RUN = {"run_id": "{{ run_id }}", "mode": "full", "run_started_at": RUN_STARTED_AT}
# A run triggered by hand has no data interval, and Airflow then leaves these keys out of the template context;
# rendering them empty lets the task refuse the run with a clear message.
INTERVAL = "{{{{ {key}.isoformat() if {key} is defined and {key} else '' }}}}"
# The same for the incremental DAG, whose scheduled and backfill runs each cover one data interval.
INCREMENTAL_RUN = FULL_RUN | {
    "mode": "incremental",
    "interval_start": INTERVAL.format(key="data_interval_start"),
    "interval_end": INTERVAL.format(key="data_interval_end"),
}


def gold_dependencies(project: Path) -> dict[str, tuple[str, ...]]:
    """Each OLTP model and seed, with the models and seeds its SQL refers to through ref()."""
    graph = {path.stem: () for path in sorted((project / "seeds").glob("*.csv"))}
    for path in sorted((project / "models" / "oltp").glob("*.sql")):
        graph[path.stem] = tuple(dict.fromkeys(REF.findall(path.read_text())))
    return graph


def dbt_command(verb: str, select: str) -> str:
    """The dbt call of a Gold task. The echo prints the run context (no secrets) so the call can be repeated by hand."""
    context = 'echo "DBT_AS_OF=$DBT_AS_OF"'
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
        "DBT_AS_OF": RUN_STARTED_AT,
        "DBT_USE_COLORS": "false",
        "DBT_PARTIAL_PARSE": "false",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
