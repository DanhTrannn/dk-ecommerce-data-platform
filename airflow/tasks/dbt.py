"""The dbt Gold tasks: the task graph from the ref() calls in the SQL, and each task's command and environment."""

import re
from pathlib import Path

DBT_PROJECT = Path("/opt/project/dbt")
GOLD_TESTS = "path:tests/oltp"  # OLTP singular tests; tests/ also holds the log pipeline's
REF = re.compile(r"""ref\(\s*['"]([a-z0-9_]+)['"]\s*\)""")


def gold_seeds(project=DBT_PROJECT):
    return {path.stem for path in (project / "seeds").glob("*.csv")}


def gold_dependencies(project=DBT_PROJECT):
    """Each OLTP model and seed, with the models and seeds its SQL refers to through ref()."""
    graph = {name: () for name in sorted(gold_seeds(project))}
    for path in sorted((project / "models" / "oltp").glob("*.sql")):
        graph[path.stem] = tuple(dict.fromkeys(REF.findall(path.read_text())))
    return graph


def dbt_command(verb, select):
    """The echo prints DBT_AS_OF (no secrets) so the call can be repeated by hand."""
    # Singular tests join several models; in a model's task the others may not be built yet, so they run once in
    # the final Gold task. Generic tests (unique, not_null) of the model still run with it.
    exclude = f" --exclude {GOLD_TESTS}" if verb == "build" else ""
    return f'echo "DBT_AS_OF=$DBT_AS_OF" && dbt {verb} --select {select}{exclude}'


def dbt_env(node):
    """The dbt folder is mounted read-only and Gold tasks run in parallel, so each task has its own work folder."""
    work = "/tmp/dbt/{{ run_id }}/" + node
    return {
        "DBT_PROJECT_DIR": str(DBT_PROJECT),
        "DBT_PROFILES_DIR": str(DBT_PROJECT),
        "DBT_TARGET_PATH": work,
        "DBT_LOG_PATH": work,
        # Fixed for the whole run: Airflow keeps run_after when a task is cleared, so a retry keeps the same Gold day.
        "DBT_AS_OF": "{{ dag_run.run_after.isoformat() }}",
        "DBT_USE_COLORS": "false",
        "DBT_PARTIAL_PARSE": "false",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
