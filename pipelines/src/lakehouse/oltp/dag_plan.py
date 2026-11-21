"""What the OLTP DAG runs: the Gold task graph and the command and environment of each task.

Airflow imports this module without the pipeline runtime, so it uses the standard library only.
"""

import re
from pathlib import Path

REF = re.compile(r"""ref\(\s*['"]([a-z0-9_]+)['"]\s*\)""")
GOLD_TESTS = "test_type:singular"
DBT_PROJECT = "/opt/project/pipelines/dbt"
# Namespace of a layer for the run's prefix, as lakehouse.utils.namespaces.namespace builds it.
LAYER_SCHEMA = "{{{{ '{layer}' if params.namespace_prefix == 'serving' else params.namespace_prefix ~ '_{layer}' }}}}"


def gold_dependencies(project: Path) -> dict[str, tuple[str, ...]]:
    """Each OLTP model and seed, with the models and seeds its SQL refers to through ref()."""
    graph = {path.stem: () for path in sorted((project / "seeds").glob("*.csv"))}
    for path in sorted((project / "models" / "oltp").glob("*.sql")):
        graph[path.stem] = tuple(dict.fromkeys(REF.findall(path.read_text())))
    return graph


def dbt_command(verb: str, select: str) -> str:
    """The dbt call of a Gold task. The echo prints the run context (no secrets) so the call can be repeated by hand."""
    context = 'echo "DBT_GOLD_SCHEMA=$DBT_GOLD_SCHEMA DBT_SILVER_SCHEMA=$DBT_SILVER_SCHEMA DBT_AS_OF=$DBT_AS_OF"'
    return f"{context} && dbt {verb} --select {select}"


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
        "DBT_AS_OF": "{{ dag_run.start_date.isoformat() }}",
        "DBT_USE_COLORS": "false",
        "DBT_PARTIAL_PARSE": "false",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
