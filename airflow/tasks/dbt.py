import re
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

DBT_PROJECT = Path("/opt/project/dbt")
GOLD_TESTS = "path:tests/oltp"
# dbt runs on Trino, so its tasks get their own pool instead of the one bounding the Python tasks' memory.
DBT_POOL = "lakehouse_dbt"
# Gold built only from reference data, by lakehouse_oltp_full_daily; everything else is built from transactions.
REFERENCE_GOLD = frozenset({"calendar_events", "delivery_sla", "dim_city", "dim_product", "dim_store", "dim_variant"})
# Marts read Gold through the live() macro, which wraps ref(); both name a parent.
REF = re.compile(r"""(?:ref|live)\(\s*['"]([a-z0-9_]+)['"]\s*\)""")


def gold_seeds(project=DBT_PROJECT):
    return {path.stem for path in (project / "seeds").glob("*.csv")}


def gold_dependencies(project=DBT_PROJECT):
    """Map each Gold model and seed to the Gold models and seeds its SQL refs."""
    models = sorted((project / "models" / "oltp").glob("*.sql"))
    names = gold_seeds(project) | {path.stem for path in models}
    graph = {name: () for name in sorted(gold_seeds(project))}
    for path in models:
        graph[path.stem] = tuple(ref for ref in dict.fromkeys(REF.findall(path.read_text())) if ref in names)
    return graph


def dbt_command(verb, select):
    exclude = f" --exclude {GOLD_TESTS}" if verb == "build" else ""
    return f'echo "DBT_AS_OF=$DBT_AS_OF" && dbt {verb} --select {select}{exclude}'


def dbt_env(node):
    # The project is mounted read-only and tasks run in parallel, so each task writes to its own folder.
    work = "/tmp/dbt/{{ run_id }}/" + node
    return {
        "DBT_PROJECT_DIR": str(DBT_PROJECT),
        "DBT_PROFILES_DIR": str(DBT_PROJECT),
        "DBT_TARGET_PATH": work,
        "DBT_LOG_PATH": work,
        "DBT_AS_OF": "{{ dag_run.run_after.isoformat() }}",
        "DBT_USE_COLORS": "false",
        "DBT_PARTIAL_PARSE": "false",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }


def gold_task_group(full_refresh, reference=None):
    """Build the Gold tasks: the reference nodes, the others (with the Gold tests), or every node when None."""
    refresh = " --full-refresh" if full_refresh else ""
    with TaskGroup("gold") as group:
        graph, seeds = gold_dependencies(), gold_seeds()
        nodes = [node for node in graph if reference is None or (node in REFERENCE_GOLD) == reference]
        models = {
            node: BashOperator(
                task_id=node,
                bash_command=dbt_command("seed", node) if node in seeds else dbt_command("build", node) + refresh,
                env=dbt_env(node),
                append_env=True,
                pool=DBT_POOL,
            )
            for node in nodes
        }
        for node in nodes:
            for ref in graph[node]:
                if ref in models:
                    models[ref] >> models[node]
        if not reference:
            list(models.values()) >> BashOperator(
                task_id="tests",
                bash_command=dbt_command("test", GOLD_TESTS),
                env=dbt_env("tests"),
                append_env=True,
                pool=DBT_POOL,
            )
    return group
