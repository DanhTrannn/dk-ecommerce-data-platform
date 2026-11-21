"""The dbt Gold tasks: one task per model or seed, wired by the ref() calls in their SQL."""

import re
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

DBT_PROJECT = Path("/opt/project/dbt")
GOLD_TESTS = "path:tests/oltp"  # OLTP singular tests; tests/ also holds the log pipeline's
REF = re.compile(r"""ref\(\s*['"]([a-z0-9_]+)['"]\s*\)""")


def gold_seeds(project=DBT_PROJECT):
    return {path.stem for path in (project / "seeds").glob("*.csv")}


def gold_dependencies(project=DBT_PROJECT):
    """Each OLTP model and seed, with the models and seeds its SQL refers to through ref()."""
    models = sorted((project / "models" / "oltp").glob("*.sql"))
    names = gold_seeds(project) | {path.stem for path in models}
    graph = {name: () for name in sorted(gold_seeds(project))}
    for path in models:
        # Refs to Silver models are left out: the Silver group runs before the Gold one.
        graph[path.stem] = tuple(ref for ref in dict.fromkeys(REF.findall(path.read_text())) if ref in names)
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


def gold_task_group(pool):
    """The Gold TaskGroup both OLTP DAGs run: one dbt task per model or seed, wired by ref(), then the tests."""
    with TaskGroup("gold") as group:
        graph, seeds = gold_dependencies(), gold_seeds()
        models = {
            node: BashOperator(
                task_id=node,
                bash_command=dbt_command("seed" if node in seeds else "build", node),
                env=dbt_env(node),
                append_env=True,
                pool=pool,
            )
            for node in graph
        }
        for node, refs in graph.items():
            for ref in refs:
                models[ref] >> models[node]
        list(models.values()) >> BashOperator(
            task_id="tests",
            bash_command=dbt_command("test", GOLD_TESTS),
            env=dbt_env("tests"),
            append_env=True,
            pool=pool,
        )
    return group
