from pendulum import datetime
from airflow import DAG
from airflow.models import Variable
from fw_constants import max_parallel_tasks
from fw_groups import create_simple_group, create_dependencies_groups
from airflow.operators.empty import EmptyOperator

def create_proplum_demo_dag() -> DAG:
    with DAG(
        "job_proplum_demo_dag",
        description="Proplum demo loading",
        start_date=datetime(2025, 1, 1, tz="Europe/Moscow"),
        schedule_interval=None,
        concurrency=max_parallel_tasks,
        catchup=False,
        tags=["demo","proplum"]
    ) as dag:

        groups = []
        start = EmptyOperator(task_id="start")
        groups.append(start)

        demo_group = create_dependencies_groups(
            "DEMO_PROPLUM",
            "Demo loading",
            dag,
        )
        if demo_group:
            for group in demo_group:
                groups.append(group)
        prev = None
        for group in groups:
            if prev is not None:
                prev >> group
            prev = group

    return dag

def create_proplum_demo_dag_no_dep() -> DAG:
    with DAG(
        "job_proplum_demo_dag_no_dep",
        description="Proplum demo loading without dependencies",
        start_date=datetime(2025, 1, 1, tz="Europe/Moscow"),
        schedule_interval=None,
        concurrency=max_parallel_tasks,
        catchup=False,
        tags=["demo","proplum"]
    ) as dag:

        groups = []
        start = EmptyOperator(task_id="start")
        groups.append(start)


        demo_group = create_simple_group(
            "DEMO_PROPLUM",
            "Demo loading",
            dag,
        )
        if demo_group:
            groups.append(demo_group)

        prev = None
        for group in groups:
            if prev is not None:
                prev >> group
            prev = group

    return dag

proplum_demo_dag: DAG = create_proplum_demo_dag()
proplum_demo_dag_no_dep: DAG = create_proplum_demo_dag_no_dep()