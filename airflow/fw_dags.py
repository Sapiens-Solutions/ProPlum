from pendulum import datetime
from airflow import DAG
from airflow.models import Variable
from fw_constants import max_parallel_tasks
from fw_groups import create_simple_group, create_dependencies_groups
from airflow.operators.empty import EmptyOperator

def create_tpch_dag() -> DAG:
    with DAG(
        "job_load_tpch",
        description="TPC-H Benchmark",
        start_date=datetime(2022, 8, 8, tz="Europe/Moscow"),
        schedule_interval=None,
        concurrency=max_parallel_tasks,
        catchup=False,
        tags=["demo"]
    ) as dag:

        groups = []
        start = EmptyOperator(task_id="start")
        groups.append(start)
        dicts_group = create_dependencies_groups(
            "TPCH_DICT",
            "Dictionaries",
            dag,
        )
        if dicts_group:
            for group in dicts_group:
                groups.append(group)

        fact_group = create_simple_group(
            "TPCH_FACT",
            "Facts",
            dag,
        )
        if fact_group:
            groups.append(fact_group)

        dds_group = create_simple_group(
             "TPCH_DDS",
             "DDS TPC-H",
             dag,
         )
        if dds_group:
            for group in dds_group:
                groups.append(group)

        prev = None
        for group in groups:
            if prev is not None:
                prev >> group
            prev = group

    return dag


def create_single_object_dag_tpch() -> DAG:
    with DAG(
            "job_load_single_object_tpch",
            description="Load single object",
            start_date=datetime(2022, 8, 8, tz="Europe/Moscow"),
            schedule_interval=None,
            catchup=False,
            tags=["demo"]
    ) as dag:
        object_id = int(Variable.get("single_object_tpch"))

        create_simple_group(
            "single_object_group",
            "Load single object",
            dag,
            object_id,
            object_id,
        )

    return dag
