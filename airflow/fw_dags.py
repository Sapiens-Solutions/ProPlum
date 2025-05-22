from pendulum import datetime
from airflow import DAG
from airflow.models import Variable
from fw_constants import max_parallel_tasks
from fw_groups import create_simple_group, create_dependencies_groups
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

gpfdist_port = str(Variable.get("gpfdist_port"))  # gpfdist port

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


        dicts_group = create_simple_group( #create_dependencies_groups(
            "DICTS",
            "Dictionaries",
            dag,
        )
        if dicts_group:
            #for group in dicts_group:
            #   groups.append(group)
            groups.append(dicts_group)

        fact_group = create_simple_group(
            "FACTS",
            "Facts",
            dag,
        )
        if fact_group:
            groups.append(fact_group)


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
    
def create_sap_odata_dag() -> DAG:
    with DAG(
        "job_load_sap_odata_all",
        description="Demo OData: load from SAP ERP into Greenplum and Clickhouse",
        start_date=datetime(2022, 8, 8, tz="Europe/Moscow"),
        schedule_interval=None,
        concurrency=max_parallel_tasks,
        catchup=False,
        tags=["demo"]
    ) as dag:

        groups = []
        start = EmptyOperator(task_id="start")
        groups.append(start)

        dicts_group = create_simple_group( #create_dependencies_groups(
            "ODS_DICTS",
            "ODS Dictionaries",
            dag,
        )
        if dicts_group:
            #for group in dicts_group:
            groups.append(dicts_group)

        fact_group = create_simple_group(
            "ODS_FACTS",
            "Facts",
            dag,
        )
        if fact_group:
            groups.append(fact_group)
        
        dicts_group_ch = create_simple_group( #create_dependencies_groups(
            "CH_DICTS",
            "Clickhouse Dictionaries",
            dag,
        )
        if dicts_group_ch:
            #for group in dicts_group:
            groups.append(dicts_group_ch)

        fact_group_ch = create_simple_group(
            "CH_FACTS",
            "Clickhouse Facts",
            dag,
        )
        if fact_group_ch:
            groups.append(fact_group_ch)

        prev = None
        for group in groups:
            if prev is not None:
                prev >> group
            prev = group

    return dag
