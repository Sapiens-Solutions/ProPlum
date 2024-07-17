from pendulum import datetime, date, time
from airflow import DAG
from airflow.models import Variable
from fw_groups import create_simple_group, create_dependencies_groups, create_task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.utils.weekday import WeekDay
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from fw_constants import adwh_conn_id, max_parallel_tasks
from psycopg2.extras import RealDictCursor
import os

import re

import ast
def parse_chain(chain):
    """
    Parse chain like '[1,2]>>3>>4' into list
    """
    # split chain by '>>'
    parts = chain.split('>>')
    parsed_parts = []

    for part in parts:
        # Check if it is a part of list
        if re.match(r'\[.*\]', part):
            # string to list
            parsed_parts.append(ast.literal_eval(part))
        else:
            # check if digital
            parsed_parts.append(int(part) if part.isdigit() else part)

    return parsed_parts


def fetch_dag_configs(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT chain_name, chain_description, schedule, job_name, sequence "
            "FROM fw.chains "
            "WHERE active "
            "ORDER BY chain_name"
        )
        return cur.fetchall()

def create_dag(dag_id, description, schedule_interval,sequence,default_args,conn):
    shedule = None if schedule_interval =='None' else schedule_interval
    dag = DAG(
        dag_id,
        description=description,
        schedule_interval=shedule,
        start_date=datetime(2022, 12, 21, tz="Europe/Moscow"),
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=["proplum"]
    )
    parsed_chains = parse_chain(sequence)
    with dag:
        groups = []
        # create branches
        task_branches = [[] for i in range(max_parallel_tasks)]
        task_branch_index = 0

        start = EmptyOperator(task_id='start')
        groups.append(start)
        for i,group_ in enumerate(parsed_chains):
            if isinstance(group_,list):
                with TaskGroup(
                    group_id=str(i)+"_"+"_".join(map(str, group_)),
                    tooltip='auto_generate',
                    dag=dag
                     ) as task_group1:
                    for object in group_:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute(
                            "select object_id, object_name, object_desc, load_method, responsible_mail "
                            "from fw.objects "
                            "where "
                            "object_id = %s ",
                            (int(object),))
                            objects = cur.fetchall()
                        for obj in objects:
                                        task_group=create_task_group(  
                                        obj=obj,
                                        dag=dag,
                                        parent_group=task_group1     
                                )
                                        # add task group into branch
                                        task_branches[task_branch_index].append(task_group)
                                        task_branch_index += 1
                                        if task_branch_index == max_parallel_tasks:
                                            task_branch_index = 0
                                    # create connection    
                        for task_branch in task_branches:
                                        prev = None
                                        for task_group in task_branch:
                                            if prev is not None:
                                                prev >> task_group
                                            prev = task_group
                    groups.append(task_group1)
            else:
                object = group_
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                    "select object_id, object_name, object_desc, load_method, responsible_mail "
                    "from fw.objects "
                    "where "
                    "object_id = %s ",
                    (int(object),))
                    objects = cur.fetchall()
                for obj in objects:
                                task_group=create_task_group(  
                                obj=obj,
                                dag=dag,
                                parent_group=None     
                        )
                                 # add task group into branch
                                task_branches[task_branch_index].append(task_group)
                                task_branch_index += 1
                                if task_branch_index == max_parallel_tasks:
                                    task_branch_index = 0
                            # create connection  
                for task_branch in task_branches:
                                prev = None
                                for task_group in task_branch:
                                    if prev is not None:
                                        prev >> task_group
                                    prev = task_group
                groups.append(task_group)

                                    
        prev = None
        for group in groups:
            if prev is not None:
                prev >> group
            prev = group

    return dag

def gen_dags():
    pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
    conn = pg_hook.get_conn()
    # Get dag config
    dag_configs = fetch_dag_configs(conn)

    # Create dags
    for config in dag_configs:
        dag_id = f"{config['job_name']}"
        default_args = {
            'retries': 1
        }
        create_dag(dag_id, config['chain_description'], config['schedule'], config['sequence'],default_args,conn)      


def auto_generate_dag() -> DAG:
    default_args={
          'owner':'airflow'
    }
    dag = DAG(
        'job_auto_generate_dags',
        description="Автогенерация дагов по таблице chains",
        schedule_interval='* * * * *',
        start_date=datetime(2022, 12, 21, tz="Europe/Moscow"),
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=["proplum"]
    )
    with dag:
        start = EmptyOperator(task_id='start')

 
        task_gen = BashOperator(
            task_id="gen_dags_from_f_chains",
            #python_callable=test_func,
            bash_command="python3 /var/lib/airflow/dags/fw_generate_dags.py",
            dag=dag
        )
        start>>task_gen
        return dag


job_auto_generate_dag: DAG = auto_generate_dag()


