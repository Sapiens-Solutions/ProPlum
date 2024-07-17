# -*- coding: utf-8 -*-

from pendulum import datetime, date, time
from datetime import datetime as dt

from airflow import DAG
from airflow.models import Variable
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
from fw_groups import create_simple_group, create_dependencies_groups,create_task_group
import logging
import re

import ast


pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
conn = pg_hook.get_conn()

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


def log_dag_config(chain_name,conn,**kwargs):
    # get DAG config
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    instance_id = None
    load_from = None
    load_to = None
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if 'instance_id' in conf:
             instance_id = conf['instance_id']
             cur.execute(f'''
                    select load_from,load_to from fw.chains_info where instance_id = {instance_id}
                    '''
                )
             record=cur.fetchall()[0]
             load_from=record['load_from']      
             load_to=record['load_to']       
        else:
            if 'load_from' in conf and 'load_to' in conf: 
                cur.execute(f'''
                    select fw.f_gen_instance_id('{chain_name}','{conf['load_from']}','{conf['load_to']}')
                    '''
                )
                load_from=conf['load_from']
                load_to=conf['load_to']
            else:  
                cur.execute(f'''
                    select fw.f_gen_instance_id('{chain_name}')
                    '''
                )
            instance_id=cur.fetchall()
            instance_id=instance_id[0]['f_gen_instance_id']
            conn.commit() 
        cur.execute(f'''
             select fw.f_set_instance_id_in_process({instance_id})
             '''
        ) 
    # save instance_id in XCom
    kwargs['ti'].xcom_push(key='instance_id', value=instance_id)

    # datetime into string

    load_from_str = load_from.isoformat() if isinstance(load_from, dt) else load_from
    load_to_str = load_to.isoformat() if isinstance(load_to, dt) else load_to
    print(load_from_str,load_to_str)
    kwargs['ti'].xcom_push(key='load_from', value=load_from_str)
    kwargs['ti'].xcom_push(key='load_to', value=load_to_str)

    logging.info(f"load_from = {load_from},load_to= {load_to}, instance_id = {instance_id}")


def log_dag_end(chain_name,conn,**kwargs):
    # finish dag
    instance_id = kwargs["ti"].xcom_pull(task_ids='log_dag_config', key='instance_id')
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f'''
             select fw.f_set_instance_id_success({instance_id})
             '''
        ) 
        conn.commit() 

    logging.info(f"instance_id = {instance_id}")



def fetch_dag_configs(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT chain_name, chain_description, schedule, job_name, sequence "
            "FROM fw.chains "
            "WHERE active "
            "ORDER BY chain_name"
        )
        return cur.fetchall()

def create_dag(config,default_args,conn):
    shedule = None if config['schedule'] =='None' else config['schedule']
    dag = DAG(
        config['job_name'],
        description= config['chain_description'],
        schedule_interval=shedule,
        start_date=datetime(2022, 12, 21, tz="Europe/Moscow"),
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=["proplum"]
    )
    parsed_chains = parse_chain(config['sequence'])
    with dag:
        # add tasks
        log_config_task = PythonOperator(
            task_id='log_dag_config',
            python_callable=log_dag_config,
            op_kwargs={'chain_name': config['chain_name'],'conn': conn},
            provide_context=True
        )
         # finish chain task
        log_task_end = PythonOperator(
            task_id='log_task_end',
            python_callable=log_dag_end,
            op_kwargs={'chain_name': config['chain_name'],'conn': conn},
            provide_context=True
        )
        groups = []
        # add branches
        task_branches = [[] for i in range(max_parallel_tasks)]
        task_branch_index = 0

        start = EmptyOperator(task_id='start')
        groups.append(start)
        groups.append(log_config_task)
        for i,group_ in enumerate(parsed_chains):
            if isinstance(group_,list):
                with TaskGroup(
                    group_id=str(i)+"_"+"_".join(map(str, group_)),
                    prefix_group_id=False,
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
                                        # add groups in branch
                                        task_branches[task_branch_index].append(task_group)
                                        task_branch_index += 1
                                        if task_branch_index == max_parallel_tasks:
                                            task_branch_index = 0
                                    # add connections
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
                                # add groups in branch
                                task_branches[task_branch_index].append(task_group)
                                task_branch_index += 1
                                if task_branch_index == max_parallel_tasks:
                                    task_branch_index = 0
                            # add connections
                for task_branch in task_branches:
                                prev = None
                                for task_group in task_branch:
                                    if prev is not None:
                                        prev >> task_group
                                    prev = task_group
                groups.append(task_group)

        groups.append(log_task_end)                           
        prev = None
        for group in groups:
            if prev is not None:
                prev >> group
            prev = group

    return dag


# get dags config
dag_configs = fetch_dag_configs(conn)

# Create DAGs from config
for config in dag_configs:
    dag_id = config['job_name']
    default_args = {
        'retries': 1
    }
    dags_list.append(create_dag(config,default_args,conn))
for i in range(len(dags_list)):
    exec(f'generated_dag{i+1}=dags_list[i]')
