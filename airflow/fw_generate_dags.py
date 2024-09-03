# -*- coding: utf-8 -*-

from pendulum import datetime, date, time
from datetime import datetime as dt

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from fw_constants import adwh_conn_id, max_parallel_tasks
from psycopg2.extras import RealDictCursor
from fw_groups import create_simple_group, create_dependencies_groups,create_task_group
import logging
import re



pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
conn = pg_hook.get_conn()

def process_string(input_string):
    """
    Parse chain like '[1,2]>>3>>4' into list
    """
    # split chain by '>>'
    pattern = r'(\d*)(>>)(\d*)'
    
    def replacer(match):
        left = match.group(1)
        right = match.group(3)
        
        if left:
            left = f'[{left}]'
        if right:
            right = f'[{right}]'
        
        # replace '>>' on ','
        return f'{left},{right}'
    
    try: 
      if isinstance(eval(re.sub(pattern, replacer, input_string)),list):
        return check_result(eval(re.sub(pattern, replacer, input_string)))
      else:
        return check_result(eval('['+re.sub(pattern, replacer, input_string)+']'))
    except Exception as ex:
      return check_result(eval('['+re.sub(pattern, replacer, input_string)+']'))
def check_result(lst):
    for i in lst:
         if not isinstance(i,list):
              return [lst]
    return lst


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

def parse_in_group(group_,dag,task_branches,task_branch_index,groups):
    # 2nd level
    gr_id = str(group_)
    with TaskGroup(
        group_id=gr_id,
        tooltip='auto_generate',
        dag=dag,
        prefix_group_id=False
            ) as task_group1:
        for object in group_:
            if isinstance(object,list):  # [1,[[1],[2],[3]]],  [[1],[2],[3]]
                with TaskGroup(
                    group_id=str(object),
                    tooltip='auto_generate',
                    dag=dag,
                    prefix_group_id=False,
                    parent_group=task_group1,
                        ) as task_group2:
                    all_tasks = []
                    for obj1 in object: # [1], [2],[3]
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute(
                            "select object_id, object_name, object_desc, load_method, responsible_mail "
                            "from fw.objects "
                            "where "
                            "object_id = %s ",
                            (int(obj1[0]),))
                            objects = cur.fetchall()
                        #all_tasks = []
                        for obj in objects:
                                        task_group=create_task_group(  
                                        obj=obj,
                                        dag=dag,
                                        parent_group=task_group2    
                                )
                                        all_tasks.append(task_group)
                    
                    # dependencies
                    for i in range(1, len(all_tasks)):
                        all_tasks[i-1] >> all_tasks[i]       

            else:  # if only one object
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                    "select object_id, object_name, object_desc, load_method, responsible_mail "
                    "from fw.objects "
                    "where "
                    "object_id = %s ",
                    (int(object),))
                    objects = cur.fetchall()
             #   all_tasks = []
                for obj in objects:
                                task_group=create_task_group(  
                                obj=obj,
                                dag=dag,
                                parent_group=task_group1     
                        )
             #                   all_tasks.append(task_group)
             #   for i in range(1, len(all_tasks)):
             #       all_tasks[i-1] >> all_tasks[i]                    
                                # добавление группы заданий в ветку
                                task_branches[task_branch_index].append(task_group)
                                task_branch_index += 1
                                if task_branch_index == max_parallel_tasks:
                                    task_branch_index = 0
                            # create branches 
                for task_branch in task_branches:
                                prev = None
                                for task_group in task_branch:
                                    if prev is not None:
                                        prev >> task_group
                                    prev = task_group
        groups.append(task_group1)
        return groups,task_branch_index

def parse_one(object,dag,task_branches,task_branch_index,groups,parent_group=None):
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
                        # add group in branch
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
        return groups,task_branch_index

def create_group(parsed_chains,dag,task_branches,groups,task_branch_index,log_task_end):
   # cur_groups = []
   # prev_elem  = groups[-1]  # previous elem before current group
    for i in parsed_chains:
        # [2,3,4]
        if isinstance(i,list) and len(i)==1 and not isinstance(i[0],list):
             groups,task_branch_index=parse_one(i[0],dag,task_branches,task_branch_index,groups)
             groups[-2]>>groups[-1]
        else:
            
            groups,task_branch_index = parse_in_group(i,dag,task_branches,task_branch_index,groups)
        '''
        if isinstance(i,list) and not isinstance(i[0],list):
             groups,task_branch_index=parse_one(i[0],dag,task_branches,task_branch_index,groups)
             groups[-2]>>groups[-1]
             flag = True  # Следующий элемент - ДОЛЖЕН СОЕДИНЯТСЯ ЧЕРЕЗ >>
             # Пример:  1>>2  => [[1],[2]]  => 1>>2
             # Если у нас обычный id загрузки, но это список => операция >>.
             # [1,2,3]   []
             # [[1],[2],3] - это некорректно. должно быть: [[[1],[2]],3] - группы выделяются ЯВНО.
        elif not isinstance(i,list):
            groups,task_branch_index = parse_in_group(parsed_chains,dag,task_branches,task_branch_index,groups)

            # Очень упрощенно - если там есть элементы типа 1,2,3
            break            
             # если текущее значение просто ОДИН ЭЛЕМЕНТ, добавление в текущую группу.
            cur_groups,task_branch_index=parse_one(i,dag,task_branches,task_branch_index,cur_groups)

        elif isinstance(i,list) and isinstance(i[0],list):
             # Тут нужно создавать подгруппу.
             create_group(i[0],dag,task_branches,groups,task_branch_index,log_task_end)
             if flag:  # Если элементы должны соединится последовательно:
                  groups[-2]>>groups[-1]
             # Список и элемент тоже список, пример:  [1,[[2],[3]]] => т.е. 1,[2>>3].
             # 1 >> [2,3]   [[1],[[2,3]]]
             # это параллельное выполнение двух тасок.
        '''
    return groups,task_branch_index
# 
def create_groups_recursive(parsed_chains,dag,task_branches,groups,task_branch_index,log_task_end):
    #parsed_chains= [[1],[[[596],[597]],[[598],[612],[613]],[[600],[610]]],[2]]#[7,[[2],[3],[6]]]
    #parsed_chains=[[1],[2,3,4],[5]]
    #parsed_chains=[[1],[2,3,4]]
    # 1>>[[596>>597],[598>>612>>613],[600>>610]]
    # [[[596],[597]],[[598],[612],[613]],[[600],610]]]
    groups,task_branch_index = create_group(parsed_chains,dag,task_branches,groups,task_branch_index,log_task_end)
    #parse_in_group(parsed_chains,dag,task_branches,task_branch_index,groups)

    groups.append(log_task_end)
                              
    prev = None
    for group in groups:
        if prev is not None:
            prev >> group
        prev = group


def create_dag(config,default_args,conn):
    shedule = None if config['schedule'] =='None' else config['schedule']
    dag = DAG(
        config['job_name'],
        description= config['chain_description'],
        schedule=shedule,
        start_date=datetime(2022, 12, 21, tz="Europe/Moscow"),
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=["proplum"]
    )
    parsed_chains = process_string(config['sequence'])
    print('PARSED chain=',parsed_chains)
    with dag:
        # start dag
        log_config_task = PythonOperator(
            task_id='log_dag_config',
            python_callable=log_dag_config,
            op_kwargs={'chain_name': config['chain_name'],'conn': conn},
            provide_context=True
        )
         # end dag
        log_task_end = PythonOperator(
            task_id='log_task_end',
            python_callable=log_dag_end,
            op_kwargs={'chain_name': config['chain_name'],'conn': conn},
            provide_context=True
        )
        groups = []
        # создание веток заданий
        task_branches = [[] for i in range(max_parallel_tasks)]
        task_branch_index = 0

        start = EmptyOperator(task_id='start')
        groups.append(start)
        groups.append(log_config_task)
        
        create_groups_recursive(parsed_chains,dag,task_branches,groups,task_branch_index,log_task_end)   

    return dag


# get dags config
dag_configs = fetch_dag_configs(conn)

# Create DAGs from config
for config in dag_configs:
    dag_id = config['job_name']
    default_args = {
        'retries': 1
    }
    create_dag(config,default_args,conn)
