from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import TaskNotFound
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
from fw_constants import adwh_conn_id, max_parallel_tasks
from fw_tasks import gen_load_id, execute_function, calc_data, file_load_data, kill_gpfdist_ssh
from fw_tasks import prepare_pipe, start_gpfdist_ssh
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from fw_odata import load_odata_data
import clickhouse_connect as ch_conn
from psycopg2.errors import Error

from fw_constants import ssh_conn_id

import logging
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import time
from airflow.exceptions import AirflowFailException

import datetime

files_path = str(Variable.get("files_path"))  # directory with files on gpfdist server
gpfdist_port = str(Variable.get("gpfdist_port"))  # gpfdist port
ADWH_PATH = files_path + "/&"
inner_pipes_dir = str(Variable.get("inner_pipes_dir"))
odata_page_size = int(Variable.get("odata_page_size"))
odata_max_workers = int(Variable.get("odata_max_workers"))
odata_delimeter = str(Variable.get("odata_delimeter"))


# task group for single object
def create_task_group(
        obj,
        dag: DAG,
        parent_group: TaskGroup
) -> TaskGroup:
    group_id = str(obj["object_id"]) + "_" + str(obj["object_name"]).replace(".", "_").replace("$", "")

    with TaskGroup(
            group_id=group_id,
            parent_group=parent_group,
            tooltip=str(obj["object_desc"]),
            dag=dag
    ) as task_group:

        # task to generate load_id
        task_gen = PythonOperator(
            task_id="f_gen_load_id",
            python_callable=gen_load_id,
            op_kwargs={"object_id": obj["object_id"]},
            email=obj["responsible_mail"],
            email_on_failure=True,
            dag=dag
        )

        # tasks for load data
        if obj["load_method"] == "pxf" or obj["load_method"] == "gpfdist":
            functions = ["f_prepare_load", "f_extract_data", "f_load_data"]
        elif obj['load_method'] == 'odata':
            functions = ['f_prepare_load', 'f_prepare_extract_odata']
            post_functions = ['f_post_extract_odata', 'f_load_data']
        elif obj['load_method'] == 'pxf_ch':
            functions = ["f_prepare_load_to_ch", "f_extract_data_for_ch"]
        else:
            functions = ["f_load_object"]

        if obj["load_method"] == "python":

            task_csv = PythonOperator(
                task_id='f_python_load_data',
                python_callable=file_load_data,
                op_kwargs={"task_id": group_id + ".f_gen_load_id"},
                email=obj["responsible_mail"],
                email_on_failure=True,
                retries=1,
                dag=dag
            )

            prev_task = task_gen
            if prev_task == task_gen:
                prev_task >> task_csv

        elif obj["load_method"] == "gpfdist":

            task_gpfdist = PythonOperator(
                task_id="f_gpfdist_start",
                python_callable=start_gpfdist_ssh,
                op_kwargs={'ssh_conn': ssh_conn_id, 'gpfdist_dir': files_path, 'gpfdist_port': gpfdist_port},
                dag=dag
            )

            prev_task = task_gen
            for func_name in functions:
                task = PythonOperator(
                    task_id=func_name,
                    python_callable=execute_function,
                    op_kwargs={"func_name": func_name, "task_id": group_id + ".f_gen_load_id"},
                    email=obj["responsible_mail"],
                    email_on_failure=True,
                    dag=dag
                )
                if prev_task == task_gen:
                    # prev_task >> task_py >> task_gpfdist >> task
                    prev_task >> task_gpfdist >> task
                    prev_task = task
                else:
                    prev_task >> task
                    prev_task = task

        elif obj["load_method"] == "odata":
            # task_gpfdist = PythonOperator(
            #     task_id='f_gpfdist_start',
            #     python_callable=start_gpfdist_ssh,
            #     op_kwargs={'ssh_conn': ssh_conn_id, 'gpfdist_dir': files_path, 'gpfdist_port': gpfdist_port},
            #     dag=dag
            # )
            prev_task = task_gen
            inner_pipe_path = f'{inner_pipes_dir}/{group_id}'
            outer_pipe_path = f'{files_path}/{group_id}'

            task_create_pipe = PythonOperator(
                task_id='pipe_preparation',
                python_callable=prepare_pipe,
                op_kwargs={'inner_pipe_path': inner_pipe_path},
                dag=dag
            )
            #функции для подготовки к загрузке, выполняющиеся до цикоической загрузки 
            for func_name in functions:
                task = PythonOperator(
                    task_id=func_name,
                    python_callable=execute_function,
                    op_kwargs={"func_name": func_name, "task_id": group_id + ".f_gen_load_id"},
                    email=obj["responsible_mail"],
                    email_on_failure=True,
                    dag=dag
                )
                if prev_task == task_gen:
                    #prev_task >> task_create_pipe >> task_gpfdist >> task
                    prev_task >> task_create_pipe >> task
                    prev_task = task
                else:
                    prev_task >> task
                    prev_task = task

            object_id_for_odata = str(obj['object_id'])
            odata_service = get_odata_service(obj_id=object_id_for_odata)

            task_extract_data_from_odata = PythonOperator(
                task_id='task_extracting_from_odata',
                python_callable=load_odata_data,
                op_kwargs={
                    'mode': 'delta', #DELETE
                    'page_size': odata_page_size,
                    'odata_service': odata_service,  # Запрос из бд
                    'load_func': f'fw.f_process_extract_odata', # id загрузки получаеться внутри функции
                    "task_id": group_id + ".f_gen_load_id",
                    'pipe_path': inner_pipe_path,
                    'delimeter': odata_delimeter,
                    'max_workers': odata_max_workers,
                    'odata_base_url': 'sap/opu/odata/sap'
                },
                dag=dag
            )
            task >> task_extract_data_from_odata
            task = task_extract_data_from_odata

            prev_task = task_extract_data_from_odata

            for func_name in post_functions:
                task = PythonOperator(
                    task_id=func_name,
                    python_callable=execute_function,
                    op_kwargs={"func_name": func_name, "task_id": group_id + ".f_gen_load_id"},
                    email=obj["responsible_mail"],
                    email_on_failure=True,
                    dag=dag
                )
                if prev_task == task_gen:
                    prev_task >> task
                    prev_task = task
                else:
                    prev_task >> task
                    prev_task = task

        elif obj["load_method"] == "pxf_ch":
            prev_task = task_gen
            for func_name in functions:
                if func_name == "f_extract_data_for_ch":
                    task_ch_1 = PythonOperator(
                        task_id='truncate_target_table_in_CH',
                        python_callable=truncate_ch_table,
                        op_kwargs={"object_name": obj["object_name"]},
                        dag=dag
                    )
                    prev_task >> task_ch_1
                    prev_task = task_ch_1

                task = PythonOperator(
                    task_id=func_name,
                    python_callable=execute_function,
                    op_kwargs={"func_name": func_name, "task_id": group_id + ".f_gen_load_id"},
                    email=obj["responsible_mail"],
                    email_on_failure=True,
                    dag=dag
                )
                prev_task >> task
                prev_task = task

                if func_name == "f_extract_data_for_ch":
                    task_ch_2 = PythonOperator(
                        task_id='move_data_from_STG_to_ODS_layer_CH',
                        python_callable=move_data_ch,
                        op_kwargs={"object_name": obj["object_name"], "task_id": group_id + ".f_gen_load_id"},
                        dag=dag
                    )
                    prev_task >> task_ch_2

        else:
            prev_task = task_gen
            for func_name in functions:
                task = PythonOperator(
                    task_id=func_name,
                    python_callable=execute_function,
                    op_kwargs={"func_name": func_name, "task_id": group_id + ".f_gen_load_id"},
                    email=obj["responsible_mail"],
                    email_on_failure=True,
                    dag=dag
                )

                prev_task >> task
                prev_task = task

        return task_group


# create group tasks without dependencies
def create_simple_group(
        load_group: str,
        tooltip: str,
        dag: DAG,
        object_id_from: int = 0,
        object_id_to: int = 1000000
) -> TaskGroup:
    # create branches
    task_branches = [[] for i in range(max_parallel_tasks)]
    task_branch_index = 0

    # create connections
    pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
    conn = pg_hook.get_conn()

    # read objects to load
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # if group = single_object_group, then single object load
        if load_group != "single_object_group":
            cur.execute(
                "select object_id, object_name, object_desc, load_method, responsible_mail "
                "from fw.objects "
                "where "
                "load_group = %s "
                "and object_id >= %s "
                "and object_id <= %s "
                "and active is true "
                "order by object_id",
                (load_group, object_id_from, object_id_to)
            )
        else:
            cur.execute(
                "select object_id, object_name, object_desc, load_method, responsible_mail "
                "from fw.objects "
                "where "
                "object_id >= %s "
                "and object_id <= %s "
                # "and active is true "
                "order by object_id",
                (object_id_from, object_id_to)
            )
        objects = cur.fetchall()

    # close connection
    conn.close()

    if not objects:
        return None

    with TaskGroup(
            group_id=load_group.lower(),
            prefix_group_id=False,
            tooltip=tooltip,
            dag=dag,
    ) as load_task_group:

        for obj in objects:
            # task group for functions
            task_group = create_task_group(
                obj=obj,
                dag=dag,
                parent_group=load_task_group
            )

            # add tasks group into branch
            task_branches[task_branch_index].append(task_group)
            task_branch_index += 1
            if task_branch_index == max_parallel_tasks:
                task_branch_index = 0

        # kill gpfdist before loading
        killing_gpfdist = PythonOperator(
            task_id=f"kill_gpfdist_{load_group.lower()}",
            python_callable=kill_gpfdist_ssh,
            op_kwargs={'ssh_conn': ssh_conn_id, 'gpfdist_port': gpfdist_port, 'gpfdist_dir': files_path},
            dag=dag
        )
        # start gpfdist before loading
        starting_gpfdist = PythonOperator(
            task_id=f"start_gpfdist_{load_group.lower()}",
            python_callable=start_gpfdist_ssh,
            op_kwargs={'ssh_conn': ssh_conn_id, 'gpfdist_port': gpfdist_port, 'gpfdist_dir': files_path},
            dag=dag
        )
        killing_gpfdist >> starting_gpfdist
        for task_branch in task_branches:
            if task_branch:
                #killing_gpfdist >> task_branch[0]
                starting_gpfdist >> task_branch[0]

        # create connection between tasks groups
        for task_branch in task_branches:
            prev = None
            for task_group in task_branch:
                if prev is not None:
                    prev >> task_group
                prev = task_group

    return load_task_group


# tasks groups with dependencies
def create_dependencies_groups(
        load_group: str,
        tooltip: str,
        dag: DAG,
        object_id_from: int = 0,
        object_id_to: int = 1000000
) -> list:
    # create connection
    pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
    conn = pg_hook.get_conn()

    # read dependencies
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select t.object_id, t.object_id_depend "
            "from ( select o.object_id, case when o.active is true then 1 else 0 end as active1, "
            "min(case when o2.active is true then 1 else 0 end) as active2, d.object_id_depend "
            "from fw.dependencies d "
            "inner join fw.objects o  on d.object_id = o.object_id "
            "inner join fw.objects o2 on d.object_id_depend = o2.object_id "
            "group by o.object_id, d.object_id_depend ) t "
            "where t.active1 = 1 and t.active2 = 1 "
        )
        deps = cur.fetchall()

    # create list with dependencies
    dep_ids = set()
    for dep in deps:
        dep_ids.add(dep["object_id"])
        dep_ids.add(dep["object_id_depend"])

    # read objects
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select object_id, object_name, object_desc, load_method, responsible_mail "
            "from fw.objects "
            "where load_group = %s "
            "and object_id >= %s "
            "and object_id <= %s "
            "and active is true "
            "order by object_id",
            (load_group, object_id_from, object_id_to)
        )
        objects = cur.fetchall()

    # close connection
    conn.close()

    if not objects:
        return None

    # without dependencies
    load_task_group_nodep = TaskGroup(
        group_id=load_group.lower() + "_nodep",
        # parent_group=load_task_group,
        prefix_group_id=False,
        tooltip=tooltip,
        dag=dag,
    )

    # with dependencies
    load_task_group_dep = TaskGroup(
        group_id=load_group.lower() + "_dep",
        # parent_group=load_task_group,
        prefix_group_id=False,
        tooltip=tooltip,
        dag=dag,
    )

    task_groups_nodeps = {}
    task_groups_deps = {}

    for obj in objects:
        parent_group = load_task_group_dep if obj["object_id"] in dep_ids else load_task_group_nodep

        # create tasks group
        task_group = create_task_group(
            obj=obj,
            dag=dag,
            parent_group=parent_group
        )

        # add to group
        if obj["object_id"] in dep_ids:
            task_groups_deps[obj["object_id"]] = task_group
        else:
            task_groups_nodeps[obj["object_id"]] = task_group

    # create connection with dependencies
    for dep in deps:
        task_id = dep["object_id"]
        task_id_dep = dep["object_id_depend"]

        try:
            task_groups_deps[task_id] << task_groups_deps[task_id_dep]
        except KeyError:
            raise TaskNotFound

    # create branches without dependencies
    task_branches = [[] for i in range(max_parallel_tasks)]
    task_branch_index = 0

    # add to branches without dependencies
    for task_group in task_groups_nodeps.values():
        task_branches[task_branch_index].append(task_group)
        task_branch_index += 1
        if task_branch_index == max_parallel_tasks:
            task_branch_index = 0

    # create connection without dependencies
    for task_branch in task_branches:
        prev = None
        for task_group in task_branch:
            if prev is not None:
                prev >> task_group
            prev = task_group

    return [load_task_group_nodep, load_task_group_dep]


def truncate_ch_table(object_name: str):
    logging.info(f"Start truncate table {object_name} in ClickHouse")
    command = ''
    try:
        client = ch_conn.get_client(host='10.2.0.141', port=8123, username='s.shushkov',
                                    password='3xmgUH9M8bhV2cXldBUo')
        object_name = object_name.split('.')[1]
        #command = f"truncate md_dwh.{object_name};"
        command = f"truncate STG.stg_{object_name};"
        client.command(command)
    except Exception as e:
        logging.error(f"Произошла ошибка при очищении таблицы {object_name} в ClickHouse!!!!\nСкрипт {command}")
        raise AirflowFailException(e)
    logging.info(f"End truncate table {object_name} in ClickHouse")


def move_data_ch(object_name: str, task_id: int, **kwargs):
    logging.info(f"Start move data from STG to ODS layer for table {object_name}")
    load_id = kwargs['ti'].xcom_pull(task_ids=task_id)
    print(f"load_id= {load_id}")
    command = ''
    conn = None
    try:
        # create connection
        pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
        conn = pg_hook.get_conn()

        logging.info(f"Get load_type and delta_field for object {object_name.split('.')[1]}")
        with conn.cursor() as cur:
            cur.execute(f"select object_id from fw.load_info where load_id = {load_id}")
            obj_id = cur.fetchone()[0]
            cur.execute(f"select load_type, coalesce(array_to_string(merge_key, ','), '') as merge_key from fw.objects where object_id = {obj_id}")
            row = cur.fetchone()
            load_type, merge_key = row
            print(f"load_type= {load_type}")
            print(f"merge_key= {merge_key}")
            if load_type == 'UPDATE_PARTITION':
                cur.execute(f"select columnname from pg_catalog.pg_partition_columns where tablename = '{object_name.split('.')[1]}'")
                part_key = cur.fetchone()[0]
                print(f"part_key= {part_key}")
                cur.execute(f"select left(replace(min({part_key})::text, '-', ''),6), left(replace(max({part_key})::text, '-', ''),6) from stg_ods.delta_{object_name.split('.')[1]}_1{str(obj_id)[1:]}")
                row = cur.fetchone()
                min_part_key, max_part_key = row
                print(f"min_part_key= {min_part_key}")
                print(f"max_part_key= {max_part_key}")

        # log messages
        for msg in conn.notices:
            logging.info(msg.strip())

        try:
            client = ch_conn.get_client(host='10.2.0.141', port=8123, username='s.shushkov',
                                        password='3xmgUH9M8bhV2cXldBUo')
            object_name = object_name.split('.')[1]
            command = f"select arrayStringConcat(groupArray(name), ', ') from system.columns where table = '{object_name}' and database = 'ODS'"
            columns = client.command(command)   
            print(f"columns= {columns}")         

            if load_type == 'FULL':
                command = f"truncate ODS.{object_name};"
                client.command(command)
                command = f"insert into ODS.{object_name} \nselect * from STG.stg_{object_name};"
                client.command(command)
            elif load_type == 'DELTA_UPSERT':
                command = f"delete from ODS.{object_name} \nwhere ({merge_key}) in (select {merge_key} from STG.stg_{object_name});"
                client.command(command)
                command = f"insert into ODS.{object_name} \nselect * from STG.stg_{object_name};"
                client.command(command)
            elif load_type == 'UPDATE_PARTITION':
                command = f"DROP TABLE IF EXISTS STG.tmp_{object_name};"
                client.command(command)
                command = f"CREATE TABLE IF NOT EXISTS STG.tmp_{object_name} AS ODS.{object_name} ENGINE = MergeTree() PARTITION BY toYYYYMM({part_key}) ORDER BY ({merge_key}) SETTINGS allow_nullable_key = 1;"
                client.command(command)
                start_month = min_part_key
                end_month = max_part_key
                start_year = int(start_month[:4])
                start_month_num = int(start_month[4:])
                end_year = int(end_month[:4])
                end_month_num = int(end_month[4:])
                for year in range(start_year, end_year + 1):
                    for month in range(1, 13):
                        if year == start_year and month < start_month_num:
                            continue
                        if year == end_year and month > end_month_num:
                            break
                        command = f"insert into STG.tmp_{object_name} \nselect {columns} from (select q.*, rank() over (partition by {merge_key} order by rnk) as rnk_f FROM (select *, 1 rnk from STG.stg_{object_name} f where {part_key} >= toDate(concat('{year}{month:02d}','01')) and {part_key} < addMonths(toDate(concat('{year}{month:02d}','01')),1) UNION ALL SELECT *, 2 rnk FROM ODS.{object_name} t where _partition_id = '{year}{month:02d}' ) q ) qr WHERE rnk_f = 1;"
                        client.command(command)
                        command = f"alter table ODS.{object_name} replace partition ({year}{month:02d}) from STG.tmp_{object_name};"
                        client.command(command)
                        command = f"truncate table STG.tmp_{object_name};"
                        client.command(command)
                command = f"DROP TABLE STG.tmp_{object_name};"
                client.command(command)

            logging.info(f"Update load_status for load {load_id}")
            # execute function f_set_load_id_success
            with conn.cursor() as cur:
                cur.execute(f"select fw.f_set_load_id_success({load_id})")
                load_id = cur.fetchone()[0]

            logging.info(f"End move data in CH")

        except Exception as e:
            logging.error(
                f"Error during loading data for object {object_name}!!!!\n"
                f"Script: {command}")
            logging.info(f"Update load_status for load {load_id}")
            # execute function f_set_load_id_success
            with conn.cursor() as cur:
                cur.execute(f"select fw.f_set_load_id_error({load_id})")
                load_id = cur.fetchone()[0]
            raise AirflowFailException(e)

        # close connection
        conn.commit()
        conn.close()

        # Catch errors
        if load_id is not None:
            logging.info(f"load_id generated: {load_id}")
        else:
            logging.error("Error while f_set_load_id_success")
            raise AirflowFailException

        return load_id

    except Error:
        logging.error("Errors in function f_set_load_id_success")
        if conn is not None:
            conn.rollback()
            conn.close()
        # logging.error(err.args)
        raise AirflowFailException
# #changed

def get_odata_service(obj_id):
    pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
    conn = pg_hook.get_conn()

    # read objects to load
    with conn.cursor() as cur:
        cmd = f"select additional from fw.ext_tables_params where object_id = {obj_id} "
        cur.execute(cmd)
        objects = cur.fetchall()

    # close connection
    conn.close()
    return objects[0][0]





######################################3

def kill_gpfdist_ssh_predefined(load_id=None):
        ssh_conn_local = ssh_conn_id
        gpfdist_port_local = gpfdist_port
        gpfdist_dir_local = files_path
        kill_gpfdist_ssh(
            ssh_conn=ssh_conn_local,
            gpfdist_port=gpfdist_port_local,
            gpfdist_dir=gpfdist_dir_local ## Не испоьзуеться можно любую строку подать   
        )

########################################
########################################
def start_gpfdist_ssh_predefined(load_id=None):
        ssh_conn_local = ssh_conn_id
        gpfdist_port_local = gpfdist_port
        gpfdist_dir_local = files_path
        start_gpfdist_ssh(
            ssh_conn=ssh_conn_local,
            gpfdist_port=gpfdist_port_local,
            gpfdist_dir=gpfdist_dir_local   
        )



#######################################