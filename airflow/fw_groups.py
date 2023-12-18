from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import TaskNotFound
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
from fw_constants import adwh_conn_id, max_parallel_tasks
from fw_tasks import gen_load_id, execute_function, calc_data, file_load_data
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

# Fill Required Information
files_path = str(Variable.get("files_path")) # directory with files on gpfdist server
gpfdist_port = str(Variable.get("gpfdist_port")) # gpfdist port
ADWH_PATH = files_path+"/&"
ADWH_BASH = f'/usr/local/greenplum-db-clients-6.20.0/bin/gpfdist -p {gpfdist_port} -d ' # command to start gpfdist server

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

            task_py = PythonOperator(
                task_id='f_pars_data',
                python_callable=calc_data,
                op_kwargs={"task_id": group_id + ".f_gen_load_id"},
                email=obj["responsible_mail"],
                email_on_failure=True,
                dag=dag
            )

            # start gpfdist
            task_gpfdist = BashOperator(
                task_id="f_gpfdist_start",
                bash_command=ADWH_BASH + ADWH_PATH,
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
                    prev_task >> task_py >> task_gpfdist >> task
                    prev_task = task
                else:
                    prev_task >> task
                    prev_task = task

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
                "from ${target_schema}.objects "
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
                "from ${target_schema}.objects "
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
            "from ${target_schema}.dependencies d "
            "inner join ${target_schema}.objects o  on d.object_id = o.object_id "
            "inner join ${target_schema}.objects o2 on d.object_id_depend = o2.object_id "
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
            "from ${target_schema}.objects "
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