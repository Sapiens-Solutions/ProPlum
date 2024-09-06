import re
from airflow.exceptions import AirflowFailException
from psycopg2.errors import Error
from psycopg2.sql import SQL, Identifier
from constants import adwh_conn_id
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

def gen_load_id(object_id: int,**kwargs):
    logging.info(f"Generate load id for object {object_id}")
    load_from = kwargs["ti"].xcom_pull(task_ids='log_dag_config', key='load_from')
    load_to = kwargs["ti"].xcom_pull(task_ids='log_dag_config', key='load_to')
    print(f"load_from= {load_from} , load_to = {load_to}")
    conn = None
    try:
        # create connection
        pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
        conn = pg_hook.get_conn()

        # execute function f_gen_load_id
        with conn.cursor() as cur:
            cur.execute("select fw.f_gen_load_id(%s,%s,%s)", (object_id,load_from,load_to))
            load_id = cur.fetchone()[0]

        # log messages
        for msg in conn.notices:
            logging.info(msg.strip())

        # close connection
        conn.commit()
        conn.close()

        # Catch errors
        if load_id is not None:
            logging.info(f"load_id generated: {load_id}")
        else:
            logging.error("Error while generate load_id")
            raise AirflowFailException

        return load_id

    except Error:
        logging.error("Errors in function f_gen_load_id")
        if conn is not None:
            conn.rollback()
            conn.close()
        # logging.error(err.args)
        raise AirflowFailException


def execute_function(func_name: str, task_id: int, **kwargs):
    logging.info("Get load_id from XCom")
    load_id = kwargs['ti'].xcom_pull(task_ids=task_id)

    if load_id is not None:
        logging.info(f"Received load_id: {load_id}")
    else:
        logging.info("Get load id from params")
        try:
            return kwargs['load_id']
        except KeyError:
            logging.error("Can't get load_id")
            raise AirflowFailException

    logging.info(f"Execute function {func_name} for load_id = {load_id}")

    conn = None
    try:
        # create connection
        pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
        conn = pg_hook.get_conn()

        # execute framework function
        with conn.cursor() as cur:
            cur.execute(
                SQL("select ${target_schema}.{func_name}(%s)").format(
                    func_name=Identifier(func_name)
                ),
                (load_id,)
            )
            ok = cur.fetchone()[0]

        # log messages
        for msg in conn.notices:
            logging.info(msg.strip())

        # close connection
        conn.commit()
        conn.close()

        # Catch errors
        if ok:
            logging.info(f"Completed successful")
        else:
            logging.error(f"Error in function {func_name}")
            raise AirflowFailException

    except Error:
        logging.error(f"Error in function {func_name}")
        if conn is not None:
            conn.rollback()
            conn.close()
        # logging.error(err.args)
        raise AirflowFailException

def calc_data(task_id: int, **kwargs):
    logging.info("Receiving load_id from XCom")
    load_id = kwargs['ti'].xcom_pull(task_ids=task_id)

    if load_id is not None:
        logging.info(f"Received load_id: {load_id}")
    else:
        # для отладки
        logging.info("Receiving load_id from function")
        try:
            return kwargs['load_id']
        except KeyError:
            logging.error("Can't get load id")
            raise AirflowFailException

    pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
    conn = pg_hook.get_conn()

    # execute function
    with conn.cursor() as cur:
        cur.execute(
            (f"""select o.load_function_name
                    from ${target_schema}.objects o
                    join ${target_schema}.load_info l  on
                    o.object_id = l.object_id 
                    where l.load_id  = {load_id}""")
        )
        function_name = cur.fetchone()[0].replace('$load_id', str(load_id))
        args = re.search(r'\(.+\)', function_name)  # Extract params from function
        function_name = re.sub(r'\(.+\)', '', function_name)
        function_name = function_name.split('.') 
        print(function_name[0], function_name[1])
        if args:#check args exists
            args = args[0].replace('(', '').replace(')', '').split(',')
        else:
            args = []
        mod = __import__(function_name[0], fromlist=[function_name[1]])#import module
        py_function = getattr(mod, function_name[1])#import function
        print("Function: ",py_function, " Args: ",*args)
        py_function(*args)#add args in function call


def file_load_data(task_id: int, **kwargs):
    logging.info("Receiving load_id XCom")
    load_id = kwargs['ti'].xcom_pull(task_ids=task_id)

    if load_id is not None:
        logging.info(f"Received load_id: {load_id}")
    else:

        logging.info("Receiving load_id from function")
        try:
            return kwargs['load_id']
        except KeyError:
            logging.error("Can't get load_id")
            raise AirflowFailException

    pg_hook = PostgresHook(postgres_conn_id=adwh_conn_id)
    conn = pg_hook.get_conn()

    # execute function
    with conn.cursor() as cur:
        cur.execute(
            (f"""select o.load_function_name
                    from ${target_schema}.objects o
                    join ${target_schema}.load_info l  on
                    o.object_id = l.object_id 
                    where l.load_id  = {load_id}""")
        )
        function_name = cur.fetchone()[0].replace('$load_id', str(load_id))
        logging.info(f"Execute function {function_name} for load_id = {load_id}")
        # eval(func_name)()

        # log messages
        for msg in conn.notices:
            logging.info(msg.strip())

        # Close connection
        conn.commit()
        conn.close()
    #Call module
    args = re.search(r'\(.+\)', function_name)  #Extract args from function
    function_name = re.sub(r'\(.+\)', '', function_name)
    function_name = function_name.split('.')  # split on module and function
    print(function_name[0], function_name[1])
    if args:  # check if args exists
        args = args[0].replace('(', '').replace(')', '').split(',')
    else:
        args = []
    mod = __import__(function_name[0], fromlist=[function_name[1]])  # module import
    py_function = getattr(mod, function_name[1])  # function import
    print("Function: ", py_function, " Args: ", *args)
    py_function(*args, load_id=load_id)  #add args in function call

