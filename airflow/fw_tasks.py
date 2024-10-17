import re
from airflow.exceptions import AirflowFailException
from psycopg2.errors import Error
from psycopg2.sql import SQL, Identifier
from fw_constants import adwh_conn_id
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import time
import subprocess
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

def gen_load_id(object_id: int, **kwargs):
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
                SQL("select fw.{func_name}(%s)").format(
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
                    from fw.objects o
                    join fw.load_info l  on
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
                    from fw.objects o
                    join fw.load_info l  on
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





#changed
def prepare_pipe(inner_pipe_path):
    logging.info('begin creating pipe')
    if os.access(inner_pipe_path, os.F_OK):
        logging.info(f"Pipe {inner_pipe_path} уже существует")
    else:
        res = subprocess.run(['mkfifo', inner_pipe_path], capture_output=True)
        if res.returncode == 0:
            logging.info(f"Pipe {inner_pipe_path} создан")
        else:
            logging.info(f"Ошибка при создании pipe {inner_pipe_path} код возврата {res.returncode}, ошибка {res.stderr}")

    if not os.access(inner_pipe_path, os.W_OK):
        logging.error(f"Pipe {inner_pipe_path} не доступен для записи")
        raise AirflowFailException
    


def start_gpfdist_ssh(ssh_conn, gpfdist_dir, gpfdist_port, run = True ):
    hook = SSHHook(ssh_conn_id = ssh_conn)
    conn = hook.get_conn()
    logging.info('---getted connector----')
    cmd = f"ps -aux | grep someprocessnamethatwillneverexist\n"
    stdin, stdout, stderr = conn.exec_command(cmd)
    res = stdout.read()
    logging.info(res)
    logging.info(stderr.read())
    base_len = len(res.decode().split('\n'))
    logging.info(f'Base length - {base_len}')
    cmd = f"ps -aux | grep 'gpfdist -p {gpfdist_port} -d {gpfdist_dir}'\n"
    stdin, stdout, stderr = conn.exec_command(cmd)
    res = stdout.read()
    logging.info(res)
    current_port_and_dir_len = len(res.decode().split('\n'))
    logging.info(f'Right directry length - {current_port_and_dir_len}')

    cmd = f"ps -aux | grep 'gpfdist -p {gpfdist_port}'\n"
    stdin, stdout, stderr = conn.exec_command(cmd)
    res = stdout.read()
    logging.info(res)
    current_port_wrong_dir_len = len(res.decode().split('\n'))
    logging.info(f'Right port length, all directories - {current_port_wrong_dir_len}')
    if current_port_and_dir_len > base_len:
        logging.info('gpfdist уже запущен на нужном порте и в нужной директории')
        return
    
    time.sleep(2) 
    if current_port_wrong_dir_len > current_port_and_dir_len:
        logging.info(f'gpfdist Запущен в другой директории.\nОстанавливаю gpfdist в неверной директории на порту: {gpfdist_port}')

        cmd = f"kill -9 $(ps -aux | grep 'gpfdist -p {gpfdist_port}' | awk '{{print $2}}')\n"
        stdin, stdout, stderr = conn.exec_command(cmd)
        logging.info(stdout.read())
        logging.info(stderr.read())
        time.sleep(2) 
    cmd = f"ps -aux | grep 'gpfdist -p {gpfdist_port} -d {gpfdist_dir}'\n"
    logging.info(f"Запуск gpfdist комманда:{cmd}")
    stdin, stdout, stderr = conn.exec_command(cmd)
    out = stdout.read()
    initial_len = len(out.decode().split('\n'))
    logging.info(str([initial_len,out]))
    time.sleep(2) 
    if run:
        cmd = f'gpfdist -p {gpfdist_port} -d {gpfdist_dir} &'
        stdin, stdout, stderr = conn.exec_command(cmd)
        time.sleep(2)    
    cmd = f"ps -aux | grep 'gpfdist -p {gpfdist_port} -d {gpfdist_dir}'"
    stdin, stdout, stderr = conn.exec_command(cmd)
    out = stdout.read()
    end_len = len(out.decode().split('\n'))
    logging.info(str([end_len,out]))
    if (end_len <= initial_len):
        raise AirflowFailException(f'gpfdist не был запущен.\n Вероятно порт уже занят или нет прав\n Попробуйте запустить {cmd} вручную')
    

def kill_gpfdist_ssh(ssh_conn, gpfdist_dir, gpfdist_port, run = True ):
    hook = SSHHook(ssh_conn_id = ssh_conn)
    conn = hook.get_conn()
    logging.info('---getted connector----')
    cmd = f"ps -aux | grep someprocessnamethatwillneverexist\n"
    stdin, stdout, stderr = conn.exec_command(cmd)
    res = stdout.read()
    logging.info(res)
    logging.info(stderr.read())
    base_len = len(res.decode().split('\n'))
    logging.info(f'Base length - {base_len}')
    
    time.sleep(2) 
    #cmd = f"kill -9 $(ps -aux | grep 'gpfdist -p {gpfdist_port}' | awk '{{print $2}}')\n\n\n"
    cmd = f"ps -aux | grep 'gpfdist -p {gpfdist_port}' | awk '{{print $2}}'"
    stdin, stdout, stderr = conn.exec_command(cmd)
    pids = list(map(lambda x: x.strip(), stdout.read().decode().split()))
    logging.info(f'пиды процессов {pids}')
    for pid in pids:
        cmd = f"kill -9 {pid}"
        print(f'Executing command\n\t-{cmd}')
        stdin, stdout, stderr = conn.exec_command(cmd)
        logging.info(stdout.read())
        logging.info(stderr.read())
    #logging.info(f'Выполняю команду: \n-\t{cmd}')
    #stdin, stdout, stderr = conn.exec_command(cmd)
    #logging.info(stdout.read())
    #logging.info(stderr.read())
    time.sleep(2) 
    cmd = f"ps -aux | grep 'gpfdist -p {gpfdist_port}'\n"
    stdin, stdout, stderr = conn.exec_command(cmd)
    out = stdout.read()
    new_len = len(out.decode().split('\n'))
    time.sleep(2)
    if new_len < base_len:
        logging.info(f'gpfdist на порту {gpfdist_port} был остановлен')
    else:
        logging.info(f'gpfdist на порту {gpfdist_port} НЕ был остановлен')
    

