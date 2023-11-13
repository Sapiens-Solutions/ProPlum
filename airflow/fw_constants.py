from airflow.models import Variable

adwh_conn_id = "greenplum"
#max_parallel_tasks = 7
max_parallel_tasks = int(Variable.get("max_parallel_tasks"))
