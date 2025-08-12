from airflow.models import Variable

adwh_conn_id = "greenplum"
max_parallel_tasks = 3
#max_parallel_tasks = int(Variable.get("max_parallel_tasks"))
ssh_conn_id = 'test_ssh_con'
odata_conn_id = 'test_http_con'
fw_schema = 'fw'