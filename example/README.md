# Example loading

## About example
Three methods of loading - pxf, function, gpfdist. Execute one function to prepare your database

## Installation 
1. Install function f_install_proplum_demo_example from file [install.sql](install.sql)
2. Execute function in your greenplum database with installed proplum framework:
```
sql
select f_proplum_demo_example(
          p_demo_schema := 'proplum_demo'::text, --demo schema (will be created if not exists)
          p_owner := 'etl'::text, --owner of the demo tables and demo schema
          p_fw_schema := 'etl'::text, --framework schema
          p_demo := '{pxf,function,gpfdist}'::_text, --set of demo examples
          p_gp_conn := '{"host":"greenplum_host","port":"greenplum_port","db":"greenplum_db","user":"greenplum_user","pass":"greenplum_user_password"}'::jsonb, --optional params for pxf example
          p_gpfdist_params:= '{"host":"gpfdist_host","port":"8081"}'::gpfdist_port); --optional params for gpfdist example
```
3. Add file [fw_demo.py](fw_demo.py) into your Airflow DAGs folder
4. `OPTIONAL` If you want to run gpfdist example, add file [gpfdist_data.csv](gpfdist_data.csv) into your gpfdist server
5. Run DAG `job_proplum_demo_dag_no_dep` (without dependencies) or `job_proplum_demo_dag` (with dependencies, only for all pxf, function and gpfdist examples)
6. `OPTIONAL` If you want to uninstall example artifacts from your system, install and execute function f_uninstall_proplum_demo_example from file [uninstall.sql](uninstall.sql): 
```
sql
select f_uninstall_proplum_demo_example(
          p_demo_schema:= 'proplum_demo',--demo schema from 2
		  p_fw_schema  := 'etl' --framework schema from 2
		  );
```
Also delete fw_demo.py file from your Airflow DAGs folder

## License
Apache 2.0

## Contacts
info@sapiens.solutions