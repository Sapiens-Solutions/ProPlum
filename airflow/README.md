# Sapiens Solutions Greenplum ETL framework Airflow DAGs

## Usage 
1. Install Psycopg2 module on Airflow server
2. Create postgressql connection with name "adwh" in Airflow Connection Manager (Admin->Connections)
3. Add constants in Airflow Variables Manager (optional) (Admin->Variables)
	1) files_path - directory on etl-server from which files will be loaded with gpfdist
	2) gpfdist_port - gpfdist port on etl-server
	3) max_parallel_tasks - maximum number of parallel tasks
4. Replace ${target_schema} in .py files with your framework schema
5. Move .py files into dags directory on Airflow server

## License
Apache 2.0

## Contacts
info@sapiens.solutions