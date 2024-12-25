from airflow import DAG
from fw_dags import create_tpch_dag, create_single_object_dag_tpch, create_sap_odata_dag

tpch_dag: DAG = create_tpch_dag()
single_object_dag: DAG = create_single_object_dag_tpch()
odata_dag: DAG =  create_sap_odata_dag()


# if __name__ == "__main__":
#     from pendulum import datetime
#     single_object_dag.clear()
#     single_object_dag.run(start_date=datetime(2022, 1, 1, tz="Europe/Moscow"), run_at_least_once=True)
