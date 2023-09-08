from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
	dag_id = 'dag_ayman_etl',
	schedule_interval = '@once',
    start_date=datetime(2023,8,28),
	catchup = False
) as dag:
	run_first = BashOperator(
	task_id = 'run_first',
	bash_command = 'python3 /root/airflow/dags/scripts/ayman/etl_script.py ' )

	run_first