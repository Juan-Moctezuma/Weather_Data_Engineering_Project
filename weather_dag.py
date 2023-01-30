from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta

# Import ETL & Delete functions - each used for one DAG
# Prerequisites : Docker and Apache Airflow need to be installed 
from weather_etl import run_weather_etl
from data_delete import run_delete

# Write default arguments for DAG
# The code runs from 1/24 - 1/25
dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 24),
    'end_date': datetime(2023, 1, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# DAG 1 is for running the pipeline (storing into database)
dag_1 = DAG(
    'loading_dag',
    default_args=dag_args,
    description='Uses weather_etl.py to load data into postgresql (weather_db)',
    schedule_interval='0 * * * *' # Once an hour at the beginning of the hour
)

# DAG 2 is for running delete function
dag_2 = DAG(
    'delete_dag',
    default_args=dag_args,
    description='Uses data_delete.py to delete old data approx. an hour after it got loaded into weather_db',
    schedule_interval='58 * * * *' # Once an hour at the 'end' of the hour
)

# Need an operator for calling the etl / Python code via Airflow
run_etl = PythonOperator(
    task_id='node 1 - run ETL',
    python_callable=run_weather_etl,
    dag=dag_1,
)

# Need an operator for calling the delete / Python code via Airflow
run_del = PythonOperator(
    task_id='node 2 - delete old data',
    python_callable=run_delete,
    dag=dag_2,
)

# The pipeline will get ran & then data gets deleted
# Note that a copy of the loaded data gets stored in CSV
run_etl
run_del
