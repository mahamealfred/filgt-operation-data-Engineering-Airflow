import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


AIRFLOW_HOME = Path("/opt/airflow") 
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0,  str(AIRFLOW_HOME))
from scripts.bronze_ingest import run_bronze_ingestion
from scripts.silver_transform import run_silver_transformation
from scripts.gold_aggregate import run_gold_aggregation
from scripts.load_gold_to_snowflake import load_gold_to_snowflake

default_args = {
    'owner': 'airflow',
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='flight_pipeline',
    default_args=default_args,
    description='A simple flight data pipeline',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 2, 25),
    catchup=False,
) as dag:

    bronze_ingestion = PythonOperator(
        task_id='bronze_ingestion',
        python_callable=run_bronze_ingestion,
        provide_context=True
    )

    silver_transformation = PythonOperator(
        task_id='silver_transformation',
        python_callable=run_silver_transformation,
        provide_context=True
    )

    gold_aggregation = PythonOperator(
        task_id='gold_aggregation', 
        python_callable=run_gold_aggregation, 
        provide_context=True
    )

    load_to_snowflake = PythonOperator(
        task_id='load_to_snowflake',    
        python_callable=load_gold_to_snowflake,
        provide_context=True
    )

    bronze_ingestion >> silver_transformation  >> gold_aggregation >> load_to_snowflake 