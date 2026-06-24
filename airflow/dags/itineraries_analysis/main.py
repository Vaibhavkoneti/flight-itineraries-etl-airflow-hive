from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from itineraries_analysis.tasks import create_connections
from itineraries_analysis.modules.bronze import process_bronze_layer
from itineraries_analysis.modules.silver import process_silver_layer
from itineraries_analysis.modules.gold import process_gold_layer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'itineraries_data_analysis_medallion',
    default_args=default_args,
    description='Medallion Architecture ETL for Itineraries Data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1
)

create_connections_task = PythonOperator(
    task_id='create_connections',
    python_callable=create_connections,
    dag=dag,
)

bronze_task = PythonOperator(
    task_id='bronze_layer',
    python_callable=process_bronze_layer,
    dag=dag,
)

silver_task = PythonOperator(
    task_id='silver_layer',
    python_callable=process_silver_layer,
    dag=dag,
)

gold_task = PythonOperator(
    task_id='gold_layer',
    python_callable=process_gold_layer,
    dag=dag,
)

create_connections_task >> bronze_task >> silver_task >> gold_task