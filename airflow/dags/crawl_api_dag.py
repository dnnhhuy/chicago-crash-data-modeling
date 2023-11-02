from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from create_conn import create_essential_conn
from api_to_delta import run
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag =  DAG(
    dag_id='api_crawler',
    default_args=default_args,
    max_active_runs=1,
	schedule_interval="0 0 */1 * *",
    catchup=False)
    
op0 = PythonOperator(
    task_id="create_connection",
    python_callable=create_essential_conn,
    dag=dag
)

op1 = PythonOperator(
    task_id="crawl_data",
    python_callable=run,
    dag=dag
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)  
start  >> op0 >> op1 >> end

if __name__ == "__main__":
    dag.cli()