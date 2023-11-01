from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from create_conn import create_essential_conn
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from etl_pipeline import run
import pendulum
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")


def get_most_recent_dag_run(dt):
    dag_runs = DagRun.find(dag_id="batch_etl_full_load")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag =  DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    max_active_runs=1,
	schedule_interval="*/10 * * * *",
    catchup=False)

wait_for_first_dag = ExternalTaskSensor(
    task_id="wait_for_first_dag",
    external_dag_id="batch_etl_full_load",
    external_task_id="end",
    mode="reschedule",
    allowed_states=["success"],
    execution_date_fn=get_most_recent_dag_run,
    timeout=3600,
    dag=dag
)

op0 = PythonOperator(
    task_id="create_connection",
    python_callable=create_essential_conn,
    dag=dag
)

op1 = PythonOperator(
    task_id= "etl_pipeline",
    python_callable=run,
    dag=dag
)

op2 = BashOperator(
    task_id="create_database_tables",
    bash_command="beeline -u {{params.url}} -f {{params.script}}",
    params={"url": "jdbc:hive2://hive-server2:10000",
            "script": "/opt/airflow/dags/hql/create_database_tables.hql"},
    dag=dag
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)  


wait_for_first_dag >> start  >> op0 >> op1 >> op2 >> end

if __name__ == "__main__":
    dag.cli()
