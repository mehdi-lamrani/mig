from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG('spark_submit_dag',
         default_args=default_args,
         schedule_interval=None,  # Manually triggered
         catchup=False) as dag:

    # Define the spark-submit task
    submit_spark_job_1 = BashOperator(
        task_id='submit_spark_job_1',
        bash_command='spark-submit /devp/job/spark_job_1.py'
    )


airflow scheduler & airflow webserver --port 8080
