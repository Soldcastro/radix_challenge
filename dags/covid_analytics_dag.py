from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Airflow',
    'email': ['solano.d.castro@gmail.com'],
    'start_date': days_ago(1),
    'email_on_failure' : False
}

with DAG(
    dag_id = 'covid_analytics_load',
    default_args = default_args,
    catchup=False,
    max_active_runs = 1,
    schedule_interval = '@daily',
    tags=['covid19']
) as dag:
    
    def run_trusted():
        import os

        os.system('/usr/local/bin/python /opt/airflow/dags/trusted_process.py')

    def run_refined():
        import os

        os.system('/usr/local/bin/python /opt/airflow/dags/refined_process.py')

    trusted_call = PythonOperator(
        task_id = 'trusted_load',
        python_callable = run_trusted
    ) 

    refined_call = PythonOperator(
        task_id = 'refined_load',
        python_callable = run_refined
    )

trusted_call >> refined_call