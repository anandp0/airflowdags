from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_date():
    print(f"Current date and time: {datetime.now()}")

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'print_date_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Run once a day
    catchup=False,
) as dag:

    # Task to print date using BashOperator
    print_date_bash = BashOperator(
        task_id='print_date_bash',
        bash_command='date'
    )

    # Task to print date using PythonOperator
    print_date_python = PythonOperator(
        task_id='print_date_python',
        python_callable=print_date
    )

    # Define task dependencies
    print_date_bash >> print_date_python
