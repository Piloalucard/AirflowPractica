import airflow
import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operator.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'end_date': datetime(2025,12,12),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG (
    'Datapath',
    default_args=default_args,
    description='Una version del datapath',
    schedule_interval=timedelta(days=1),
)

def pc():
    return 'This is the PC counter'
pc_counter = PythonOperator(task_id='pc_task', python_callable=pc, dag=dag)

def add():
    return 'ADDING'
adder = PythonOperator(task_id='add_task', python_callable=pc, dag=dag)

def mul():
    return 'MULTIPLYING'
multiplier = PythonOperator(task_id='mul_task', python_callable=pc, dag=dag)

def printf():
    return 'RESULT = 0'
prt = PythonOperator(task_id='print_task', python_callable=pc, dag=dag)

pc_counter >> [adder,multiplier] >> prt >> pc_counter