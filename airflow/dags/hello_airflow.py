from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def say_hello():
    print("Hello from Airflow!")


def say_goodbye():
    print("Goodbye from Airflow!")


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 9, 6),
    schedule=None,
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    finish = PythonOperator(
        task_id="say_goodbye",
        python_callable=say_goodbye,
    )

    start >> finish