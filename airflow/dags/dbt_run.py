from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="dbt_run",
    start_date=datetime(2026, 9, 6),
    schedule=None,
    catchup=False,
) as dag:

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="""
        docker run --rm \
          --network de_interview_project_default \
          -v "C:/Users/karma/OneDrive/Documents/de_interview_project/dbt:/usr/app" \
          -v "C:/Users/karma/OneDrive/Documents/de_interview_project/dbt/.dbt:/root/.dbt" \
          -w /usr/app/de_warehouse \
          ghcr.io/dbt-labs/dbt-postgres:1.9.0 \
          run
        """,
    )