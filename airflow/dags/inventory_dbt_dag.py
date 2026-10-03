from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="inventory_dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # build_dim_customer = BashOperator(
    #     task_id="build_dim_customer",
    #     bash_command="docker exec dbt dbt run --select dim_customer",
    # )

    get_next_customer_key = BashOperator(
        task_id="get_next_customer_key",
        bash_command="""
        docker exec de_interview_project-postgres-1 psql -U de_user -d warehouse -t -A -c "SELECT 
        COALESCE(MAX(customer_key), 0) + 1 FROM analytics.dim_customer;"
        """,
        do_xcom_push=True,
    )

    create_new_customers_to_append = BashOperator(
        task_id="create_new_customers_to_append",
        bash_command="""
        docker exec de_interview_project-postgres-1 psql -U de_user -d warehouse -c "
        DROP TABLE IF EXISTS analytics.new_customers_to_append;

        CREATE TABLE analytics.new_customers_to_append AS
        SELECT
            {{ ti.xcom_pull(task_ids='get_next_customer_key') }} +
            ROW_NUMBER() OVER (ORDER BY customer_id) - 1 AS customer_key,
            customer_id
        FROM (
            SELECT DISTINCT o.customer_id
            FROM raw.orders o
            LEFT JOIN analytics.dim_customer d
                ON o.customer_id = d.customer_id
            WHERE d.customer_id IS NULL
        ) new_customers;
        "
        """,
    )

    append_new_customers = BashOperator(
        task_id="append_new_customers",
        bash_command="""
        docker exec de_interview_project-postgres-1 psql -U de_user -d warehouse -c "
        INSERT INTO analytics.dim_customer (customer_key, customer_id)
        SELECT customer_key, customer_id
        FROM analytics.new_customers_to_append;
        "
        """,
    )

    build_dim_date = BashOperator(
        task_id="build_dim_date",
        bash_command="docker exec dbt dbt run --select dim_date",
    )

    build_int_orders = BashOperator(
        task_id="build_int_orders",
        bash_command="docker exec dbt dbt run --select int_orders",
    )

    build_fact_orders = BashOperator(
        task_id="build_fact_orders",
        bash_command="docker exec dbt dbt run --select fact_orders",
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="docker exec dbt dbt test",
    )

  

    get_next_customer_key >> create_new_customers_to_append >> append_new_customers >> build_dim_date >> build_int_orders >> build_fact_orders >> run_dbt_tests