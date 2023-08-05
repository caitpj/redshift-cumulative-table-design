TODO
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

cumulated_sql_list = [
    "active_users_cumulated_populate_1.sql",
    "active_users_cumulated_populate_2.sql",
    "active_users_cumulated_populate_3.sql",
    "active_users_cumulated_populate_4.sql",
]

default_args = {
    'owner': 'Cai Parry-Jones',
    # Very important to have your cumulative DAGs be set `depends_on_past = True`
    'depends_on_past': True,
}
with DAG(
    'cumulative_table_example_dag',
    default_args=default_args,
    description='A cumulative table example',
    schedule_interval='@daily',
    start_date='2023-01-31',
    catchup=False,
    tags=['example'],
) as dag:
    daily_task = RedshiftDataOperator(
        aws_conn_id="connection id here",
        region="region name here",
        database="database name here",
        task_id='compute_active_user_daily',
        sql='queries/active_users_daily_populate.sql'
        wait_for_completion=True,
    )

    cumulative_task = RedshiftDataOperator(
        aws_conn_id="connection id here",
        region="region name here",
        database="database name here",
        task_id="compute_active_users_cumulated",
        sql=[
            # Using jinja to run each of the SQL scripts one after anopther in the cumulated_sql_list
            f"{{% include 'queries/{sql}' %}}" for sql in cumulated_sql_list
        ],
        wait_for_completion=True,
    )

    # the daily task has to run before the cumulative task
    daily_task >> cumulative_task
