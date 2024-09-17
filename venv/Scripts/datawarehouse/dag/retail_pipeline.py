from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta
from extract import extract_data

with DAG(
        'retail_pipeline',
        description="Air flow pipe line for retail data set",
        start_date=datetime(year=2024, month=9, day=20),
        schedule_interval=timedelta(minutes=5)
) as dag:

    begin_retail_pipeline = EmptyOperator(
        task_id='begin_retail_pipeline',
    )

    create_table = MySqlOperator(
        task_id='create_retail_table',
        postgres_conn_id='mysql_conn',
        sql='sql/create-retail-table.sql'
    )


    etl = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_data
    )


    clean_retail_table = MySqlOperator(
        task_id='clean_retail_table',
        postgres_conn_id='mysql_conn',
        sql=["""TRUNCATE TABLE retails"""]
    )

    end_pipeline = EmptyOperator(
        task_id='end_retail_pipeline',
    )