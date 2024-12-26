from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context

default_args = {
    "owner": "Data Engineering",
    "retries": 0,
    "retry_delay": timedelta(seconds=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=15),
}


@dag(
    dag_id="insert_csv_to_db",
    start_date=datetime(2024, 12, 21),
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["csv", "insert", "db"],
)
def dag_declaration():
    import csv
    import ast
    import logging
    import psycopg2

    from common.utils import db_connection, airflow_connection

    @task()
    def connection():
        try:
            connection = psycopg2.connect(**db_connection)
            cursor = connection.cursor()

            # Create tables if they don't exist
            cursor.execute(open("/opt/airflow/dags/sql_files/table_creation.sql", "r").read())
            connection.commit()

            with open("/opt/airflow/flat_files/genres.csv", 'r') as f:
                # Skip the headers
                next(f)
                cursor.copy_from(f, 'genres', sep=',', null='NULL', columns=('id', 'genre'))
            
            with open("/opt/airflow/flat_files/production_companies.csv", 'r') as f:
                # Skip the headers
                next(f)
                cursor.copy_from(f, 'production_companies', sep=',', null='NULL', columns=('id', 'prod_comp'))
            
            with open("/opt/airflow/flat_files/spoken_languages.csv", 'r') as f:
                # Skip the headers
                next(f)
                cursor.copy_from(f, 'languages', sep=',', null='NULL', columns=('id', 'lang'))
            
            # Commit the transaction
            connection.commit()
            print("Data inserted successfully.")
            
        except Exception as e:
            print(f"Error: {e}")
            connection.rollback()  # Rollback in case of an error
        finally:
            # Close the connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def main() -> None:
        connection()

    main()


dag_declaration()
