from datetime import datetime
from airflow.decorators import task, dag

default_args = {
    "owner": "Data Engineering",
    "retries": 0,
}


@dag(
    dag_id="insert_csv_to_db",
    start_date=datetime(2024, 12, 21),
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["csv", "insert", "db"],
)
def insert_csv_into_db_dag():
    """DAG to create SQL tables if they do not exist and insert csv files into the DB"""
    import psycopg2
    import logging

    from common.utils import db_connection

    @task()
    def sql_table_creation():
        """Create SQL tables from the table_creation.sql file

            Args:
                None

            Returns:
                None
        """
        try:
            connection = psycopg2.connect(**db_connection)
            cursor = connection.cursor()

            cursor.execute(
                open("/opt/airflow/dags/sql_files/table_creation.sql", "r").read()
            )
            connection.commit()

        except Exception as e:
            logging.error(f"Error: {e}")
            connection.rollback()
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    @task()
    def upload_csv_files():
        """Task to upload all created CSV files to the SQL DB
        
            Args:
                None

            Returns:
                None
        """
        try:
            connection = psycopg2.connect(**db_connection)
            cursor = connection.cursor()

            csv_val_list = [
                ["genres", "genres", ("id", "genre")],
                ["production_companies", "production_companies", ("id", "prod_comp")],
                ["spoken_languages", "languages", ("id", "lang")],
                ["movies", "movies", ("id", "budget", "imdb_id", "revenue", "release_date", "title",)],
                ["movie_genre_nm", "movie_genre_nm", ("movie_id", "genre_id")],
                ["movie_language_nm", "movie_language_nm", ("movie_id", "language_id")]
            ]

            for csv_list in csv_val_list:
                csv_copy_from(cursor=cursor, file_name=csv_list[0], table_name=csv_list[1], columns=csv_list[2])
            
            # Commit the transaction
            connection.commit()
            logging.info("Data inserted successfully.")

        except Exception as e:
            logging.error(f"Error: {e}")
            connection.rollback()
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def csv_copy_from(cursor, file_name: str, table_name: str, columns: tuple[str]) -> None:
        """Reusable fn to handle copy_from programatically to reduce code
        
            Args:
                cursor: psycopg2 cursor object
                file_name (str): The csv file name to be read
                table_name (str): The SQL table name to upload to
                columns (tuple[str]): A tuple of SQL table column names

            Returns:
                None
        """
        with open(f"/opt/airflow/flat_files/{file_name}.csv", "r") as f:
            next(f)
            cursor.copy_from(
                f, table_name, sep=",", null="NULL", columns=columns
            )

    def main() -> None:
        """Primary entry point for the DAG where the task flow is declared

            Args:
                None

            Returns:
                None
        """
        sql_table_creation() >> upload_csv_files()

    main()


insert_csv_into_db_dag()
