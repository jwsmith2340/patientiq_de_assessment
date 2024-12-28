from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "Data Engineering",
    "retries": 0,
}


@dag(
    dag_id="movies_csv_creator",
    start_date=datetime(2024, 12, 21),
    default_args=default_args,
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["csv", "movies"],
)
def movie_csv_creation_dag():
    """Creates movie related CSV files for upload to a SQL DB"""
    import re
    import csv
    import ast
    import logging

    from common.utils import write_csv_file, string_formatting

    sql_column_map = {
        "genres": "genre",
        "production_companies": "production_company",
        "spoken_languages": "language",
    }

    @task()
    def parse_data_to_dicts() -> list[
        dict[
            str, str | float | None | dict[str, int | str] | list[dict[str, str | int]]
        ]
    ]:
        """Parses data from movies_metadata.csv to dict form with data type conversions

            Args:
                None

            Returns:
                list: List of dicts and type converted fields of movie metadata
        """
        rows = []

        with open(
            "/opt/airflow/flat_files/movies_metadata.csv", "r", encoding="utf-8"
        ) as file:
            reader = csv.DictReader(file)

            for row in reader:
                for col in [
                    "budget",
                    "revenue",
                    "runtime",
                    "popularity",
                    "vote_average",
                    "vote_count",
                    "id",
                ]:
                    try:
                        row[col] = float(row[col]) if row[col] else None
                    except Exception as e:
                        logging.warning(
                            f"Issue with column {col}: {row[col]}. Normalizing to None"
                        )
                        logging.warning(f"Error message returned {e}")
                        row[col] = None

                for col in [
                    "belongs_to_collection",
                    "genres",
                    "production_companies",
                    "production_countries",
                    "spoken_languages",
                ]:
                    row[col] = ast.literal_eval(row[col]) if row[col] else []

                rows.append(row)

        return rows

    @task(map_index_template="{{ entity_index }}")
    def create_csv_files(
        data_list: list[
            dict[
                str,
                str | float | None | dict[str, int | str] | list[dict[str, str | int]],
            ]
        ],
        data_category: str,
    ) -> None:
        """Dynamically generated task that creates csv files

            Args:
                data_list (list): List of dicts with all movie metadata in type converted fields
                data_category (str): Data category derived from list expansion in main() function

            Returns:
                None
        """
        context = get_current_context()
        context["entity_index"] = data_category

        csv_set = set()
        csv_list = []

        for data in data_list:
            try:
                for category in data[data_category]:
                    if data_category is not "spoken_languages":
                        data = string_formatting(category["name"])
                    else:
                        data = string_formatting(category["iso_639_1"])

                    if data_cleaning(data, data_category):
                        csv_set.add(data)
            except Exception as e:
                logging.warning(
                    f"Error encountered with this data point: {category}: {e}"
                )

        id_count = 1
        for val in csv_set:
            csv_list.append([id_count, val])
            id_count += 1

        write_csv_file(
            header=["id", sql_column_map[data_category]],
            csv_list=csv_list,
            file_name=data_category,
        )

    @task()
    def create_movies_csv_file(
        data_list: list[
            dict[
                str,
                str | float | None | dict[str, int | str] | list[dict[str, str | int]],
            ]
        ]
    ) -> None:
        """Task to create the movies.csv file, requiring additional logic over the generic create_csv_files task

            Args:
                data_list (list): List of dicts with all movie metadata in type converted fields

            Returns:
                None
        """

        movies_csv_columns = [
            "budget",
            "imdb_id",
            "revenue",
            "release_date",
            "title",
        ]
        count = 0
        csv_list = []

        for data in data_list:
            count += 1
            csv_data_list = [count]

            try:
                for category in movies_csv_columns:
                    movies_data_cleaning(data, category)
                    csv_data_list.append(data[category])

            except Exception as e:
                logging.warning(
                    f"Error encountered with this data point: {data[category]}: {e}"
                )

            csv_list.append(csv_data_list)

        movies_csv_header = ["id"]
        movies_csv_header.extend(movies_csv_columns)
        write_csv_file(header=movies_csv_header, csv_list=csv_list, file_name="movies")

    def data_cleaning(
        data: dict[
            str, str | float | None | dict[str, int | str] | list[dict[str, str | int]]
        ],
        category: str,
    ) -> bool:
        """Function to clean data for the expanded columns in the create_csv_files() call

            Args:
                data (dict): Individual data dict from derived from the movies_metadata.csv data
                category (str): The category name used in the data dict as a key

            Returns:
                bool: True/False return for conditional addition to csv_set for csv addition or exclusion
        """
        genre_exclude_list = ["filmworks", "entertainment", "production"]

        data = data.replace(",", "")
        data = re.sub(",", "", data)

        if category == "genres":
            for exclusion in genre_exclude_list:
                if exclusion in data:
                    return False
        elif category == "production_companies":
            data = data.replace('"', "")
            if ", the" in data:
                data = data.replace(", the", "")
                data = f"the {data}"
        elif category == "spoken_languages":
            if "???" in data or not data:
                return False

        return True

    def movies_data_cleaning(
        data: dict[
            str, str | float | None | dict[str, int | str] | list[dict[str, str | int]]
        ],
        category: str,
    ) -> None:
        """Function to clean data for the create_movies_csv_file task

            Args:
                data (dict): Individual data dict from derived from the movies_metadata.csv data
                category (str): The category name used in the data dict as a key

            Returns:
                None
        """
        if category in ["revenue", "budget"]:
            if not data[category]:
                data[category] = 0
            data[category] = int(data[category])
        elif category == "title":
            if not data[category]:
                data[category] = "null"
            data[category] = string_formatting(data[category])
        elif category == "release_date":
            if data[category] == "" or not data[category]:
                data[category] = "1901-01-01"
            elif len(data[category]) < 10:
                data[category] = "1901-01-01"

    @task()
    def create_nm_csv_files(
        data_list: list[
            dict[
                str,
                str | float | None | dict[str, int | str] | list[dict[str, str | int]],
            ]
        ]
    ) -> None:
        """Dynamically generated task to create nm relationship csv files for SQL DB loading for m:m tables

            Args:
                data_list (list): List of dicts with all movie metadata in type converted fields

            Returns:
                None
        """
        genre_dict = {}
        movie_dict = {}
        language_dict = {}
        movie_genre_nm_csv_list = []
        movie_language_nm_csv_list = []

        csv_reader_list = [
            ["genres", "genre", genre_dict],
            ["movies", "title", movie_dict],
            ["spoken_languages", "language", language_dict],
        ]

        for cat_list in csv_reader_list:
            read_created_csv_files(
                csv_name=cat_list[0], category=cat_list[1], cat_dict=cat_list[2]
            )

        for data in data_list:
            try:
                title = string_formatting(data["title"])
            except Exception:
                title = "NULL"

            try:
                for genre in data["genres"]:
                    genre_name = string_formatting(genre["name"])

                    movie_genre_nm_csv_list.append(
                        [movie_dict[title], genre_dict[genre_name]]
                    )
            except Exception as e:
                logging.warning(f"Error encountered with this data point: {data}: {e}")
            try:
                for language in data["spoken_languages"]:
                    language_name = string_formatting(language["iso_639_1"])

                    movie_language_nm_csv_list.append(
                        [movie_dict[title], language_dict[language_name]]
                    )
            except Exception as e:
                logging.warning(f"Error with languages: {e}")

        write_csv_file(
            header=["movie_id", "genre_id"],
            csv_list=movie_genre_nm_csv_list,
            file_name="movie_genre_nm",
        )
        write_csv_file(
            header=["movie_id", "language_id"],
            csv_list=movie_language_nm_csv_list,
            file_name="movie_language_nm",
        )

    def read_created_csv_files(csv_name: str, category: str, cat_dict: dict) -> None:
        """Reusable csv reader function for code readability improvement to update a dict

            Args:
                csv_name (str): String csv_name for file name declaration
                category (str): Column category name for dict access
                cat_dict (dict): Dict to hold csv results

            Returns:
                None
        """
        with open(
            f"/opt/airflow/flat_files/{csv_name}.csv", "r", encoding="utf-8"
        ) as file:
            reader = csv.DictReader(file)

            for row in reader:
                cat_dict[row[category]] = row["id"]

    def main() -> None:
        """Primary entry point for the DAG where the task flow is declared

            Args:
                None

            Returns:
                None
        """
        parsed_list_data = parse_data_to_dicts()
        create_csv_files.partial(data_list=parsed_list_data).expand(
            data_category=["genres", "production_companies", "spoken_languages"]
        )
        create_movies_csv_file(parsed_list_data) >> create_nm_csv_files(
            parsed_list_data
        )

    main()


movie_csv_creation_dag()
