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
    tags=["csv", "movies"],
)
def dag_declaration():
    import re
    import csv
    import ast
    import logging

    sql_column_map = {
        "genres": "genre", 
        "production_companies": "production_company", 
        "spoken_languages": "language",
    }

    @task()
    def parse_data_to_dicts() -> list[dict[str, str | float | None | dict[str, int | str] | list[dict[str, str | int]]]]:
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
        data_list: list[dict[str, str | float | None | dict[str, int | str] | list[dict[str, str | int]]]],
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
                        data = category["name"].lower().replace('"', "").replace(",", "").replace("\\", "")
                    else:
                        data = category["iso_639_1"].lower().replace('"', "").replace(",", "").replace("\\", "")

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

        with open(f"/opt/airflow/flat_files/{data_category}.csv", "w") as f:
            header = ["id", sql_column_map[data_category]]
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(csv_list)

    @task()
    def create_movies_csv_file(
        data_list: list[dict[str, str | float | None | dict[str, int | str] | list[dict[str, str | int]]]]
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
        
        with open(f"/opt/airflow/flat_files/movies.csv", "w") as f:
            header = ["id"]
            header.extend(movies_csv_columns)

            w = csv.writer(f)
            w.writerow(header)
            w.writerows(csv_list)

    def data_cleaning(data: dict[str, str | float | None | dict[str, int | str] | list[dict[str, str | int]]], category: str) -> bool:
        """Function to clean data for the expanded columns in the create_csv_files() call

            Args:
                data (dict): Individual data dict from derived from the movies_metadata.csv data
                category (str): The category name used in the data dict as a key

            Returns:
                bool: True/False return for conditional addition to csv_set for csv addition or exclusion
        """
        genre_exclude_list = ["filmworks", "entertainment", "production"]

        data = data.replace(',', '')
        data = re.sub(",", "", data)

        if category == "genres":
            for exclusion in genre_exclude_list:
                if exclusion in data:
                    return False
        elif category == "production_companies":
            data = data.replace('"', '')
            if ", the" in data:
                data = data.replace(', the', '')
                data = f"the {data}"
        elif category == "spoken_languages":
            if "???" in data or not data:
                return False

        return True

    def movies_data_cleaning(data: dict[str, str | float | None | dict[str, int | str] | list[dict[str, str | int]]], category: str) -> None:
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
            data[category] = data[category].lower().replace('"', "").replace(",", "").replace("\\", "")
        elif category == "release_date":
            if data[category] == "" or not data[category]:
                data[category] = '1901-01-01'
            elif len(data[category]) < 10:
                data[category] = '1901-01-01'

    @task()
    def create_n_m_csv_files(data_list: list[dict[str, str | float | None | dict[str, int | str] | list[dict[str, str | int]]]]) -> None:
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

        with open(
            "/opt/airflow/flat_files/genres.csv", "r", encoding="utf-8"
        ) as file:
            reader = csv.DictReader(file)

            for row in reader:
                genre_dict[row["genre"]] = row["id"]
       
        with open(
            "/opt/airflow/flat_files/movies.csv", "r", encoding="utf-8"
        ) as file:
            reader = csv.DictReader(file)

            for row in reader:
                movie_dict[row["title"]] = row["id"]
        
        with open(
            "/opt/airflow/flat_files/spoken_languages.csv", "r", encoding="utf-8"
        ) as file:
            reader = csv.DictReader(file)

            for row in reader:
                language_dict[row["language"]] = row["id"]

        for data in data_list:
            try:
                title = data["title"].lower().replace('"', "").replace(",", "").replace("\\", "")
            except Exception:
                title = "NULL"

            try:
                for genre in data["genres"]:
                    genre_name = genre["name"].lower().replace('"', "").replace(",", "").replace("\\", "")

                    movie_genre_nm_csv_list.append([movie_dict[title], genre_dict[genre_name]])
            except Exception as e:
                logging.warning(
                    f"Error encountered with this data point: {data}: {e}"
                )
            try:
                for language in data["spoken_languages"]:
                    language_name = language["iso_639_1"].lower().replace('"', "").replace(",", "").replace("\\", "")

                    movie_language_nm_csv_list.append([movie_dict[title], language_dict[language_name]])
            except Exception as e:
                logging.warning(f"Error with languages: {e}")

        with open(f"/opt/airflow/flat_files/movie_genre_nm.csv", "w") as f:
            header = ["movie_id", "genre_id"]
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(movie_genre_nm_csv_list)
        
        with open(f"/opt/airflow/flat_files/movie_language_nm.csv", "w") as f:
            header = ["movie_id", "language_id"]
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(movie_language_nm_csv_list)

    def main() -> None:
        parsed_list_data = parse_data_to_dicts()
        create_csv_files.partial(data_list=parsed_list_data).expand(
            data_category=["genres", "production_companies", "spoken_languages"]
        )
        create_movies_csv_file(parsed_list_data) >> create_n_m_csv_files(parsed_list_data)

    main()


dag_declaration()
