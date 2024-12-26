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
    dag_id="test",
    start_date=datetime(2024, 12, 21),
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["test"],
)
def dag_declaration():
    import csv
    import ast
    import json
    import logging

    # # HEADERS
    # list_of_stuff = [
    # 'adult': bool,
    # 'belongs_to_collection': dict,
    # 'budget': int,
    # 'genres': list[dict[str, str]],
    # 'homepage': str,
    # 'id': int,
    # 'imdb_id': str, # jumble of letters and numbers UID
    # 'original_language': str, # abbreviated country code
    # 'original_title',
    # 'overview',
    # 'popularity',
    # 'poster_path',
    # 'production_companies',
    # 'production_countries',
    # 'release_date',
    # 'revenue',
    # 'runtime',
    # 'spoken_languages',
    # 'status',
    # 'tagline',
    # 'title',
    # 'video',
    # 'vote_average',
    # 'vote_count',
    # ]

    # # MOVIES TABLE
    # """
    # I'll need revenue for 3.1 (3rd highest revenue)
    # Will need movies that didn't recoup budget (revenue < budget, top 3 ordered by imdb_id)
    # Will need the top 3 average revenue per genre from highest to lowest average
    # How many movies are in more than one language?
    # For each month (release date), which genre (genre) had the highest proportion of releases? 1 per calendar month, if a tie, list all the genres that tied
    # """

    # # GENRES TABLE
    # id: int = "primary_key auto increment"
    # genre_name: str = "varchar"

    # # PRODUCTION COMPANY TABLE
    # id: int = "primary_key auto increment"
    # production_companies: str = "varchar"
    # procution_countries: list[dict[str, str]] = Probably going to use a join table here? not sure, think about it, might not be necessary, will revisit"

    # # LANGUAGES TABLE
    # id: int = "primary_key auto_increment"
    # language: str = "varchar" # This should just be the language at the high level

    @task()
    def parse_data_to_dicts() -> list[
        dict[str, str | float | None | dict[str, int | str],],
        list[dict[str, str | int]],
    ]:
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
            dict[str, str | float | None | dict[str, int | str],],
            list[dict[str, str | int]],
        ],
        data_category: str,
    ) -> None:
        context = get_current_context()
        context["entity_index"] = data_category

        csv_set = set()
        csv_list = []

        for data in data_list:
            try:
                for category in data[data_category]:
                    data = category["name"].lower().replace('"', "")

                    if data_cleaning(data, data_category):
                        csv_set.add(data)
            except Exception as e:
                logging.warning(
                    f"Error encountered with this data point: {category}: {e}"
                )

        for val in csv_set:
            csv_list.append([val])

        with open(f"/opt/airflow/flat_files/{data_category}.csv", "w") as f:
            header = ["name"]
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(csv_list)

    @task()
    def create_movies_csv_file(
        data_list: list[
            dict[str, str | float | None | dict[str, int | str],],
            list[dict[str, str | int]],
        ]
    ) -> None:
        pass

    def data_cleaning(data: str, category: str) -> bool:
        genre_exclude_list = ["production", "entertainment", "production"]

        if category == "genres":
            for exclusion in genre_exclude_list:
                if exclusion in data:
                    return False
        elif category == "production_companies":
            data = data.replace('"', "")
            if ", the" in data:
                data = data.replace(", the", "")
                data = f"the {data}"

        return True

    def main() -> None:
        parsed_list_data = parse_data_to_dicts()
        create_csv_files.partial(data_list=parsed_list_data).expand(
            data_category=["genres", "production_companies", "spoken_languages"]
        )
        create_movies_csv_file(parsed_list_data)

    main()


dag_declaration()
