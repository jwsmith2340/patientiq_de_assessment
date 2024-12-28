import csv

db_connection = {
    'dbname': 'data',
    'user': 'admin',
    'password': 'admin',
    'host': 'data-postgres',
    'port': '5432'
}


def write_csv_file(header: list[str], csv_list: list[list], file_name: str):
    with open(f"/opt/airflow/flat_files/{file_name}.csv", "w") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(csv_list)


def string_formatting(data_str: str) -> str:
    """Common string formatting functionality created as a util function

        Args:
            data_str (str): The string to be formatted
        
        Returns:
            str: The formatted string
    
    """
    return data_str.lower().replace('"', "").replace(",", "").replace("\\", "")