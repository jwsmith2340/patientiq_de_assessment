from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "Data Engineering",
    "retries": 3,
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

    @task()
    def test_task():
        print("This was only a test")
    
    def main():
        test_task()
    
    main()

dag_declaration()