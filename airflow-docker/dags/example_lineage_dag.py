from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from openlineage.airflow.extractors.base import TaskMetadata

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='lineage_demo',
    schedule='@daily',
    default_args=default_args,
    tags=['lineage']
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> end
