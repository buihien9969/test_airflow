from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from openlineage.airflow.extractors.base import TaskMetadata

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='lineage_demo',
    schedule_interval='@daily',
    default_args=default_args,
    tags=['lineage']
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> end
