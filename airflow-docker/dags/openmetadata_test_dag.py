from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from datetime import datetime, timedelta
import logging

# Hàm để tạo dữ liệu mẫu
def generate_test_data(**kwargs):
    logging.info("Tạo dữ liệu mẫu cho OpenMetadata")
    data = [
        {"id": 1, "name": "Test 1", "value": 100},
        {"id": 2, "name": "Test 2", "value": 200},
        {"id": 3, "name": "Test 3", "value": 300},
    ]
    return data

# Hàm để xử lý dữ liệu
def process_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='generate_data')
    logging.info(f"Xử lý {len(data)} bản ghi")

    # Xử lý dữ liệu
    processed_data = []
    for item in data:
        processed_data.append({
            "id": item["id"],
            "name": item["name"].upper(),
            "value": item["value"] * 2
        })

    ti.xcom_push(key='processed_data', value=processed_data)
    return processed_data

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'openmetadata_test',
    default_args=default_args,
    description='DAG để kiểm tra kết nối với OpenMetadata',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['openmetadata', 'test'],
) as dag:

    # Tạo bảng
    create_table = PostgresOperator(
        task_id='create_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS openmetadata_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            value INTEGER
        );
        """
    )

    # Tạo dữ liệu
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_test_data,
    )

    # Xử lý dữ liệu
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    # Lưu dữ liệu vào database
    save_data = PostgresOperator(
        task_id='save_data',
        conn_id='postgres_default',
        sql="""
        INSERT INTO openmetadata_test (id, name, value)
        VALUES
        ({{ ti.xcom_pull(task_ids='process_data')[0]['id'] }},
         '{{ ti.xcom_pull(task_ids='process_data')[0]['name'] }}',
         {{ ti.xcom_pull(task_ids='process_data')[0]['value'] }}),
        ({{ ti.xcom_pull(task_ids='process_data')[1]['id'] }},
         '{{ ti.xcom_pull(task_ids='process_data')[1]['name'] }}',
         {{ ti.xcom_pull(task_ids='process_data')[1]['value'] }}),
        ({{ ti.xcom_pull(task_ids='process_data')[2]['id'] }},
         '{{ ti.xcom_pull(task_ids='process_data')[2]['name'] }}',
         {{ ti.xcom_pull(task_ids='process_data')[2]['value'] }})
        ON CONFLICT (id) DO UPDATE
        SET name = EXCLUDED.name, value = EXCLUDED.value;
        """
    )

    # Định nghĩa thứ tự thực hiện
    create_table >> generate_data >> process_data_task >> save_data
