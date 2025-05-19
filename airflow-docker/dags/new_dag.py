from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# OpenLineage imports
from openlineage.client.run import Dataset
from openlineage.client.facet import SchemaDatasetFacet, SchemaField

# Hàm để tạo dữ liệu người dùng mẫu
def generate_user_data(num_users, ti):
    # Tạo dữ liệu người dùng mẫu
    users = []
    for i in range(1, num_users + 1):
        user = {
            "Username": f"user{i}",
            "Email": f"user{i}@example.com",
            "FullName": f"User {i}",
            "Age": 20 + (i % 30)  # Tuổi từ 20-49
        }
        users.append(user)
    
    # Lưu dữ liệu vào XCom
    ti.xcom_push(key='user_data', value=users)
    
    # Thêm metadata OpenLineage
    inlets = []
    outlets = [
        Dataset(
            namespace="default",
            name="sample_users_data",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="Username", type="string"),
                        SchemaField(name="Email", type="string"),
                        SchemaField(name="FullName", type="string"),
                        SchemaField(name="Age", type="integer")
                    ]
                )
            }
        )
    ]
    
    return users

# Hàm để chèn dữ liệu người dùng vào PostgreSQL
def insert_user_data_into_postgres(ti):
    user_data = ti.xcom_pull(key='user_data', task_ids='generate_user_data')
    if not user_data:
        raise ValueError("Không tìm thấy dữ liệu người dùng")
    
    # Sử dụng connection ID mặc định
    postgres_hook = PostgresHook(conn_id='postgres_default')
    insert_query = """
    INSERT INTO users (username, email, fullname, age)
    VALUES (%s, %s, %s, %s)
    """
    for user in user_data:
        postgres_hook.run(insert_query, parameters=(
            user['Username'], 
            user['Email'], 
            user['FullName'], 
            user['Age']
        ))
    
    return len(user_data)

# Định nghĩa tham số mặc định
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Tạo DAG mới
dag = DAG(
    'user_data_processing',  # Tên DAG mới
    default_args=default_args,
    description='DAG để tạo và lưu trữ dữ liệu người dùng mẫu',
    schedule=timedelta(days=1),
)

# Task 1: Tạo dữ liệu người dùng
generate_user_data_task = PythonOperator(
    task_id='generate_user_data',
    python_callable=generate_user_data,
    op_args=[20],  # Tạo 20 người dùng
    dag=dag,
)

# Task 2: Tạo bảng users
create_users_table_task = PostgresOperator(
    task_id='create_users_table',
    conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL,
        fullname TEXT,
        age INTEGER
    );
    """,
    dag=dag,
)

# Task 3: Chèn dữ liệu người dùng
insert_user_data_task = PythonOperator(
    task_id='insert_user_data',
    python_callable=insert_user_data_into_postgres,
    dag=dag,
)

# Định nghĩa thứ tự thực thi
generate_user_data_task >> create_users_table_task >> insert_user_data_task
