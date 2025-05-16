
#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#1) fetch amazon data (extract) 2) clean data (transform)

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}


def get_amazon_data_books(num_books, ti):
    # Generate mock data instead of scraping Amazon
    books = [
        {
            "Title": "Python Crash Course, 3rd Edition",
            "Author": "Eric Matthes",
            "Price": "29.99",
            "Rating": "4.7 out of 5 stars"
        },
        {
            "Title": "Automate the Boring Stuff with Python, 2nd Edition",
            "Author": "Al Sweigart",
            "Price": "24.95",
            "Rating": "4.6 out of 5 stars"
        },
        {
            "Title": "Fluent Python: Clear, Concise, and Effective Programming",
            "Author": "Luciano Ramalho",
            "Price": "39.99",
            "Rating": "4.8 out of 5 stars"
        },
        {
            "Title": "Python for Data Analysis",
            "Author": "Wes McKinney",
            "Price": "34.99",
            "Rating": "4.5 out of 5 stars"
        },
        {
            "Title": "Learning Python, 5th Edition",
            "Author": "Mark Lutz",
            "Price": "44.99",
            "Rating": "4.4 out of 5 stars"
        }
    ]

    # Duplicate the books to reach the requested number
    while len(books) < num_books:
        for book in books[:]:
            if len(books) < num_books:
                new_book = book.copy()
                new_book["Title"] = f"{book['Title']} (Copy {len(books)})"
                books.append(new_book)
            else:
                break

    # Limit to the requested number of books
    books = books[:num_books]

    # Push the data to XCom
    ti.xcom_push(key='book_data', value=books)

    return books

#3) create and store data in table on postgres (load)

def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")

    # Use the default connection ID that Airflow creates automatically
    postgres_hook = PostgresHook(conn_id='postgres_default')
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task