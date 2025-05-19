FROM apache/airflow:3.0.1

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install 'openlineage-airflow[db]'
RUN pip install 'openmetadata-ingestion[airflow]'
