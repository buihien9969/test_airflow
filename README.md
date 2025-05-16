# Airflow ETL Pipeline với OpenLineage

Dự án này là một triển khai Airflow cho pipeline ETL (Extract, Transform, Load) để thu thập dữ liệu sách, xử lý và lưu trữ vào cơ sở dữ liệu PostgreSQL. Dự án sử dụng OpenLineage để theo dõi và hiển thị quá trình xử lý dữ liệu.

## Cấu trúc dự án

```
airflow-docker/
├── config/           # Cấu hình Airflow
├── dags/             # Các DAG files
│   ├── app.py        # DAG chính để thu thập và lưu trữ dữ liệu sách
│   └── example_lineage_dag.py  # DAG mẫu cho OpenLineage
├── plugins/          # Các plugin tùy chỉnh
├── .env              # Biến môi trường
├── docker-compose.yaml  # Cấu hình Docker Compose
├── Dockerfile        # Định nghĩa Docker image
├── init.sql          # Script khởi tạo database
└── requirements.txt  # Các dependency của dự án
```

## Cài đặt và chạy

1. Đảm bảo bạn đã cài đặt Docker và Docker Compose

2. Clone repository này:
   ```
   git clone <repository-url>
   cd airflow
   ```

3. Khởi động các container:
   ```
   cd airflow-docker
   docker-compose up -d
   ```

4. Truy cập Airflow UI tại http://localhost:8080 (username/password: airflow/airflow)

5. Truy cập Marquez UI (OpenLineage) tại http://localhost:3000

## DAGs

### fetch_and_store_amazon_books
DAG này thực hiện các nhiệm vụ:
1. Thu thập dữ liệu sách từ Amazon
2. Làm sạch và xử lý dữ liệu
3. Lưu trữ dữ liệu vào PostgreSQL

### lineage_demo
DAG mẫu để minh họa cách OpenLineage theo dõi luồng dữ liệu.

## Công nghệ sử dụng

- Apache Airflow 3.0.1
- PostgreSQL
- Marquez (OpenLineage)
- Docker & Docker Compose
- Python 3.12
