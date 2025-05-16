# Airflow ETL Pipeline với OpenLineage (Marquez)

Dự án này là một triển khai Airflow cho pipeline ETL (Extract, Transform, Load) để thu thập dữ liệu sách, xử lý và lưu trữ vào cơ sở dữ liệu PostgreSQL. Dự án sử dụng Marquez (OpenLineage) để theo dõi và hiển thị quá trình xử lý dữ liệu.

## Cấu trúc dự án

```
airflow-docker/         # Thư mục chính chứa cấu hình Airflow và Docker
├── config/             # Cấu hình Airflow
├── dags/               # Các DAG files
│   ├── app.py          # DAG chính để thu thập và lưu trữ dữ liệu sách
│   └── example_lineage_dag.py  # DAG mẫu cho OpenLineage
├── plugins/            # Các plugin tùy chỉnh
├── docker-compose.yaml # Cấu hình Docker Compose
├── Dockerfile          # Định nghĩa Docker image
├── init.sql            # Script khởi tạo database
├── marquez-config.yml  # Cấu hình Marquez
└── requirements.txt    # Các dependency của dự án

tests/                  # Thư mục chứa các file test
├── __init__.py
├── test_dags.py        # Test các DAG
├── test_docker_config.py  # Test cấu hình Docker
├── test_docker_compose.py # Test cấu hình docker-compose.yaml
└── test_marquez_integration.py  # Test tích hợp Marquez
```

## Yêu cầu hệ thống

- Docker và Docker Compose
- Python 3.8+
- Git

## Cài đặt và chạy

### 1. Clone repository

```bash
git clone https://github.com/buihien9969/test_airflow.git
cd test_airflow
```

### 2. Chạy test trước khi khởi động Docker

```bash
# Cài đặt các dependency cho test
pip install -r requirements-test.txt

# Chạy tất cả các test
python run_tests.py

# Hoặc chạy test riêng cho docker-compose.yaml
python test_docker_compose_only.py
```

### 3. Khởi động Docker

```bash
cd airflow-docker
docker-compose up -d
```

### 4. Truy cập các UI

- **Airflow UI**: http://localhost:8080 (username/password: airflow/airflow)
- **Marquez UI**: http://localhost:3000
- **Marquez API**: http://localhost:5000/api/v1
- **PgAdmin**: http://localhost:5050 (email: admin@admin.com, password: root)

## Cấu hình Marquez

Marquez được cấu hình trong file `marquez-config.yml`:

```yaml
db:
  url: jdbc:postgresql://marquez-db:5432/marquez
  user: postgres
  password: postgres
  driverClass: org.postgresql.Driver
```

## DAGs

### fetch_and_store_amazon_books
DAG này thực hiện các nhiệm vụ:
1. Thu thập dữ liệu sách từ Amazon
2. Làm sạch và xử lý dữ liệu
3. Lưu trữ dữ liệu vào PostgreSQL

### lineage_demo
DAG mẫu để minh họa cách OpenLineage theo dõi luồng dữ liệu.

## Xem Lineage trong Marquez

1. Truy cập Airflow UI (http://localhost:8080) và kích hoạt DAG `lineage_demo`
2. Đợi DAG chạy xong
3. Truy cập Marquez UI (http://localhost:3000) để xem thông tin lineage

## Các vấn đề thường gặp và cách khắc phục

### 1. Marquez UI không hoạt động

Kiểm tra log của container Marquez Web:

```bash
docker logs airflow-docker-marquez-web-1
```

Đảm bảo biến môi trường `WEB_PORT` đã được thiết lập:

```yaml
environment:
  MARQUEZ_HOST: marquez
  MARQUEZ_PORT: 8080
  WEB_PORT: 3000
```

### 2. Marquez API không hoạt động

Kiểm tra log của container Marquez:

```bash
docker logs airflow-docker-marquez-1
```

Đảm bảo cấu hình kết nối database trong `marquez-config.yml` là chính xác:

```yaml
db:
  url: jdbc:postgresql://marquez-db:5432/marquez
  user: postgres
  password: postgres
  driverClass: org.postgresql.Driver
```

## Công nghệ sử dụng

- Apache Airflow 3.0.1
- PostgreSQL 13
- Marquez (OpenLineage)
- Docker & Docker Compose
- Python 3.12
