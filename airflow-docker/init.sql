
CREATE EXTENSION IF NOT EXISTS dblink;
-- Kiểm tra và tạo database "airflow" nếu chưa tồn tại
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') THEN
      PERFORM dblink_connect('dbname=postgres');
      PERFORM dblink_exec('CREATE DATABASE airflow');
      PERFORM dblink_disconnect();
   END IF;

   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'marquez') THEN
      PERFORM dblink_connect('dbname=postgres');
      PERFORM dblink_exec('CREATE DATABASE marquez');
      PERFORM dblink_disconnect();
   END IF;
END
$$;
