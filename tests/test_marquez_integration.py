import unittest
import requests
import os
import time
from datetime import datetime, timedelta

class TestMarquezIntegration(unittest.TestCase):
    """
    Kiểm tra tích hợp giữa Airflow và Marquez
    """

    def setUp(self):
        """
        Thiết lập các thông số cần thiết cho việc kiểm tra
        """
        self.marquez_api_url = "http://localhost:5000/api/v1"
        self.marquez_ui_url = "http://localhost:3000"
        self.airflow_api_url = "http://localhost:8080/api/v2"
        self.namespace = "default"

        # Thông tin đăng nhập Airflow
        self.airflow_username = "airflow"
        self.airflow_password = "airflow"

        # Kiểm tra xem Docker đã chạy chưa
        self.docker_running = self._check_docker_running()

    def _check_docker_running(self):
        """
        Kiểm tra xem Docker đã chạy chưa
        """
        try:
            # Thử kết nối đến Marquez API
            requests.get(self.marquez_api_url, timeout=1)
            return True
        except (requests.RequestException, requests.Timeout):
            return False

    def test_marquez_service_running(self):
        """
        Kiểm tra xem Marquez API có đang chạy không
        """
        if not self.docker_running:
            self.skipTest("Docker không chạy, bỏ qua test này")

        try:
            response = requests.get(f"{self.marquez_api_url}/namespaces")
            self.assertEqual(response.status_code, 200, "Marquez API không hoạt động")

            # Kiểm tra xem namespace mặc định có tồn tại không
            namespaces = response.json()
            namespace_names = [ns.get('name') for ns in namespaces]
            self.assertIn(self.namespace, namespace_names,
                         f"Namespace '{self.namespace}' không tồn tại trong Marquez")

            print(f"✅ Marquez API đang chạy và namespace '{self.namespace}' tồn tại")
        except requests.RequestException as e:
            self.fail(f"Không thể kết nối đến Marquez API: {str(e)}")

    def test_marquez_ui_running(self):
        """
        Kiểm tra xem Marquez UI có đang chạy không
        """
        if not self.docker_running:
            self.skipTest("Docker không chạy, bỏ qua test này")

        try:
            response = requests.get(self.marquez_ui_url)
            self.assertEqual(response.status_code, 200, "Marquez UI không hoạt động")
            print("✅ Marquez UI đang chạy")
        except requests.RequestException as e:
            self.fail(f"Không thể kết nối đến Marquez UI: {str(e)}")

    def test_airflow_api_running(self):
        """
        Kiểm tra xem Airflow API có đang chạy không
        """
        if not self.docker_running:
            self.skipTest("Docker không chạy, bỏ qua test này")

        try:
            # Lấy token xác thực từ Airflow API
            auth_response = requests.post(
                f"{self.airflow_api_url}/auth/login",
                json={"username": self.airflow_username, "password": self.airflow_password}
            )

            self.assertEqual(auth_response.status_code, 200, "Không thể xác thực với Airflow API")
            token = auth_response.json().get('token')

            # Kiểm tra API với token
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(f"{self.airflow_api_url}/dags", headers=headers)

            self.assertEqual(response.status_code, 200, "Airflow API không hoạt động")
            print("✅ Airflow API đang chạy")
        except requests.RequestException as e:
            self.fail(f"Không thể kết nối đến Airflow API: {str(e)}")

    def test_lineage_dag_exists(self):
        """
        Kiểm tra xem DAG lineage_demo có tồn tại trong Airflow không
        """
        if not self.docker_running:
            self.skipTest("Docker không chạy, bỏ qua test này")

        try:
            # Lấy token xác thực từ Airflow API
            auth_response = requests.post(
                f"{self.airflow_api_url}/auth/login",
                json={"username": self.airflow_username, "password": self.airflow_password}
            )

            self.assertEqual(auth_response.status_code, 200, "Không thể xác thực với Airflow API")
            token = auth_response.json().get('token')

            # Kiểm tra API với token
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(f"{self.airflow_api_url}/dags", headers=headers)

            self.assertEqual(response.status_code, 200, "Không thể lấy danh sách DAG từ Airflow API")

            dags = response.json().get('dags', [])
            dag_ids = [dag.get('dag_id') for dag in dags]

            self.assertIn('lineage_demo', dag_ids, "DAG 'lineage_demo' không tồn tại trong Airflow")
            print("✅ DAG 'lineage_demo' tồn tại trong Airflow")
        except requests.RequestException as e:
            self.fail(f"Không thể kết nối đến Airflow API: {str(e)}")

    def test_lineage_data_in_marquez(self):
        """
        Kiểm tra xem dữ liệu lineage từ DAG lineage_demo có được ghi vào Marquez không
        """
        if not self.docker_running:
            self.skipTest("Docker không chạy, bỏ qua test này")

        try:
            # Đợi một chút để đảm bảo dữ liệu đã được ghi vào Marquez
            time.sleep(2)

            # Kiểm tra xem có job nào trong namespace mặc định không
            response = requests.get(f"{self.marquez_api_url}/namespaces/{self.namespace}/jobs")

            self.assertEqual(response.status_code, 200, "Không thể lấy danh sách job từ Marquez API")

            jobs = response.json()
            job_names = [job.get('name') for job in jobs]

            # Kiểm tra xem có job nào liên quan đến lineage_demo không
            lineage_jobs = [job for job in job_names if 'lineage_demo' in job]

            # Nếu không tìm thấy job nào, test sẽ fail
            self.assertTrue(len(lineage_jobs) > 0,
                           "Không tìm thấy job nào liên quan đến 'lineage_demo' trong Marquez")

            print(f"✅ Tìm thấy {len(lineage_jobs)} job liên quan đến 'lineage_demo' trong Marquez")
            print(f"Các job: {lineage_jobs}")
        except requests.RequestException as e:
            self.fail(f"Không thể kết nối đến Marquez API: {str(e)}")

if __name__ == '__main__':
    unittest.main()
