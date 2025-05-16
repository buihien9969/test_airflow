import unittest
import os
import yaml
import re

class TestDockerCompose(unittest.TestCase):
    """
    Kiểm tra cấu hình docker-compose.yaml
    """
    
    def setUp(self):
        """
        Thiết lập đường dẫn đến file docker-compose.yaml
        """
        self.docker_compose_path = os.path.join(os.path.dirname(__file__), '../airflow-docker/docker-compose.yaml')
        
        # Đọc file docker-compose.yaml
        with open(self.docker_compose_path, 'r') as f:
            self.docker_compose = yaml.safe_load(f)
    
    def test_docker_compose_file_exists(self):
        """
        Kiểm tra xem file docker-compose.yaml có tồn tại không
        """
        self.assertTrue(
            os.path.exists(self.docker_compose_path),
            f"File docker-compose.yaml không tồn tại tại đường dẫn: {self.docker_compose_path}"
        )
        print("✅ File docker-compose.yaml tồn tại")
    
    def test_required_services_exist(self):
        """
        Kiểm tra xem các service cần thiết có tồn tại trong file docker-compose.yaml không
        """
        required_services = [
            'postgres', 
            'marquez-db', 
            'marquez', 
            'redis', 
            'airflow-apiserver', 
            'airflow-scheduler', 
            'airflow-worker', 
            'airflow-triggerer', 
            'airflow-init'
        ]
        
        for service in required_services:
            self.assertIn(
                service,
                self.docker_compose['services'],
                f"Service '{service}' không tồn tại trong file docker-compose.yaml"
            )
        
        print(f"✅ Tất cả các service cần thiết đều tồn tại: {required_services}")
    
    def test_airflow_common_config(self):
        """
        Kiểm tra cấu hình chung của Airflow
        """
        airflow_common = self.docker_compose.get('x-airflow-common', {})
        
        # Kiểm tra xem có sử dụng Dockerfile không
        self.assertIn(
            'build',
            airflow_common,
            "Không tìm thấy cấu hình 'build' trong x-airflow-common"
        )
        
        # Kiểm tra các biến môi trường cần thiết
        required_env_vars = [
            'AIRFLOW__CORE__EXECUTOR', 
            'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 
            'AIRFLOW__CELERY__RESULT_BACKEND', 
            'AIRFLOW__CELERY__BROKER_URL',
            'AIRFLOW__CORE__LOAD_EXAMPLES',
            'AIRFLOW_CONFIG'
        ]
        
        for env_var in required_env_vars:
            self.assertIn(
                env_var,
                airflow_common.get('environment', {}),
                f"Biến môi trường '{env_var}' không tồn tại trong x-airflow-common"
            )
        
        # Kiểm tra các volume được mount
        required_volume_patterns = [
            r'/dags:/opt/airflow/dags',
            r'/logs:/opt/airflow/logs',
            r'/config:/opt/airflow/config',
            r'/plugins:/opt/airflow/plugins'
        ]
        
        volumes = airflow_common.get('volumes', [])
        for pattern in required_volume_patterns:
            pattern_found = False
            for volume in volumes:
                if re.search(pattern, volume):
                    pattern_found = True
                    break
            
            self.assertTrue(
                pattern_found,
                f"Volume pattern '{pattern}' không được tìm thấy trong x-airflow-common"
            )
        
        print("✅ Cấu hình chung của Airflow đúng")
    
    def test_postgres_config(self):
        """
        Kiểm tra cấu hình của service postgres
        """
        postgres_service = self.docker_compose['services']['postgres']
        
        # Kiểm tra image
        self.assertEqual(
            postgres_service.get('image', ''),
            'postgres:13',
            "Image của service 'postgres' không phải là 'postgres:13'"
        )
        
        # Kiểm tra các biến môi trường cần thiết
        required_env_vars = ['POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB']
        
        for env_var in required_env_vars:
            self.assertIn(
                env_var,
                postgres_service.get('environment', {}),
                f"Biến môi trường '{env_var}' không tồn tại trong service 'postgres'"
            )
        
        # Kiểm tra healthcheck
        self.assertIn(
            'healthcheck',
            postgres_service,
            "Không tìm thấy cấu hình 'healthcheck' trong service 'postgres'"
        )
        
        print("✅ Service 'postgres' được cấu hình đúng")
    
    def test_marquez_config(self):
        """
        Kiểm tra cấu hình của service marquez
        """
        marquez_service = self.docker_compose['services']['marquez']
        
        # Kiểm tra xem service marquez có phụ thuộc vào marquez-db không
        self.assertIn(
            'marquez-db',
            marquez_service.get('depends_on', {}),
            "Service 'marquez' không phụ thuộc vào 'marquez-db'"
        )
        
        # Kiểm tra các biến môi trường cần thiết
        required_env_vars = ['DB_HOST', 'DB_NAME', 'DB_PASSWORD', 'DB_PORT', 'DB_USER', 'MARQUEZ_NAMESPACE']
        
        for env_var in required_env_vars:
            self.assertIn(
                env_var,
                marquez_service.get('environment', {}),
                f"Biến môi trường '{env_var}' không tồn tại trong service 'marquez'"
            )
        
        # Kiểm tra các port được expose
        port_5000_found = False
        port_3000_found = False
        
        for port_mapping in marquez_service.get('ports', []):
            port_str = str(port_mapping)
            if '5000' in port_str:
                port_5000_found = True
            if '3000' in port_str:
                port_3000_found = True
        
        self.assertTrue(
            port_5000_found,
            "Port 5000 (API) không được expose trong service 'marquez'"
        )
        
        self.assertTrue(
            port_3000_found,
            "Port 3000 (UI) không được expose trong service 'marquez'"
        )
        
        print("✅ Service 'marquez' được cấu hình đúng")
    
    def test_volumes_defined(self):
        """
        Kiểm tra xem các volume cần thiết có được định nghĩa không
        """
        required_volumes = ['postgres-db-volume', 'marquez-db-volume']
        
        for volume in required_volumes:
            self.assertIn(
                volume,
                self.docker_compose.get('volumes', {}),
                f"Volume '{volume}' không được định nghĩa trong docker-compose.yaml"
            )
        
        print(f"✅ Tất cả các volume cần thiết đều được định nghĩa: {required_volumes}")

if __name__ == '__main__':
    unittest.main()
