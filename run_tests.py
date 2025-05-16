import unittest
import sys
import os
import importlib.util

# Thêm thư mục hiện tại vào sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import các test case
from tests.test_docker_config import TestDockerConfig
from tests.test_marquez_integration import TestMarquezIntegration
from tests.test_docker_compose import TestDockerCompose

# Import TestDagIntegrity một cách an toàn
test_dags_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tests/test_dags.py')
spec = importlib.util.spec_from_file_location("test_dags", test_dags_path)
test_dags = importlib.util.module_from_spec(spec)
spec.loader.exec_module(test_dags)
TestDagIntegrity = test_dags.TestDagIntegrity

if __name__ == '__main__':
    # Tạo test suite
    test_suite = unittest.TestSuite()

    # Thêm các test case vào test suite
    test_suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestDockerConfig))
    test_suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestDagIntegrity))
    test_suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestMarquezIntegration))
    test_suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestDockerCompose))

    # Chạy test suite
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Kết thúc với mã lỗi phù hợp
    sys.exit(not result.wasSuccessful())
