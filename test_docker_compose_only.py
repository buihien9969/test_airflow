import unittest
import sys
import os

# Thêm thư mục hiện tại vào sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import test case
from tests.test_docker_compose import TestDockerCompose

if __name__ == '__main__':
    # Tạo test suite
    test_suite = unittest.TestSuite()
    
    # Thêm test case vào test suite
    test_suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestDockerCompose))
    
    # Chạy test suite
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Kết thúc với mã lỗi phù hợp
    sys.exit(not result.wasSuccessful())
