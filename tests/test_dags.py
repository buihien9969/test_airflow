import unittest
import os
import sys
import yaml
import re
from datetime import datetime, timedelta

# Thêm thư mục dags vào sys.path để có thể import các module từ đó
sys.path.append(os.path.join(os.path.dirname(__file__), '../airflow-docker/dags'))

class TestDagIntegrity(unittest.TestCase):
    """
    Kiểm tra tính toàn vẹn của các DAG bằng cách phân tích mã nguồn
    thay vì sử dụng DagBag của Airflow (để tránh lỗi trên Windows)
    """

    def setUp(self):
        """
        Thiết lập đường dẫn đến thư mục dags
        """
        self.dags_folder = os.path.join(os.path.dirname(__file__), '../airflow-docker/dags')
        self.dag_files = self._get_dag_files()
        self.dag_contents = self._read_dag_files()

    def _get_dag_files(self):
        """
        Lấy danh sách các file DAG trong thư mục dags
        """
        dag_files = []
        for file in os.listdir(self.dags_folder):
            if file.endswith('.py'):
                dag_files.append(os.path.join(self.dags_folder, file))
        return dag_files

    def _read_dag_files(self):
        """
        Đọc nội dung của các file DAG
        """
        dag_contents = {}
        for file_path in self.dag_files:
            with open(file_path, 'r') as f:
                dag_contents[file_path] = f.read()
        return dag_contents

    def _extract_dag_ids(self):
        """
        Trích xuất các DAG ID từ nội dung của các file DAG
        """
        dag_ids = []
        for file_path, content in self.dag_contents.items():
            # Tìm các dòng khai báo DAG
            dag_matches = re.findall(r"dag\s*=\s*DAG\(\s*['\"]([^'\"]+)['\"]", content)
            dag_matches += re.findall(r"dag_id\s*=\s*['\"]([^'\"]+)['\"]", content)

            # Thêm các DAG ID vào danh sách
            dag_ids.extend(dag_matches)

        return list(set(dag_ids))  # Loại bỏ các DAG ID trùng lặp

    def _extract_tasks(self, dag_id):
        """
        Trích xuất các task của một DAG từ nội dung của các file DAG
        """
        tasks = []
        for file_path, content in self.dag_contents.items():
            # Tìm các dòng khai báo task
            if dag_id in content:
                # Tìm các task_id trong các task
                task_matches = re.findall(r"task_id\s*=\s*['\"]([^'\"]+)['\"]", content)
                tasks.extend(task_matches)

                # Tìm các biến task
                task_var_matches = re.findall(r"(\w+)_task\s*=", content)
                tasks.extend([f"{var}_task" for var in task_var_matches])

        return list(set(tasks))  # Loại bỏ các task trùng lặp

    def _extract_dependencies(self, dag_id):
        """
        Trích xuất các dependency của một DAG từ nội dung của các file DAG
        """
        dependencies = []
        for file_path, content in self.dag_contents.items():
            if dag_id in content:
                # Tìm các dòng khai báo dependency
                dep_matches = re.findall(r"([a-zA-Z0-9_]+)\s*>>\s*([a-zA-Z0-9_]+)", content)

                # Tìm các dòng khai báo dependency dạng chuỗi
                dep_chain_matches = re.findall(r"([a-zA-Z0-9_]+)\s*>>\s*([a-zA-Z0-9_]+)\s*>>\s*([a-zA-Z0-9_]+)", content)

                # Thêm các dependency từ chuỗi
                for match in dep_chain_matches:
                    if len(match) >= 3:
                        dependencies.append((match[0], match[1]))
                        dependencies.append((match[1], match[2]))

                dependencies.extend(dep_matches)

        return dependencies

    def test_dag_files_exist(self):
        """
        Kiểm tra xem có file DAG nào trong thư mục dags không
        """
        self.assertTrue(len(self.dag_files) > 0, "Không tìm thấy file DAG nào trong thư mục dags")
        print(f"✅ Tìm thấy {len(self.dag_files)} file DAG trong thư mục dags")

    def test_expected_dags_exist(self):
        """
        Kiểm tra xem các DAG mong đợi có tồn tại không
        """
        expected_dag_ids = ['fetch_and_store_amazon_books', 'lineage_demo']
        dag_ids = self._extract_dag_ids()

        for dag_id in expected_dag_ids:
            self.assertIn(
                dag_id,
                dag_ids,
                f"DAG '{dag_id}' không tồn tại trong danh sách DAG"
            )

        print(f"✅ Tất cả các DAG mong đợi đều tồn tại: {expected_dag_ids}")

    def test_dag_fetch_and_store_amazon_books(self):
        """
        Kiểm tra cấu trúc của DAG fetch_and_store_amazon_books
        """
        dag_id = 'fetch_and_store_amazon_books'

        # Kiểm tra các task trong DAG
        expected_tasks = ['fetch_book_data', 'create_table', 'insert_book_data']
        tasks = self._extract_tasks(dag_id)

        # Kiểm tra xem các task có tồn tại không (có thể có hậu tố _task)
        for task_id in expected_tasks:
            found = False
            for actual_task in tasks:
                if task_id in actual_task:
                    found = True
                    break

            self.assertTrue(
                found,
                f"Task '{task_id}' không tồn tại trong DAG '{dag_id}'"
            )

        # Kiểm tra các dependency trong DAG
        dependencies = self._extract_dependencies(dag_id)

        # Kiểm tra fetch_book_data_task >> create_table_task
        fetch_create_found = False
        for upstream, downstream in dependencies:
            if 'fetch_book_data' in upstream and 'create_table' in downstream:
                fetch_create_found = True
                break

        self.assertTrue(
            fetch_create_found,
            "Dependency 'fetch_book_data_task >> create_table_task' không tồn tại"
        )

        # Kiểm tra create_table_task >> insert_book_data_task
        create_insert_found = False
        for upstream, downstream in dependencies:
            if 'create_table' in upstream and 'insert_book_data' in downstream:
                create_insert_found = True
                break

        self.assertTrue(
            create_insert_found,
            "Dependency 'create_table_task >> insert_book_data_task' không tồn tại"
        )

        print(f"✅ DAG '{dag_id}' có cấu trúc đúng")

    def test_dag_lineage_demo(self):
        """
        Kiểm tra cấu trúc của DAG lineage_demo
        """
        dag_id = 'lineage_demo'

        # Kiểm tra các task trong DAG
        expected_tasks = ['start', 'end']
        tasks = self._extract_tasks(dag_id)

        for task_id in expected_tasks:
            self.assertIn(
                task_id,
                tasks,
                f"Task '{task_id}' không tồn tại trong DAG '{dag_id}'"
            )

        # Kiểm tra các dependency trong DAG
        dependencies = self._extract_dependencies(dag_id)

        # Kiểm tra start >> end
        self.assertIn(
            ('start', 'end'),
            dependencies,
            "Dependency 'start >> end' không tồn tại"
        )

        print(f"✅ DAG '{dag_id}' có cấu trúc đúng")

if __name__ == '__main__':
    unittest.main()
