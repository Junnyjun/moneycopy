import unittest

import os

# 테스트 환경에서는 DAG 직렬화 기능을 비활성화하여 DB 접근을 피합니다.
os.environ["AIRFLOW__CORE__STORE_SERIALIZED_DAGS"] = "False"

from airflow.models import DagBag
from main.usa.usaStockDag import dag

class TestUSAStockDAGIntegration(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder=".", include_examples=False)

    def test_dag_loaded(self):
        """
        us_stock_data_pipeline DAG가 올바르게 로드되는지 확인합니다.
        """
        self.assertIn("us_stock_data_pipeline", self.dagbag.dags)
        dag_obj = self.dagbag.get_dag("us_stock_data_pipeline")
        self.assertIsNotNone(dag_obj)
        # 본 DAG에는 세 개의 태스크가 있어야 합니다.
        self.assertEqual(len(dag_obj.tasks), 3)

    def test_task_order(self):
        """
        태스크 간 의존성이 올바른지 (collect → process → register) 확인합니다.
        """
        tasks = {t.task_id for t in dag.tasks}
        expected_tasks = {"collect_task", "process_task", "register_task"}
        self.assertEqual(tasks, expected_tasks)

        collect = dag.get_task("collect_task")
        process = dag.get_task("process_task")
        register = dag.get_task("register_task")
        self.assertIn(process.task_id, [t.task_id for t in collect.downstream_list])
        self.assertIn(register.task_id, [t.task_id for t in process.downstream_list])

if __name__ == '__main__':
    unittest.main()
