# test_stock_dag_unit.py
import unittest
from datetime import datetime
from unittest.mock import patch
import pandas as pd

from stock_data_dag import collect_stock_data


class TestStockDAG(unittest.TestCase):
    """주식 데이터 수집 DAG 테스트"""

    def setUp(self):
        """테스트 설정"""
        self.context = {
            "logical_date": datetime(2024, 1, 29),
            "task_instance": None
        }

    @patch('yfinance.Ticker')
    def test_fetch_daily_stock_data(self, mock_ticker):
        """데이터 수집 task 테스트"""
        # Mock 데이터 설정
        mock_history = pd.DataFrame({
            'Open': [150.0],
            'High': [155.0],
            'Low': [149.0],
            'Close': [153.0],
            'Volume': [1000000]
        })
        mock_ticker.return_value.history.return_value = mock_history

        # Task 실행
        result = fetch_daily_stock_data.function(
            symbols=["AAPL"],
            **self.context
        )

        # 결과 검증
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith('.csv'))

if __name__ == '__main__':
    unittest.main()