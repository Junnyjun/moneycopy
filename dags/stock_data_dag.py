"""
### US Stock Market Data Collection DAG
수집 대상: S&P 500 주요 기업들의 일별 주가 데이터
수집 주기: 매일 장 마감 후 (미국 주중)
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
import yfinance as yf
import pandas as pd


# DAG의 기본 설정값
default_args: Dict[str, Any] = {
    "owner": "airflow",  # DAG 소유자
    "retries": 3,  # 실패시 재시도 횟수
    "retry_delay": timedelta(minutes=5),  # 재시도 간격
    "depends_on_past": False,  # 이전 DAG 실행 결과에 의존하지 않음
}

# 수집할 주식 심볼 리스트
STOCK_SYMBOLS = [
    "AAPL",  # Apple
    "MSFT",  # Microsoft
    "GOOGL", # Google
    "AMZN",  # Amazon
    "META",  # Meta
]

@dag(
    dag_id="us_stock_daily_data",  # DAG 이름
    schedule_interval="0 2 * * 2-6",  # 미국 장 마감 후 (UTC 기준 다음날 02:00)
    start_date=days_ago(1),  # 시작일
    catchup=False,  # 과거 데이터 백필 하지 않음
    default_args=default_args,
    tags=["stock", "finance", "US"],  # DAG 태그
)
def collect_stock_data():
    """미국 주요 기업들의 주가 데이터를 수집하는 DAG"""

    @task()
    def fetch_daily_stock_data(symbols: list = STOCK_SYMBOLS, **context) -> str:
        """
        yfinance를 사용하여 주식 데이터를 수집하는 task
        
        Args:
            symbols: 수집할 주식 심볼 리스트
            
        Returns:
            저장된 파일 경로
        """
        execution_date = context["logical_date"]
        all_stock_data = []

        for symbol in symbols:
            try:
                # 주식 데이터 수집
                stock = yf.Ticker(symbol)
                df = stock.history(
                    start=execution_date,
                    end=execution_date + timedelta(days=1),
                    interval="1d"
                )

                if not df.empty:
                    df["symbol"] = symbol
                    df = df.reset_index()
                    all_stock_data.append(df)

            except Exception as e:
                print(f"Error fetching {symbol}: {str(e)}")
                continue

        if not all_stock_data:
            raise ValueError("No stock data collected")

        # 모든 데이터 합치기
        result_df = pd.concat(all_stock_data, ignore_index=True)

        # 파일로 저장
        date_str = execution_date.strftime("%Y-%m-%d")
        output_path = f"/opt/airflow/data/stocks_{date_str}.csv"
        result_df.to_csv(output_path, index=False)

        return output_path

    @task()
    def process_stock_data(file_path: str) -> None:
        """
        수집된 주식 데이터를 처리하는 task
        
        Args:
            file_path: 처리할 파일 경로
        """
        df = pd.read_csv(file_path)

        # 여기에 데이터 처리 로직 추가
        # 예: 이동평균 계산, 기술적 지표 계산 등

        processed_path = file_path.replace(".csv", "_processed.csv")
        df.to_csv(processed_path, index=False)

    # Task 의존성 설정
    stock_file = fetch_daily_stock_data()
    process_stock_data(stock_file)

# DAG 인스턴스 생성
dag_instance = collect_stock_data()