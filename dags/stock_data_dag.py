from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import yfinance as yf
import pandas as pd

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
        'stock_data_collection',
        default_args=default_args,
        description='Collect stock data using yfinance',
        schedule_interval='0 6 * * *',  # 매일 오전 6시에 실행
        start_date=datetime(2025, 1, 1),
        catchup=False,
) as dag:

    # Python 함수 정의
    def fetch_stock_data(**kwargs):
        ticker = kwargs.get('ticker', 'AAPL')  # 기본 티커는 AAPL
        output_file = kwargs.get('output_file', f'/tmp/{ticker}_data.csv')

        # 주식 데이터 다운로드
        data = yf.download(ticker, period='1d', interval='1m')

        if not data.empty:
            # 데이터 저장
            data.to_csv(output_file)
            print(f"Stock data saved to {output_file}")
        else:
            print(f"No data fetched for {ticker}")

    # PythonOperator 생성
    fetch_data_task = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data,
        op_kwargs={
            'ticker': 'AAPL',  # 원하는 주식 심볼
            'output_file': '/tmp/AAPL_stock_data.csv',  # CSV 저장 경로
        },
    )

    # Task 연결
    fetch_data_task
