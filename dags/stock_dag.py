# stock_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from stock_collector import fetch_us_stock_data, process_stock_data, register_stock_data

# DAG 기본 인자 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="us_stock_data_pipeline",
        default_args=default_args,
        description="미국 주식 데이터를 수집, 가공, 등록하는 파이프라인 DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2025, 1, 1),
        catchup=False,
) as dag:

    def collect(**context):
        """
        수집 태스크: 미국 주식 데이터를 외부 API에서 수집하고 XCom으로 전달합니다.
        """
        symbol = "AAPL"  # 예시: 애플 주식 데이터 수집 (필요에 따라 파라미터화 가능)
        data = fetch_us_stock_data(symbol)
        # 수집한 데이터를 XCom으로 반환하면, 후속 태스크에서 자동 전달됨
        return data

    def process(**context):
        """
        가공 태스크: 이전 태스크의 XCom에서 수집 데이터를 받아 가공합니다.
        """
        ti = context["ti"]
        collected_data = ti.xcom_pull(task_ids="collect_task")
        if not collected_data:
            raise ValueError("수집된 데이터가 없습니다.")
        processed = process_stock_data(collected_data)
        # 가공 결과를 XCom으로 반환하여 다음 태스크에서 사용
        return processed

    def register(**context):
        """
        등록 태스크: 가공된 데이터를 XCom에서 가져와 등록(저장) 처리합니다.
        """
        ti = context["ti"]
        processed_data = ti.xcom_pull(task_ids="process_task")
        if not processed_data:
            raise ValueError("가공된 데이터가 없습니다.")
        register_stock_data(processed_data)

    collect_task = PythonOperator(
        task_id="collect_task",
        python_callable=collect,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id="process_task",
        python_callable=process,
        provide_context=True,
    )

    register_task = PythonOperator(
        task_id="register_task",
        python_callable=register,
        provide_context=True,
    )

    # 태스크 간 순차 실행: 수집 → 가공 → 등록
    collect_task >> process_task >> register_task
