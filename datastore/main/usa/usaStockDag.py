from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from main.usa.usaStock import fetch_us_stock_data, process_stock_data, register_stock_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="us_stock_data_pipeline",
    default_args=default_args,
    description="미국 주식 데이터를 수집, 가공, 등록하는 파이프라인 DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

def collect(**context):
    """
    수집 태스크: 미국 주식 데이터를 수집하여 XCom으로 전달합니다.
    """
    symbol = "AAPL"  # 예시 심볼
    data = fetch_us_stock_data(symbol)
    return data

def process(**context):
    """
    가공 태스크: collect 태스크에서 전달받은 데이터를 가공합니다.
    """
    ti = context["ti"]
    collected_data = ti.xcom_pull(task_ids="collect_task")
    if not collected_data:
        raise ValueError("수집된 데이터가 없습니다.")
    processed = process_stock_data(collected_data)
    return processed

def register(**context):
    """
    등록 태스크: 가공 태스크의 결과를 등록(저장 또는 출력)합니다.
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
    dag=dag,
)

process_task = PythonOperator(
    task_id="process_task",
    python_callable=process,
    provide_context=True,
    dag=dag,
)

register_task = PythonOperator(
    task_id="register_task",
    python_callable=register,
    provide_context=True,
    dag=dag,
)

collect_task >> process_task >> register_task
