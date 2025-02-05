# usaStock.py

import requests

def fetch_us_stock_data(symbol: str) -> dict:
    """
    미국 주식 데이터를 외부 API(예: Alpha Vantage)를 통해 수집합니다.
    실제 사용 시에는 API_KEY를 발급받아 올바른 엔드포인트를 사용하세요.
    """
    API_KEY = "YOUR_API_KEY"  # 발급받은 API 키로 교체
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def process_stock_data(data: dict) -> dict:
    """
    수집한 데이터를 필요한 필드만 추출하여 가공합니다.
    """
    processed = {
        "symbol": data.get("symbol"),
        "price": data.get("price"),
        "volume": data.get("volume"),
        "timestamp": data.get("timestamp")
    }
    return processed

def register_stock_data(processed_data: dict) -> None:
    """
    가공된 데이터를 등록(예: DB 저장 또는 파일 기록)합니다.
    예제에서는 단순 출력으로 결과를 확인합니다.
    """
    print(f"등록 완료: {processed_data}")
