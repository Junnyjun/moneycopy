# stock_collector.py
import requests

def fetch_us_stock_data(symbol: str) -> dict:
    """
    미국 주식 데이터를 외부 API에서 수집하는 함수.
    symbol 매개변수에 따라 데이터를 가져오며, 성공 시 JSON 데이터를 반환합니다.
    """
    # 예시 API 엔드포인트 (실제 사용 시 올바른 URL과 인증 정보 등 필요)
    url = f"https://api.example.com/us_stocks/{symbol}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        # API 호출 실패 시 예외 발생
        response.raise_for_status()

def process_stock_data(data: dict) -> dict:
    """
    수집한 주식 데이터를 가공하여 필요한 정보만 추출하는 함수.
    예시로 심볼, 가격, 거래량, 타임스탬프 정보를 추출합니다.
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
    가공된 주식 데이터를 등록(저장)하는 함수.
    여기서는 단순히 결과를 출력하지만, 실제 환경에서는 DB 저장, 파일 기록 등으로 대체 가능합니다.
    """
    # 예시: 데이터를 출력하는 로직 (실제 등록 로직으로 변경)
    print(f"등록 완료: {processed_data}")
