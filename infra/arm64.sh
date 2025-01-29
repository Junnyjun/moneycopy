# 1. Airflow 최신 버전 가져오기
git clone https://github.com/apache/airflow.git
cd airflow

# 2. ARM64용 Docker 이미지 빌드
docker buildx build --platform linux/arm64 -t airflow-arm64 .
