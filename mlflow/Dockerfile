# Python 3.9 이미지를 기반으로 빌드
FROM python:3.9

# MLflow 및 PostgreSQL 연동을 위한 psycopg2 설치
RUN pip install mlflow psycopg2-binary

# 컨테이너의 5000 포트를 열어 외부에서 접속 가능하도록 설정
EXPOSE 5000

# MLflow 서버 명령어 실행 (docker-compose에서 이 명령어를 설정하므로 Dockerfile에서는 CMD를 생략할 수 있습니다)
CMD mlflow server \
    --backend-store-uri postgresql://postgres:connectslab@172.31.29.95:5432/mlflow_db \
    --default-artifact-root /mlflow/mlruns \
    --host 0.0.0.0 \
    --port 5000
