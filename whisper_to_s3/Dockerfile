# Python 3.9 이미지 기반
FROM python:3.9

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 시스템 패키지 설치
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    apt-get clean

# 필요한 Python 라이브러리 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# .env 파일 복사
COPY .env .

# 오디오와 텍스트 디렉토리 복사 (디렉토리가 없으면 자동 생성됨)
RUN mkdir -p /app/audio /app/split_audio /app/split_text

# Whisper 스크립트 복사
COPY main.py .

# 환경 변수 로드
RUN pip install python-dotenv

# 엔트리포인트 설정
CMD ["python", "main.py"]
