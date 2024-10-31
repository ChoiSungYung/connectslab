import os
from datetime import datetime, timedelta
import boto3
from kafka import KafkaProducer
from dotenv import load_dotenv
import traceback

# .env 파일의 환경 변수 로드
load_dotenv()

# AWS S3 클라이언트 초기화
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# Kafka 프로듀서 초기화 (바이트 직렬화)
producer = KafkaProducer(
    bootstrap_servers='172.31.28.191:9092',  # Kafka 브로커 주소
    value_serializer=lambda v: v,  # 바이너리 데이터는 직렬화하지 않음
    max_request_size=2 * 1024 * 1024
)

# S3 버킷 이름 및 기본 폴더 경로
BUCKET_NAME = 'connects-origin'  # S3 버킷 이름
FOLDER_PREFIX = '01.rawdata/'  # 조회하려는 디렉토리 경로

# 로그 파일 경로
LOG_FILE = "log.txt"

# 로그 파일에서 마지막 처리된 날짜 추출
def get_last_processed_date_from_log():
    if not os.path.exists(LOG_FILE):
        print("Log file does not exist. Processing from the beginning.")
        return None

    with open(LOG_FILE, "r") as log_file:
        lines = log_file.readlines()

        # 파일의 마지막 줄에서 날짜 정보 추출
        for line in reversed(lines):
            if "- 전송 완료" in line:
                try:
                    # 로그 형식: "2024-10-24 09:31:31: 01.rawdata/2024/10/13/...wav - 전송 완료"
                    # 먼저 ": "로 분리하여 객체 키 추출
                    object_key = line.split(": ")[1].split(" ")[0]  # "01.rawdata/2024/10/13/...wav"
                    
                    # 객체 키에서 날짜 부분 추출 (예: "2024/10/13")
                    date_parts = object_key.split("/")[1:4]  # ['2024', '10', '13']
                    date_str = "/".join(date_parts)  # "2024/10/13"
                    
                    print(f"Last processed date found in log: {date_str}")
                    return date_str  # "2024/10/13"
                except IndexError:
                    continue

    print("No valid last processed date found in log.")
    return None

# 특정 객체의 메타데이터 확인
def is_processed(bucket_name, object_key):
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        processed = response['Metadata'].get('processed') == 'true'
        print(f"Metadata for {object_key}: {'processed' if processed else 'not processed'}")
        return processed
    except Exception as e:
        print(f"Error fetching metadata for {object_key}: {e}")
        return False

# 처리 후 메타데이터로 처리 완료 표시
def mark_as_processed(bucket_name, object_key):
    try:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': object_key},
            Key=object_key,
            Metadata={'processed': 'true'},
            MetadataDirective='REPLACE'
        )
        print(f"Marked {object_key} as processed.")
    except Exception as e:
        print(f"Error marking {object_key} as processed: {e}")

# S3 객체를 청크 단위로 Kafka로 전송하는 함수
def send_s3_file_to_kafka(bucket_name, object_key):
    try:
        # S3 객체 가져오기
        print(f"Fetching S3 object: {object_key}")
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # 파일을 청크 단위로 읽기
        chunk_size = 1024 * 1024  # 1MB 청크
        total_bytes_sent = 0
        chunk_index = 0  # 청크 인덱스 초기화
        file_id = object_key  # 파일명을 고유 ID로 사용

        # 전체 파일 크기 계산
        file_size = s3_object['ContentLength']
        total_chunks = (file_size + chunk_size - 1) // chunk_size  # 총 청크 수 계산

        while True:
            chunk = s3_object['Body'].read(chunk_size)
            if not chunk:
                break  # 더 이상 읽을 데이터가 없으면 종료

            # 청크 메타데이터를 헤더로 포함하여 전송
            headers = [
                ('chunk_index', str(chunk_index).encode('utf-8')),
                ('total_chunks', str(total_chunks).encode('utf-8')),
                ('file_id', file_id.encode('utf-8') if isinstance(file_id, str) else file_id)
            ]

            # 각 청크를 Kafka로 전송 (헤더 포함)
            future = producer.send('test2', key=object_key.encode('utf-8'), value=chunk, headers=headers)
            future.get(timeout=10)  # 전송이 완료될 때까지 대기

            # 전송된 데이터 크기 누적 및 청크 인덱스 증가
            total_bytes_sent += len(chunk)
            chunk_index += 1
            print(f"Sent chunk {chunk_index}/{total_chunks} of {object_key} to Kafka")

        print(f"Total bytes sent for {object_key}: {total_bytes_sent}")

    except Exception as e:
        print(f"Error sending {object_key} to Kafka: {e}")
        traceback.print_exc()  # 상세 에러 스택 출력

# 메타데이터 검사 후 Kafka로 전송할 파일 리스트 가져오기
def get_unprocessed_files_with_metadata_check(date_input):
    print(f"Checking unprocessed files for date: {date_input}")
    unprocessed_files = []
    folder_path = f"{FOLDER_PREFIX}{date_input}/"

    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder_path)

    if 'Contents' in response and len(response['Contents']) > 0:
        files = response['Contents']
        for file_obj in files:
            object_key = file_obj['Key']
            if object_key.endswith("/"):
                continue

            processed = is_processed(BUCKET_NAME, object_key)
            if not processed:
                unprocessed_files.append(object_key)
    else:
        print(f"No files found for date {date_input}.")
    return unprocessed_files

# 로그 파일 업데이트 함수
def update_log_file(object_key):
    with open(LOG_FILE, "a") as log_file:
        log_file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {object_key} - 전송 완료\n")
    print(f"Log updated for file: {object_key}")

# 전체 작업 수행
def process_files():
    last_processed_date = get_last_processed_date_from_log()
    today = datetime.now()

    if last_processed_date:
        last_processed_dt = datetime.strptime(last_processed_date, "%Y/%m/%d")
        current_date = last_processed_dt - timedelta(days=1)  # 마지막 처리된 날짜의 전날부터 시작

        while current_date <= today:
            date_str = current_date.strftime("%Y/%m/%d")
            print(f"Processing files for date: {date_str}")

            # 메타데이터를 체크하면서 파일을 처리
            files_to_process = get_unprocessed_files_with_metadata_check(date_str)

            for file_key in files_to_process:
                send_s3_file_to_kafka(BUCKET_NAME, file_key)
                mark_as_processed(BUCKET_NAME, file_key)
                update_log_file(file_key)

            current_date += timedelta(days=1)
    else:
        date_str = today.strftime("%Y/%m/%d")
        print(f"Processing files for today: {date_str}")
        files_to_process = get_unprocessed_files_with_metadata_check(date_str)
        for file_key in files_to_process:
            send_s3_file_to_kafka(BUCKET_NAME, file_key)
            mark_as_processed(BUCKET_NAME, file_key)
            update_log_file(file_key)

# 최초 실행
if __name__ == "__main__":
    process_files()
