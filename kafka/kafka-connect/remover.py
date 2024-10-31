import boto3
from datetime import datetime
from dotenv import load_dotenv
import os

# .env 파일의 환경 변수 로드
load_dotenv()

# AWS 자격 증명 설정
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# 버킷 이름 및 기본 폴더 경로
BUCKET_NAME = 'connects-origin'  # S3 버킷 이름
FOLDER_PREFIX = '01.rawdata/'  # 조회하려는 디렉토리 경로

# S3 객체의 메타데이터에서 'processed' 값 제거
def remove_processed_metadata(bucket_name, object_key):
    try:
        # 먼저 해당 객체의 메타데이터를 가져옵니다
        response = s3.head_object(Bucket=bucket_name, Key=object_key)
        metadata = response['Metadata']
        
        # 'processed' 키를 메타데이터에서 제거
        if 'processed' in metadata:
            del metadata['processed']
        
        # 메타데이터 업데이트 (삭제된 상태로 복사)
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': object_key},
            Key=object_key,
            Metadata=metadata,  # 'processed'가 제거된 메타데이터로 교체
            MetadataDirective='REPLACE'  # 기존 메타데이터를 대체
        )
        print(f"Processed metadata removed for {object_key}.")
    except Exception as e:
        print(f"Error removing processed metadata for {object_key}: {e}")

# 입력된 날짜에 해당하는 경로의 모든 파일에서 'processed' 메타데이터를 제거
def reset_processed_metadata_by_date(date_input):
    try:
        # 입력된 날짜를 datetime 객체로 변환
        date_obj = datetime.strptime(date_input, "%Y-%m-%d")
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d")
        
        # 동적으로 경로 생성
        folder_path = f"{FOLDER_PREFIX}{year}/{month}/{day}/"
        
        # 해당 경로의 파일 목록 가져오기
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder_path)

        if 'Contents' in response and len(response['Contents']) > 0:
            files = response['Contents']
            print(f"총 {len(files)}개의 파일이 {folder_path}에 있습니다.")
            
            # 모든 파일에 대해 반복하며 'processed' 메타데이터 제거
            for file_obj in files:
                object_key = file_obj['Key']
                # 폴더 자체(접미사에 "/"가 있음)는 제외
                if object_key.endswith("/"):
                    continue

                # 각 파일의 'processed' 메타데이터 제거
                remove_processed_metadata(BUCKET_NAME, object_key)
        else:
            print(f"{folder_path}에 파일이 없습니다.")
    
    except ValueError:
        print("날짜 형식이 잘못되었습니다. YYYY-MM-DD 형식으로 입력해 주세요.")

# 예시: '2024-10-13' 날짜에 해당하는 파일들의 메타데이터에서 'processed' 값 제거
reset_processed_metadata_by_date('2024-10-25')
