'''
S3로부터 원본 음성 데이터 경로를 가져오는 파이썬 파일 
IAM 권한을 이용해서 Ubuntu Linux 서버가 S3에 접근할 수 있는 권한을 받아야 한다. (AmazonS3FullAccess 권한을 받아야 한다. )
파이썬 파일을 실행하기 위해 임시적으로 pyenv 가상 환경 (data_pipline_env)를 활용하고 있다.
궁극적으로 이 파이썬 파일을 도커 컨테이너로 옮겨서 컨테이너 상에서 실행할 것이다. 
'''

# 파이썬 제공 기본 패키지 모듈 
import os
from datetime import datetime

# 설치된 패키지 모듈 
import boto3
from dotenv import load_dotenv

# 사용자 정의 모듈 
from send_original_voice_path_to_kafka import send_kafka

load_dotenv() # 같은 경로에 .env 파일에 정의된 환경 변수를 가져온다. 

# 리눅스 서버가 S3에 접근할 수 있도록 connection 생성
s3 = boto3.client('s3',  
                  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                  region_name=os.getenv("AWS_REGION"))

bucket_name = os.getenv("connects-origin") # .env 파일에 "AWS_ORIGINAL_VOICE_BUCKET_NAME" 환경 변수에 대한 값을 불러온다. 

unprocessed_path = [] # S3로부터 처리되지 않은 음성 파일 경로를 담는 리스트(List) 

# 원본 음성 데이터가 적재된 S3 Bucket에서 처리되지 않은 파일을 가져오는 함수 
def get_unprocessed_files_for_today(bucket_name):
    # 오늘 날짜를 동적으로 가져오기
    today = datetime.today()
    year = today.strftime('%Y')  # 연
    month = today.strftime('%m')  # 월 (2자리 숫자)
    day = today.strftime('%d')    # 일 (2자리 숫자)
    
    prefix = f"01.rawdata/{year}/{month}/{day}/"  # 원본 음성 데이터를 가져오기 위한 S3 경로 설정 (예: connects-origin/01.rawdata/2024/10/21/)
    
    # print(f"bucket_name : {bucket_name}") # bucket_name : connects-origin 으로 나온다. 
    # print(f"prefix : {prefix}")           # prefix : connects-origin/01.rawdata/2024/10/21/ 이런식으로 나온다. 
    
    # S3에서 해당 경로의 파일 목록 가져오기 
    response = s3.list_objects_v2(Bucket=bucket_name, 
                                  Prefix=prefix)
    
    # 만약 지정한 prefix(경로)를 가지고 원본 음성 데이터가 없는 경우를 대비하여 처리 
    if 'Contents' not in response:
        print(f"{prefix} 경로에 파일이 없습니다.")
        return []
    
    # 각 원본 음성 파일에 대해 메타데이터 확인 및 경로를 확인하기 위해 for문을 탐색한다. 
    for obj in response['Contents']:
        file_key = obj['Key']

        # 음성 파일 객체의 메타데이터 가져오기
        head_response = s3.head_object(Bucket=bucket_name, 
                                       Key=file_key)
        
        # print(f"head_response : {head_response}")

        # 메타데이터에서 'processed' 속성 확인 (없으면 'false'로 처리)
        metadata = head_response.get('Metadata', {}) # Metadata 속성이 빈값이면 {}으로 처리 
        processed = metadata.get('processed', 'false')  # 소문자로 변환하여 'true'인지 확인

        # 'processed'가 'true'가 아닌 경우에만 리스트에 추가
        if processed != 'true':
            unprocessed_files.append(f's3://{bucket_name}/{file_key}')

    return unprocessed_files

unprocessed_path = get_unprocessed_files_for_today(bucket_name) # get_unprocessed_files_for_today 함수를 부른다. 
unprocessed_path = unprocessed_files[1::1]

send_kafka(unprocessed_path) # 같은 경로에 send_original_voice_path_to_kafka.py에 있는 send_kafka 함수를 호출한다. 