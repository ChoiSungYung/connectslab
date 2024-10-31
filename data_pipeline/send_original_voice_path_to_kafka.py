'''
S3에 위치한 원본 음성 데이터 경로(처리되지 않은)를 받아서 Kafka에 Produce 하는 파이썬 파일 
파이썬 파일을 실행하기 위해 임시적으로 pyenv 가상 환경 (data_pipline_env)를 활용하고 있다.
궁극적으로 이 파이썬 파일을 도커 컨테이너로 옮겨서 컨테이너 상에서 실행할 것이다. 
'''

def send_kafka(unprocessed_path):
    print(f"unprocessed_path : {unprocessed_path}")
    
    # unprocessed_path에 담긴 원본 음성 데이터 path를 Kafka에 Produce 한다. 
    
    
    # 해당 음성 데이터 path를 Kakfa에 Produce 하면 메타 데이터에 있는 Metadata 속성에 '처리 경험 있음' 이런 식으로 저장 
    
    
    
    