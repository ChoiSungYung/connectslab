import os
import boto3
from kafka import KafkaConsumer
from pydub import AudioSegment
from dotenv import load_dotenv
import whisper
from datetime import datetime
import io

# .env 파일에서 환경 변수 로드
load_dotenv()

# S3 클라이언트 생성
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                  region_name=os.getenv("AWS_REGION"))

# 분할된 파일을 저장할 S3 버킷
split_bucket_name = "connects-split"

# Whisper 모델 로드
model = whisper.load_model("base")

# Kafka 소비자 초기화
consumer = KafkaConsumer(
    'test6',  # 카프카 토픽 이름
    bootstrap_servers='172.31.28.191:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    key_deserializer=lambda k: k.decode('utf-8'),
    value_deserializer=lambda v: v  # 바이트 형태로 받음
)

# 메시지를 처리하는 함수
def process_audio_message(key, value):
    # 파일 이름과 폴더 구조를 카프카 키로부터 설정
    today = datetime.today()
    year = today.strftime('%Y')
    month = today.strftime('%m')
    day = today.strftime('%d')
    file_name_no_ext = os.path.splitext(os.path.basename(key))[0]

    # 파일 저장할 디렉터리 설정
    audio_dir = os.path.expanduser("/app/audio/")
    os.makedirs(audio_dir, exist_ok=True)
    local_audio_file = os.path.join(audio_dir, f"{file_name_no_ext}.wav")

    # 메모리 내 파일에 저장 후 로드
    audio_data = io.BytesIO(value)
    audio = AudioSegment.from_file(audio_data, format="wav")

    # Whisper로 음성 파일을 텍스트로 변환
    result = model.transcribe(audio_data)

    # 변환된 텍스트 출력
    print(result["text"])

    # 분할된 텍스트와 타임스탬프 저장
    segments = result['segments']

    # 분할된 음성 파일을 저장할 폴더
    output_dir = os.path.expanduser("/app/split_audio")
    os.makedirs(output_dir, exist_ok=True)

    # Kafka 키에서 폴더 경로 구조 만들기
    origin_folder_path = f"to_do/{year}/{month}/{day}"

    # 텍스트 파일 경로 설정
    transcription_file_path = os.path.join(output_dir, f"transcription_{file_name_no_ext}.txt")

    # 텍스트 파일에 저장 시작
    with open(transcription_file_path, 'w', encoding='utf-8') as f:
        for i, segment in enumerate(segments):
            start = segment['start'] * 1000  # milliseconds 단위로 변환
            end = segment['end'] * 1000      # milliseconds 단위로 변환
            audio_segment = audio[start:end]

            # 분할된 음성 파일 로컬에 저장
            output_path = os.path.join(output_dir, f"output_{i}_{file_name_no_ext}.wav")
            audio_segment.export(output_path, format="wav")

            # S3에 저장될 분할된 파일 경로 설정
            split_s3_key = f"{origin_folder_path}/split_output_{i}_{file_name_no_ext}.wav"

            # 분할된 파일을 S3에 업로드
            s3.upload_file(output_path, split_bucket_name, split_s3_key)

            # 세그먼트 텍스트와 시간 정보 파일에 저장
            f.write(f"Segment {i} [{segment['start']} - {segment['end']}]: {segment['text']}\n")
            print(f"Segment {i}: {segment['text']} saved and uploaded to s3://{split_bucket_name}/{split_s3_key}")

    # 텍스트 파일도 S3에 'to_do/' 경로 구조로 업로드
    split_text_s3_key = f"{origin_folder_path}/transcription_{file_name_no_ext}.txt"
    s3.upload_file(transcription_file_path, split_bucket_name, split_text_s3_key)
    print(f"Transcription saved to s3://{split_bucket_name}/{split_text_s3_key}")

# Kafka에서 메시지 소비
print("Waiting for messages...")
for message in consumer:
    try:
        key = message.key  # S3 파일 경로를 Kafka 키로 설정
        value = message.value  # 오디오 청크 데이터

        print(f"Received message with key: {key}")

        # 메시지 처리
        process_audio_message(key, value)

    except Exception as e:
        print(f"Error processing message: {e}")