import os
import boto3
from kafka import KafkaConsumer
from pydub import AudioSegment
from dotenv import load_dotenv
import whisper
import warnings
import io
import tempfile
from collections import defaultdict
from datetime import datetime

# FutureWarning 무시
warnings.filterwarnings("ignore", category=FutureWarning)

# .env 파일의 환경 변수 로드
load_dotenv()

# AWS S3 클라이언트 초기화
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# Whisper 모델 로드
try:
    model = whisper.load_model("base")  # 원하는 모델로 변경 가능
    print("Whisper model loaded successfully.")
except Exception as e:
    print(f"Failed to load Whisper model: {e}")
    exit(1)

# Kafka 소비자 초기화
try:
    consumer = KafkaConsumer(
        'test6',
        bootstrap_servers='172.31.28.191:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        key_deserializer=lambda k: k.decode('utf-8'),
        value_deserializer=lambda v: v  # 바이트 형태로 받음
    )
    print("Kafka consumer initialized successfully.")
except Exception as e:
    print(f"Failed to initialize Kafka consumer: {e}")
    exit(1)

# 청크 데이터를 저장할 딕셔너리
chunks_data = defaultdict(list)

def process_full_audio(file_id, audio_data):
    try:
        print(f"Processing full audio for file ID: {file_id}")
        
        # 임시 파일에 전체 데이터를 쓰기
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_audio_file:
            temp_audio_file.write(audio_data)

            # Whisper 모델로 처리
            result = model.transcribe(temp_audio_file.name)
            print(f"Transcription result: {result}")

            # 오늘 날짜에 맞는 경로 설정
            today = datetime.today()
            year, month, day = today.strftime('%Y'), today.strftime('%m'), today.strftime('%d')

            # 시작 및 끝 시간 확인 및 자르기
            if result.get('segments'):
                for i, segment in enumerate(result['segments']):
                    start_time = segment['start']
                    end_time = segment['end']

                    if start_time < end_time:
                        trimmed_audio = AudioSegment.from_file(temp_audio_file.name)[start_time * 1000:end_time * 1000]

                        # 잘린 파일 경로 지정
                        trimmed_file_name = f"to_do/{year}/{month}/{day}/trimmed_segment_{i}_{file_id}.wav"
                        print(f"Exporting trimmed audio to {trimmed_file_name}...")

                        # 로컬 임시 파일에 저장
                        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as trimmed_temp_file:
                            trimmed_audio.export(trimmed_temp_file.name, format="wav")

                            # S3에 업로드
                            print(f"Uploading {trimmed_file_name} to S3...")
                            s3_client.upload_file(trimmed_temp_file.name, 'connects-split', trimmed_file_name)
                            print(f"Uploaded {trimmed_file_name} to S3")

                            # 임시 파일 삭제
                            os.remove(trimmed_temp_file.name)
                            print(f"Temporary file deleted: {trimmed_temp_file.name}")
                    else:
                        print("Invalid segment duration, skipping trim.")

    except Exception as e:
        print(f"Error processing full audio: {e}")

def assemble_and_process_chunks(key, total_chunks, file_id):
    # 모든 청크가 모이면 처리
    if len(chunks_data[file_id]) == total_chunks:
        print(f"Assembling chunks for {file_id}...")
        # 모든 청크를 순서대로 결합
        audio_data = b''.join([chunk for _, chunk in sorted(chunks_data[file_id])])
        
        # 전체 오디오 파일을 Whisper로 처리
        process_full_audio(file_id, audio_data)
        
        # 처리 완료 후 청크 데이터 삭제
        del chunks_data[file_id]

# Kafka에서 메시지 소비
print("Waiting for messages...")
for message in consumer:
    try:
        key = message.key
        value = message.value
        headers = message.headers

        print(f"Received message:")
        print(f"Key: {key}")
        print(f"Headers: {headers}")
        print(f"Value (bytes): {value[:50]}...")  # 바이트의 일부만 출력 (길어질 경우)

        # 청크 정보 파싱
        chunk_index = int(headers[0][1].decode('utf-8'))
        total_chunks = int(headers[1][1].decode('utf-8'))
        file_id = headers[2][1].decode('utf-8').replace('/', '_')

        # 청크 저장
        chunks_data[file_id].append((chunk_index, value))
        
        # 청크가 모두 수신되면 파일로 결합 및 처리
        assemble_and_process_chunks(key, total_chunks, file_id)

    except Exception as e:
        print(f"Error consuming message: {e}")
