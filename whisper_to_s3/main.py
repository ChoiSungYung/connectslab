import os
import boto3
import io
from dotenv import load_dotenv
from pydub import AudioSegment
from kafka import KafkaConsumer
import whisper
from threading import Thread

# .env 파일에서 환경 변수 로드
load_dotenv()

# S3 클라이언트 생성
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                  region_name=os.getenv("AWS_REGION"))

# Whisper 모델 로드
try:
    model = whisper.load_model("base")
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
        value_deserializer=lambda v: v
    )
    print("Kafka consumer initialized successfully.")
except Exception as e:
    print(f"Failed to initialize Kafka consumer: {e}")
    exit(1)

# 오디오 처리 함수
def process_audio_chunk(key, value, headers):
    try:
        file_id = headers[2][1].decode('utf-8').replace('/', '_')
        
        # 바이트 데이터를 BytesIO 객체로 변환
        audio_io = io.BytesIO(value)

        # Whisper로 음성 처리
        result = model.transcribe(audio_io)
        start_time = result['segments'][0]['start']
        end_time = result['segments'][-1]['end']

        # 음성 자르기
        audio = AudioSegment.from_wav(audio_io)
        trimmed_audio = audio[start_time * 1000:end_time * 1000]
        trimmed_file_name = f"trimmed_{file_id}.wav"
        trimmed_audio.export(trimmed_file_name, format="wav")

        # S3에 업로드 후 로컬 파일 삭제
        s3.upload_file(trimmed_file_name, 'connects-split', trimmed_file_name)
        os.remove(trimmed_file_name)
        print(f"File {trimmed_file_name} uploaded to S3 successfully.")
        
    except Exception as e:
        print(f"Error processing audio chunk: {e}")

# Kafka 소비를 별도 스레드로 실행
def start_kafka_consumer():
    print("Starting Kafka consumer thread...")
    for message in consumer:
        print(f"Received message with key: {message.key}, headers: {message.headers}")
        process_audio_chunk(message.key, message.value, message.headers)

# 스레드 시작
consumer_thread = Thread(target=start_kafka_consumer)
consumer_thread.start()