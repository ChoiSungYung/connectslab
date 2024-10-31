import os
import boto3
from kafka import KafkaConsumer
from pydub import AudioSegment
from dotenv import load_dotenv
import whisper
import warnings
import io
import numpy as np
from collections import defaultdict

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

def process_audio_chunk(file_id, chunk_data):
    try:
        # BytesIO 객체로 변환
        audio_io = io.BytesIO(chunk_data)

        # pydub로 오디오 파일을 읽음
        audio_segment = AudioSegment.from_wav(audio_io)

        # NumPy 배열로 변환하고 정규화
        samples = np.array(audio_segment.get_array_of_samples(), dtype=np.float32)
        if audio_segment.channels > 1:
            samples = samples.reshape((-1, audio_segment.channels))  # 스테레오의 경우 형태 변환
        samples /= np.max(np.abs(samples))  # 정규화

        # Whisper 모델로 처리
        result = model.transcribe(samples)
        print(f"Transcription result for {file_id}: {result}")

        # 잘린 파일 저장
        trimmed_file_name = f"trimmed_{file_id}.wav"
        audio_segment.export(trimmed_file_name, format="wav")

        # S3에 업로드
        print(f"Uploading {trimmed_file_name} to S3...")
        s3_client.upload_file(trimmed_file_name, 'connects-split', trimmed_file_name)
        print(f"Uploaded {trimmed_file_name} to S3")

        # 임시 파일 삭제
        os.remove(trimmed_file_name)
        print(f"Temporary file deleted: {trimmed_file_name}")

    except Exception as e:
        print(f"Error processing audio chunk: {e}")

def assemble_and_process_chunks(file_id, total_chunks):
    # 모든 청크가 모이면 처리
    if len(chunks_data[file_id]) == total_chunks:
        print(f"Assembling chunks for {file_id}...")
        
        # 모든 청크를 순서대로 결합
        audio_data = b''.join([chunk for _, chunk in sorted(chunks_data[file_id])])
        
        # 유효한 WAV 헤더가 있는지 확인
        if audio_data.startswith(b'RIFF'):
            # 전체 오디오 청크를 한 번에 처리
            process_audio_chunk(file_id, audio_data)
        else:
            print(f"Invalid WAV header for file ID: {file_id}")

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
        chunk_index = int(headers[0][1])  # 청크 인덱스
        total_chunks = int(headers[1][1])  # 총 청크 수
        file_id = headers[2][1].decode('utf-8').replace('/', '_')  # 파일 ID

        # 청크 저장
        chunks_data[file_id].append((chunk_index, value))
        # 청크가 모두 수신되면 파일로 결합 및 처리
        assemble_and_process_chunks(file_id, total_chunks)
    except Exception as e:
        print(f"Error consuming message: {e}")