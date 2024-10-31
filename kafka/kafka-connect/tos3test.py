import os
import asyncio
import wave
import io
import numpy as np
import boto3  # AWS S3에 업로드하기 위해 boto3 추가
from kafka import KafkaConsumer
from collections import defaultdict
from queue import Queue

# S3 클라이언트 설정
s3_client = boto3.client('s3')

# Kafka 컨슈머 설정
consumer = KafkaConsumer(
    'test2',  # 수신할 토픽 이름
    bootstrap_servers='172.31.28.191:9092',
    auto_offset_reset='earliest',  # 처음부터 메시지를 읽음
    enable_auto_commit=True,  # 자동으로 오프셋을 커밋
    value_deserializer=lambda v: v  # 역직렬화하지 않음
)

# 파일 청크와 파일 정보 저장소
file_chunks = defaultdict(list)
file_chunk_info = {}

# WAV 데이터 저장 큐
wav_queue = Queue()

# 동시 처리 제한 (최대 동시 처리 수를 설정)
semaphore = asyncio.Semaphore(5)

# 비동기 메시지 처리 함수 (메모리에 청크 저장)
async def process_message_async(message):
    async with semaphore:  # 동시 처리 제한
        try:
            # 파일 ID, 청크 인덱스, 총 청크 수 추출
            file_id = message.headers[2][1].decode('utf-8')
            chunk_index = int(message.headers[0][1].decode('utf-8'))
            total_chunks = int(message.headers[1][1].decode('utf-8'))

            print(f"[INFO] Received chunk {chunk_index + 1}/{total_chunks} for file {file_id}.")

            # 파일 청크를 메모리에 저장
            file_chunks[file_id].append((chunk_index, message.value))
            file_chunk_info[file_id] = total_chunks

            # 모든 청크가 수신되었는지 확인
            if len(file_chunks[file_id]) == total_chunks:
                print(f"[INFO] All chunks received for file {file_id}. Reassembling...")

                # 청크를 인덱스 순서대로 정렬하여 재조립
                file_chunks[file_id].sort(key=lambda x: x[0])
                file_content = b''.join([chunk[1] for chunk in file_chunks[file_id]])

                # WAV 파일로 변환 후 큐에 저장
                wav_data = create_wav_in_memory(file_content)
                queue_wav_file(file_id, wav_data)

                # 메모리에서 청크 데이터 삭제
                del file_chunks[file_id]
                del file_chunk_info[file_id]

        except Exception as e:
            print(f"[ERROR] Error processing message for file {file_id}: {e}")

# WAV 파일 생성 (메모리 내에서 처리)
def create_wav_in_memory(raw_audio_data):
    # 새로운 WAV 파일을 메모리에서 처리하기 위해 BytesIO 사용
    wav_io = io.BytesIO()
    with wave.open(wav_io, 'wb') as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(44100)
        wav_file.writeframes(raw_audio_data)
    wav_io.seek(0)
    return wav_io.read()

# WAV 데이터를 numpy 배열로 변환하는 함수
def wav_to_np_array(wav_data):
    wav_io = io.BytesIO(wav_data)
    with wave.open(wav_io, 'rb') as wav_file:
        audio_data = wav_file.readframes(wav_file.getnframes())
        audio_array = np.frombuffer(audio_data, dtype=np.int16)
    return audio_array

# 큐에 WAV 데이터 저장
def queue_wav_file(file_id, wav_data):
    wav_queue.put((file_id, wav_data))
    print(f"[INFO] {file_id} WAV 데이터를 큐에 저장했습니다.")

# Whisper 모델을 호출하는 함수 (처리 완료 후 S3 업로드)
def process_with_whisper():
    while True:
        if not wav_queue.empty():
            file_id, wav_data = wav_queue.get()
            print(f"[INFO] Processing audio file {file_id} with Whisper model...")
            
            try:
                # WAV 데이터를 numpy 배열로 변환하여 Whisper에 전달
                audio_array = wav_to_np_array(wav_data)
                
                # Whisper 모델 처리 (실제 모델 호출 예제)
                # 예: processed_data = whisper_model.process(audio_array)
                processed_data = audio_array  # 모델 결과로 가정
                
                # 처리된 데이터를 S3에 업로드
                trimmed_file_name = f"{file_id}_processed.wav"
                with open(trimmed_file_name, 'wb') as f:
                    f.write(processed_data)

                print(f"[INFO] Uploading {trimmed_file_name} to S3...")
                s3_client.upload_file(trimmed_file_name, 'connects-split', trimmed_file_name)
                print(f"[INFO] Uploaded {trimmed_file_name} to S3")

            except Exception as e:
                print(f"[ERROR] Failed to process file {file_id} with Whisper: {e}")

# Kafka 메시지를 비동기 수신 및 처리
async def consume_messages():
    for message in consumer:
        await process_message_async(message)

# 이벤트 루프 실행
loop = asyncio.get_event_loop()
loop.run_until_complete(consume_messages())
