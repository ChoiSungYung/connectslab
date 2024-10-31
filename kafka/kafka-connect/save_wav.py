import os
import asyncio
import wave
import io
from kafka import KafkaConsumer
from collections import defaultdict

# Kafka 컨슈머 초기화
consumer = KafkaConsumer(
    'test2',  # 청취할 토픽 이름
    bootstrap_servers='172.31.28.191:9092',
    auto_offset_reset='earliest',  # 처음부터 메시지를 읽음
    enable_auto_commit=True,  # 자동으로 오프셋을 커밋
    value_deserializer=lambda v: v  # 역직렬화하지 않음
)

# 파일별로 청크를 메모리에 저장할 사전 (file_id를 키로 사용)
file_chunks = defaultdict(list)
# 전체 파일 크기 및 상태 추적
file_chunk_info = {}

# 동시 처리 제한 (최대 동시 처리 수를 설정)
semaphore = asyncio.Semaphore(5)

# 비동기 메시지 처리 함수 (메모리에 청크 저장)
async def process_message_async(message):
    async with semaphore:  # 동시 처리 제한
        try:
            file_id = message.headers[2][1].decode('utf-8')  # file_id 추출
            chunk_index = int(message.headers[0][1].decode('utf-8'))  # chunk_index 추출
            total_chunks = int(message.headers[1][1].decode('utf-8'))  # total_chunks 추출

            # 파일 청크를 메모리에 저장
            file_chunks[file_id].append((chunk_index, message.value))

            # 파일의 총 청크 정보 기록
            file_chunk_info[file_id] = total_chunks

            # 모든 청크가 수신되었는지 확인
            if len(file_chunks[file_id]) == total_chunks:
                # 청크를 청크 인덱스 순서대로 정렬하여 재조립
                file_chunks[file_id].sort(key=lambda x: x[0])
                file_content = b''.join([chunk[1] for chunk in file_chunks[file_id]])

                # WAV 파일로 변환하여 Whisper 모델에 전달
                wav_data = create_wav_in_memory(file_content)
                handle_completed_wav_file(file_id, wav_data)

                # 메모리에서 청크 데이터 삭제
                del file_chunks[file_id]
                del file_chunk_info[file_id]

        except Exception as e:
            print(f"Error processing message for file {file_id}: {e}")

# WAV 파일 생성 (메모리 내에서 처리)
def create_wav_in_memory(raw_audio_data):
    # 새로운 WAV 파일을 메모리에서 처리하기 위해 BytesIO 사용
    wav_io = io.BytesIO()

    with wave.open(wav_io, 'wb') as wav_file:
        # 채널 수, 샘플 폭, 샘플링 레이트를 지정해야 함 (예: 모노, 16비트, 44100Hz)
        wav_file.setnchannels(1)  # 모노
        wav_file.setsampwidth(2)  # 16비트 (2바이트)
        wav_file.setframerate(44100)  # 44100Hz 샘플링 레이트
        wav_file.writeframes(raw_audio_data)  # WAV 파일에 오디오 데이터 작성

    # 메모리 내에서 생성된 WAV 파일 데이터를 반환
    wav_io.seek(0)
    return wav_io.read()

# 파일이 WAV 형식인지 확인하는 함수
def is_wav_file(wav_data):
    try:
        # 메모리에서 WAV 파일로 열기
        wav_io = io.BytesIO(wav_data)
        with wave.open(wav_io, 'rb') as wav_file:
            # 파일이 정상적으로 열리면, 파일 정보 출력
            channels = wav_file.getnchannels()  # 채널 수
            sample_width = wav_file.getsampwidth()  # 샘플 폭 (바이트 단위)
            frame_rate = wav_file.getframerate()  # 샘플링 레이트 (Hz)
            n_frames = wav_file.getnframes()  # 총 프레임 수

            print(f"Channels: {channels}, Sample Width: {sample_width}, Frame Rate: {frame_rate}, Total Frames: {n_frames}")
            return True
    except wave.Error:
        # wave.Error가 발생하면 WAV 파일이 아님
        return False

# 파일 처리 함수 (완성된 WAV 파일 데이터를 처리)
def handle_completed_wav_file(file_id, wav_data):
    # 파일 ID 출력 (파일 이름)
    print(f"Processing File ID (Name): {file_id}")

    # 파일 크기 출력
    file_size = len(wav_data)
    print(f"File Size: {file_size} bytes")

    # 파일이 WAV 형식인지 확인
    if is_wav_file(wav_data):
        print(f"The file {file_id} is a valid WAV file.")
        
        # 로컬에 파일 저장
        file_name = f"test.wav"  # 파일명에 .wav 확장자 추가
        with open(file_name, 'wb') as f:
            f.write(wav_data)
        print(f"File {file_name} saved locally.")
    else:
        print(f"The file {file_id} is not a valid WAV file.")

# Kafka 메시지 비동기 수신 및 처리
async def consume_messages():
    for message in consumer:
        await process_message_async(message)

# 이벤트 루프 실행
loop = asyncio.get_event_loop()
loop.run_until_complete(consume_messages())
