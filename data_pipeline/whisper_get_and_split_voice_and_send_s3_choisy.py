import os
import boto3
from dotenv import load_dotenv
from pydub import AudioSegment
import whisper

# .env 파일에서 환경 변수 로드
load_dotenv()

# S3 클라이언트 생성
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                  region_name=os.getenv("AWS_REGION"))

# S3에서 다운로드할 파일 정보
bucket_name = "connects-origin"
s3_file_key = "01.rawdata/2024/10/25/2_1373G2A4_1447G2A5_T1_2D07T0333C000498_005453_segments_1.mp3"

# 로컬에 저장할 파일 경로 설정
audio_dir = os.path.expanduser("/home/ubuntu/instance1/data_pipeline/audio/")
os.makedirs(audio_dir, exist_ok=True)

# S3 경로에서 파일명을 추출하고 로컬 저장 경로와 결합
file_name = os.path.basename(s3_file_key)
local_audio_file = os.path.join(audio_dir, file_name)

# S3에서 음성 파일 다운로드
print(f"Downloading {s3_file_key} from S3 to {local_audio_file}...")
s3.download_file(bucket_name, s3_file_key, local_audio_file)

# 음성 파일 로드 및 처리
print(f"Loading audio file: {local_audio_file}")
audio = AudioSegment.from_file(local_audio_file)

# Whisper 모델 로드
print("Loading Whisper model...")
model = whisper.load_model("base")

# Whisper로 음성 파일을 텍스트로 변환
print(f"Transcribing audio file: {local_audio_file}")
result = model.transcribe(local_audio_file)

# 변환된 텍스트 출력
print("Transcription result:")
print(result["text"])

# 분할된 텍스트와 타임스탬프 저장
segments = result['segments']

# 분할된 음성 파일을 저장할 폴더 (수정된 경로)
output_dir = os.path.expanduser("/home/ubuntu/instance1/data_pipeline/split_audio/")
os.makedirs(output_dir, exist_ok=True)

# 텍스트 파일 경로 설정
transcription_file_path = os.path.join(output_dir, f"{file_name.split('.')[0]}_transcription.txt")

# 텍스트 파일에 저장 시작
print(f"Saving transcription and audio segments to {output_dir}...")
with open(transcription_file_path, 'w', encoding='utf-8') as f:
    for i, segment in enumerate(segments):
        start = segment['start'] * 1000  # milliseconds 단위로 변환
        end = segment['end'] * 1000      # milliseconds 단위로 변환
        audio_segment = audio[start:end]

        # 분할된 음성 파일 저장
        output_path = os.path.join(output_dir, f"{file_name.split('.')[0]}_output_{i}.wav")
        audio_segment.export(output_path, format="wav")

        # 세그먼트 텍스트와 시간 정보 파일에 저장
        f.write(f"Segment {i} [{segment['start']} - {segment['end']}]: {segment['text']}\n")

        # 로그: 세그먼트 저장 정보
        print(f"Segment {i}: {segment['text']} saved at {output_path}")

print(f"Transcription saved at {transcription_file_path}")
