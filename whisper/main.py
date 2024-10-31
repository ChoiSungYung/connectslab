import whisper
from pydub import AudioSegment
import os

# Whisper 모델 로드 (base, small, medium, large 옵션 가능)
model = whisper.load_model("base")

# 음성 파일 경로
audio_dir = os.path.expanduser("/app/audio/")
audio_file = os.path.join(audio_dir, "2.wav")  # 파일명을 적절히 변경해주세요.

# 음성 파일 로드
audio = AudioSegment.from_file(audio_file)

# Whisper로 음성 파일을 텍스트로 변환
result = model.transcribe(audio_file)

# 변환된 텍스트 출력
print(result["text"])

# 분할된 텍스트를 timestamps에 따라 저장하기
segments = result['segments']

# 분할된 음성 파일을 저장할 폴더
output_dir = os.path.expanduser("/app/split_audio")
os.makedirs(output_dir, exist_ok=True)

# 분할된 텍스트 파일을 저장할 폴더
output_text_dir = os.path.expanduser("/app/split_text")
os.makedirs(output_text_dir, exist_ok=True)

# 텍스트 파일 경로 설정
transcription_file_path = os.path.join(output_text_dir, "split_text.txt")

# 텍스트 파일에 저장 시작
with open(transcription_file_path, 'w', encoding='utf-8') as f:
    for i, segment in enumerate(segments):
        start = segment['start'] * 1000  # milliseconds 단위로 변환
        end = segment['end'] * 1000      # milliseconds 단위로 변환
        audio_segment = audio[start:end]

        # 분할된 음설파일 저장
        output_path = os.path.join(output_dir, f"output_{i}.wav")
        audio_segment.export(output_path, format="wav")

        # 세그먼트 텍스트와 시간 정보 파일에 저장
        f.write(f"Segment {i} [{segment['start']} - {segment['end']}]: {segment['text']}\n")

        print(f"Segment {i}: {segment['text']} saved at {output_path}")

# 텍스트 파일 저장 완료 메시지
print(f"Transcription saved to {transcription_file_path}")