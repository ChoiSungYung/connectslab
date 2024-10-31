'''
connects-split S3 Bucket에 적재된 split된 음성 파일을 매칭하여 Whisper로 돌리고 

결과물(S3에 split된 음성 파일 경로와 그에 매칭되는 텍스트)을 Supabase에 적재하는 파이썬 파일 

파이썬 파일을 실행하기 위해 임시적으로 pyenv 가상 환경 (data_pipline_env)를 활용하고 있다.
궁극적으로 이 파이썬 파일을 도커 컨테이너로 옮겨서 컨테이너 상에서 실행할 것이다. 
'''