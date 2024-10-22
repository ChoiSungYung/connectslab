FROM apache/kafka:3.8.0

# 필요한 패키지 설치
USER root
RUN apk update && apk add --no-cache unzip curl

# 플러그인 디렉토리 생성
RUN mkdir -p /opt/kafka/plugins

# Confluent S3 Source Connector 다운로드 및 설치
RUN cd /opt/kafka/plugins && \
    curl -L -o confluentinc-kafka-connect-s3-source.zip "https://d2p6pa21dvn84.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3-source/versions/2.6.6/confluentinc-kafka-connect-s3-source-2.6.6.zip" && \
    unzip confluentinc-kafka-connect-s3-source.zip && \
    rm confluentinc-kafka-connect-s3-source.zip

# Kafka Connect REST 포트 노출
EXPOSE 8083

# 환경 변수 설정
ENV CONNECT_PLUGIN_PATH="/opt/kafka/plugins"

# Kafka Connect 구성 파일 복사
COPY connect-distributed.properties /opt/kafka/config/

# 작업 디렉토리 설정
WORKDIR /opt/kafka

# Kafka Connect 시작
CMD ["bin/connect-distributed.sh", "config/connect-distributed.properties"]

