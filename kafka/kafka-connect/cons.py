from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test6',
    bootstrap_servers='172.31.28.191:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    key_deserializer=lambda k: k.decode('utf-8'),
    value_deserializer=lambda v: v  # 바이트 형태로 받음
)

for message in consumer:
    key = message.key
    value = message.value
    headers = message.headers

    print(f"Key: {key}")
    print(f"Headers: {headers}")
    print(f"Value (bytes): {value}")
    # 필요에 따라 value를 처리 (예: 파일 청크로 저장)
