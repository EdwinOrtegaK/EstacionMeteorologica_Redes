from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers=["iot.redesuvg.cloud:9092"],
    security_protocol="PLAINTEXT"
)

print("Topics disponibles en el cluster:")
print(consumer.topics())
