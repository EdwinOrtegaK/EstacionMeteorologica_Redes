# producer_3bytes.py
import time
from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
    SENSOR_ID,
    PUBLISH_INTERVAL_SECONDS,
)
from sensor import generar_medicion
from codec_payload import encode_measurement


def crear_producer_bytes():
    # value_serializer = identidad, ya mandaremos bytes
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v,
        key_serializer=lambda k: k.encode("utf-8"),
    )
    return producer


def main():
    producer = crear_producer_bytes()
    print(
        f"[Producer-3B] Enviando datos (payload de 3 bytes) a "
        f"{KAFKA_BOOTSTRAP_SERVERS}, topic='{TOPIC_NAME}'"
    )

    try:
        while True:
            data = generar_medicion()
            payload = encode_measurement(data)

            print(f"[Producer-3B] Original: {data} -> bytes={payload.hex()}")

            future = producer.send(
                TOPIC_NAME,
                key=SENSOR_ID,
                value=payload,
            )

            _ = future.get(timeout=10)
            time.sleep(PUBLISH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n[Producer-3B] Interrumpido por el usuario. Cerrando...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
