# producer.py
import json
import time

from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME,
    SENSOR_ID,
    PUBLISH_INTERVAL_SECONDS,
)
from sensor import generar_medicion


def crear_producer():
    # value_serializer: convierte dict -> JSON -> bytes
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    return producer


def main():
    producer = crear_producer()
    print(f"[Producer] Enviando datos a {KAFKA_BOOTSTRAP_SERVERS}, topic='{TOPIC_NAME}'")
    try:
        while True:
            data = generar_medicion()
            print(f"[Producer] Enviando: {data}")

            future = producer.send(
                TOPIC_NAME,
                key=SENSOR_ID,
                value=data,
            )

            # Esperar confirmación del broker (debug útil)
            record_metadata = future.get(timeout=10)
            # Impresion por si acaso (confirmación)
            # print(f"[OK] offset={record_metadata.offset} partition={record_metadata.partition}")

            time.sleep(PUBLISH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n[Producer] Interrumpido por el usuario. Cerrando...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
