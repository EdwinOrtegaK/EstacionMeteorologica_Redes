# consumer_3bytes.py
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from datetime import datetime

from codec_payload import decode_measurement

BOOTSTRAP_SERVERS = ["iot.redesuvg.cloud:9092"]
TOPIC = "22305"

def crear_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="cons-3bytes-live-3",
        auto_offset_reset="latest",      # solo mensajes nuevos
        enable_auto_commit=False,
        value_deserializer=lambda v: v,  # dejamos los bytes tal cual
    )
    return consumer


def main():
    consumer = crear_consumer()
    print(f"[Consumer-3B] Conectado a Kafka en {BOOTSTRAP_SERVERS}, escuchando topic '{TOPIC}'...")

    tiempos = []
    temperaturas = []
    humedades = []

    plt.ion()
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)
    fig.suptitle("Datos meteorológicos en tiempo real (payload 3 bytes)")

    try:
        for mensaje in consumer:
            raw_bytes = mensaje.value          # b'\x10\x91\x1e'
            print(f"[Consumer-3B] Raw bytes={raw_bytes.hex()}")

            data = decode_measurement(raw_bytes)   # le pasamos bytes
            print(f"[Consumer-3B] Decodificado -> {data}")

            ts = datetime.now()
            temp = data["temperatura"]
            hum = data["humedad"]

            tiempos.append(ts)
            temperaturas.append(temp)
            humedades.append(hum)

            if len(tiempos) > 100:
                tiempos = tiempos[-100:]
                temperaturas = temperaturas[-100:]
                humedades = humedades[-100:]

            ax1.clear()
            ax2.clear()

            ax1.plot(tiempos, temperaturas, color="orange", marker="o")
            ax1.set_ylabel("Temperatura (°C)")

            ax2.plot(tiempos, humedades, color="cyan", marker="o")
            ax2.set_ylabel("Humedad (%)")
            ax2.set_xlabel("Tiempo")

            plt.tight_layout()
            plt.pause(0.1)

    except KeyboardInterrupt:
        print("\n[Consumer-3B] Detenido por el usuario. Cerrando...")
    finally:
        consumer.close()
        plt.ioff()
        plt.show()


if __name__ == "__main__":
    main()
