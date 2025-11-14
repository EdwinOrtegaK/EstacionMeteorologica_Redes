# consumer.py 
import json
import time
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from datetime import datetime

# Configuración del Consumer
BOOTSTRAP_SERVERS = ["iot.redesuvg.cloud:9092"]
TOPIC = "22305"

def crear_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000
    )
    return consumer


def main():
    consumer = crear_consumer()
    print(f"[Consumer] Conectado a Kafka en {BOOTSTRAP_SERVERS}, escuchando topic '{TOPIC}'...")

    # Datos para graficar
    tiempos = []
    temperaturas = []
    humedades = []

    plt.ion()
    fig, (ax1, ax2) = plt.subplots(2, 1)
    fig.suptitle("Datos meteorológicos en tiempo real")

    try:
        for mensaje in consumer:
            data = mensaje.value
            print("[Consumer] Recibido:", data)

            # Extraer datos
            ts = datetime.fromisoformat(data["timestamp"].replace("Z", ""))
            temp = data["temperatura"]
            hum = data["humedad"]

            tiempos.append(ts)
            temperaturas.append(temp)
            humedades.append(hum)

            # Limitar tamaño para evitar sobrecarga
            if len(tiempos) > 100:
                tiempos = tiempos[-100:]
                temperaturas = temperaturas[-100:]
                humedades = humedades[-100:]

            # Actualizar gráficas
            ax1.clear()
            ax2.clear()

            ax1.plot(tiempos, temperaturas, color='orange')
            ax1.set_title("Temperatura (°C)")

            ax2.plot(tiempos, humedades, color='cyan')
            ax2.set_title("Humedad (%)")

            plt.pause(0.1)

    except KeyboardInterrupt:
        print("\n[Consumer] Detenido por el usuario. Cerrando...")

    finally:
        consumer.close()
        plt.ioff()
        plt.show()


if __name__ == "__main__":
    main()
