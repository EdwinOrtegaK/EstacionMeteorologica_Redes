# sensor.py
import random
import time
from datetime import datetime

DIRECCIONES_VIENTO = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]

def generar_medicion():
    """
    Genera una medición simulada de la estación meteorológica:
    - temperatura: float [0, 110] con 2 decimales
    - humedad: entero [0, 100]
    - direccion_viento: uno de {N, NO, O, SO, S, SE, E, NE}
    """

    # Temperatura: media 25°C, desviación estándar 10°C
    temp = random.gauss(25, 10)
    temp = max(0, min(110, temp))      # recortar a [0, 110]
    temp = round(temp, 2)              # 2 decimales

    # Humedad: media 60%, desviación estándar 20%
    hum = random.gauss(60, 20)
    hum = int(round(hum))
    hum = max(0, min(100, hum))        # recortar a [0, 100]

    # Dirección del viento: simple uniforme
    dir_viento = random.choice(DIRECCIONES_VIENTO)

    # Timestamp para graficar luego
    ts = datetime.utcnow().isoformat() + "Z"

    return {
        "timestamp": ts,
        "temperatura": temp,
        "humedad": hum,
        "direccion_viento": dir_viento,
    }

if __name__ == "__main__":
    # Prueba rápida en consola
    while True:
        m = generar_medicion()
        print(m)
        time.sleep(2)
