# codec_payload.py
from typing import Dict

DIR_TO_CODE = {
    "N": 0,
    "NE": 1,
    "E": 2,
    "SE": 3,
    "S": 4,
    "SO": 5,
    "O": 6,
    "NO": 7,
}

CODE_TO_DIR = {v: k for k, v in DIR_TO_CODE.items()}


def encode_measurement(data: Dict) -> bytes:
    """
    data = {
        "temperatura": float (0-110),
        "humedad": int (0-100),
        "direccion_viento": str in DIR_TO_CODE
    }
    -> 3 bytes
    """
    temp = float(data["temperatura"])
    hum = int(data["humedad"])
    wind = str(data["direccion_viento"])

    # Clamp por seguridad
    if temp < 0:
        temp = 0.0
    if temp > 110.0:
        temp = 110.0

    if hum < 0:
        hum = 0
    if hum > 100:
        hum = 100

    if wind not in DIR_TO_CODE:
        # valor por defecto si algo raro viene
        wind = "N"

    temp_int = int(round(temp * 100))  # 2 decimales -> 0..11000
    hum_int = hum                      # 0..100
    dir_code = DIR_TO_CODE[wind]       # 0..7

    # Verificaciones rápidas
    assert 0 <= temp_int <= 11000
    assert 0 <= hum_int <= 100
    assert 0 <= dir_code <= 7

    # Empaquetar en 24 bits: [temp(14)][hum(7)][dir(3)]
    value = (temp_int << (7 + 3)) | (hum_int << 3) | dir_code

    # 3 bytes big-endian
    return value.to_bytes(3, "big")


def decode_measurement(payload: bytes) -> Dict:
    """
    3 bytes -> dict con temperatura, humedad, direccion_viento
    """
    if len(payload) != 3:
        raise ValueError(f"Payload inválido, longitud={len(payload)}")

    value = int.from_bytes(payload, "big")

    # Extraer 3 bits menos significativos: dirección
    dir_code = value & 0b111
    value >>= 3

    # Siguientes 7 bits: humedad
    hum_int = value & 0b1111111
    value >>= 7

    # Lo que queda (14 bits): temperatura * 100
    temp_int = value

    temp = temp_int / 100.0
    hum = hum_int
    wind = CODE_TO_DIR.get(dir_code, "N")

    return {
        "temperatura": temp,
        "humedad": hum,
        "direccion_viento": wind,
    }
