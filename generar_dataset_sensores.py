import csv
import random
from datetime import datetime, timedelta

random.seed(42)

TOTAL_REGISTROS = 100000
fecha_inicio = datetime(2026, 10, 1, 0, 0, 0)

with open("sensor_historical.csv", "w", newline="", encoding="utf-8") as archivo:
    writer = csv.writer(archivo)

    writer.writerow([
        "sensor_id",
        "temperature",
        "humidity",
        "timestamp"
    ])

    for i in range(1, TOTAL_REGISTROS + 1):
        sensor_id = random.randint(1, 10)
        temperature = round(random.uniform(18, 32), 2)
        humidity = round(random.uniform(35, 65), 2)

        if i % 4000 == 0:
            temperature = round(random.uniform(33, 37), 2)

        if i % 5500 == 0:
            humidity = round(random.uniform(75, 85), 2)

        if i % 3333 == 0:
            humidity = ""

        timestamp = (
            fecha_inicio + timedelta(seconds=i * 10)
        ).strftime("%Y-%m-%d %H:%M:%S")

        registro = [
            sensor_id,
            temperature,
            humidity,
            timestamp
        ]

        writer.writerow(registro)

        if i % 5000 == 0:
            writer.writerow(registro)

print("Dataset generado correctamente.")
print("Archivo: sensor_historical.csv")
print("Registros base:", TOTAL_REGISTROS)
print("Duplicados agregados:", TOTAL_REGISTROS // 5000)
print("Filas totales esperadas:", TOTAL_REGISTROS + (TOTAL_REGISTROS // 5000))
