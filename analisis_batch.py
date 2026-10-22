from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    avg,
    min as spark_min,
    max as spark_max,
    count,
    when,
    sum as spark_sum,
    round as spark_round
)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("AnalisisSensoresBatch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("sensor_historical.csv")

print("\n==========================================")
print("ESQUEMA ORIGINAL")
print("==========================================")
df.printSchema()

print("\n==========================================")
print("PRIMEROS REGISTROS")
print("==========================================")
df.show(10, truncate=False)

registros_originales = df.count()
print("Cantidad inicial de registros:", registros_originales)

print("\n==========================================")
print("VALORES NULOS")
print("==========================================")

df.select([
    spark_sum(
        when(col(c).isNull(), 1).otherwise(0)
    ).alias(c)
    for c in df.columns
]).show()

df_sin_duplicados = df.dropDuplicates()

print(
    "Registros despues de eliminar duplicados:",
    df_sin_duplicados.count()
)

df_clean = df_sin_duplicados.dropna(
    subset=[
        "sensor_id",
        "temperature",
        "humidity",
        "timestamp"
    ]
)

df_clean = df_clean \
    .withColumn(
        "sensor_id",
        col("sensor_id").cast("integer")
    ) \
    .withColumn(
        "temperature",
        col("temperature").cast("double")
    ) \
    .withColumn(
        "humidity",
        col("humidity").cast("double")
    ) \
    .withColumn(
        "timestamp",
        to_timestamp(
            col("timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

print("\n==========================================")
print("ESQUEMA DESPUES DE LA TRANSFORMACION")
print("==========================================")
df_clean.printSchema()

registros_limpios = df_clean.count()
print("Cantidad final de registros limpios:", registros_limpios)

df_clean = df_clean.withColumn(
    "alerta",
    when(
        (col("temperature") > 30) |
        (col("temperature") < 18) |
        (col("humidity") > 70) |
        (col("humidity") < 30),
        1
    ).otherwise(0)
)

print("\n==========================================")
print("ESTADISTICAS GENERALES")
print("==========================================")

estadisticas = df_clean.agg(
    count("*").alias("REGISTROS"),
    spark_round(
        avg("temperature"),
        2
    ).alias("TEMPERATURA_PROMEDIO"),
    spark_round(
        avg("humidity"),
        2
    ).alias("HUMEDAD_PROMEDIO"),
    spark_min(
        "temperature"
    ).alias("TEMPERATURA_MINIMA"),
    spark_max(
        "temperature"
    ).alias("TEMPERATURA_MAXIMA"),
    spark_min(
        "humidity"
    ).alias("HUMEDAD_MINIMA"),
    spark_max(
        "humidity"
    ).alias("HUMEDAD_MAXIMA"),
    spark_sum(
        "alerta"
    ).alias("TOTAL_ALERTAS")
)

estadisticas.show(truncate=False)

print("\n==========================================")
print("ANALISIS POR SENSOR")
print("==========================================")

resumen_sensor = df_clean.groupBy(
    "sensor_id"
).agg(
    count("*").alias("REGISTROS"),
    spark_round(
        avg("temperature"),
        2
    ).alias("TEMPERATURA_PROMEDIO"),
    spark_round(
        avg("humidity"),
        2
    ).alias("HUMEDAD_PROMEDIO"),
    spark_min(
        "temperature"
    ).alias("TEMPERATURA_MINIMA"),
    spark_max(
        "temperature"
    ).alias("TEMPERATURA_MAXIMA"),
    spark_sum(
        "alerta"
    ).alias("ALERTAS")
).orderBy(
    "sensor_id"
)

resumen_sensor.show(
    20,
    truncate=False
)

print("\n==========================================")
print("EJEMPLOS DE REGISTROS CON ALERTA")
print("==========================================")

df_clean.filter(
    col("alerta") == 1
).select(
    "sensor_id",
    "temperature",
    "humidity",
    "timestamp"
).show(
    20,
    truncate=False
)

resumen_sensor.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("resultado_resumen_sensores")

filas = resumen_sensor.collect()

sensores = [
    str(fila["sensor_id"])
    for fila in filas
]

temp_prom = [
    float(fila["TEMPERATURA_PROMEDIO"])
    for fila in filas
]

alertas = [
    int(fila["ALERTAS"])
    for fila in filas
]

plt.figure(figsize=(10, 6))
plt.bar(
    sensores,
    temp_prom
)
plt.title(
    "Temperatura promedio por sensor"
)
plt.xlabel(
    "Sensor"
)
plt.ylabel(
    "Temperatura promedio"
)
plt.tight_layout()
plt.savefig(
    "temperatura_promedio_sensor.png"
)
plt.close()

plt.figure(figsize=(10, 6))
plt.bar(
    sensores,
    alertas
)
plt.title(
    "Cantidad de alertas por sensor"
)
plt.xlabel(
    "Sensor"
)
plt.ylabel(
    "Cantidad de alertas"
)
plt.tight_layout()
plt.savefig(
    "alertas_por_sensor.png"
)
plt.close()

print("\nGraficas generadas:")
print("temperatura_promedio_sensor.png")
print("alertas_por_sensor.png")

spark.stop()
