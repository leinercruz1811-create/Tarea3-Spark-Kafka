# Tarea 3 - Procesamiento de Datos con Apache Spark

Proyecto académico del curso **Big Data** de la Universidad Nacional Abierta y a Distancia - UNAD.

## Descripción

El proyecto implementa dos tipos de procesamiento de datos:

1. **Procesamiento Batch con Apache Spark**
   - Generación de un conjunto histórico de datos de sensores.
   - Carga del archivo CSV.
   - Limpieza de duplicados y valores nulos.
   - Transformación de tipos.
   - Análisis exploratorio de datos (EDA).
   - Estadísticas generales y por sensor.
   - Detección de alertas.
   - Generación de gráficas.

2. **Procesamiento en tiempo real con Apache Kafka y Spark Streaming**
   - Creación del topic `sensor_data`.
   - Generación continua de datos mediante un Producer de Kafka.
   - Consumo de los eventos mediante Spark Streaming.
   - Cálculo de temperatura y humedad promedio por sensor en ventanas de un minuto.

## Estructura del repositorio

```text
Tarea3-Spark-Kafka/
├── README.md
├── generar_dataset_sensores.py
├── analisis_batch.py
├── kafka_producer.py
├── spark_streaming_consumer.py
├── requirements.txt
└── .gitignore
```

## Requisitos

- Ubuntu
- Python 3
- Apache Spark 3.5.8
- Apache Kafka 3.9.2
- Java 11
- Paquetes de Python:
  - kafka-python
  - matplotlib

## 1. Generar el conjunto de datos histórico

Ejecutar:

```bash
python3 generar_dataset_sensores.py
```

Se generará:

```text
sensor_historical.csv
```

El archivo contiene aproximadamente 100.000 registros con las variables:

- `sensor_id`
- `temperature`
- `humidity`
- `timestamp`

También se agregan intencionalmente algunos registros duplicados y valores nulos para evidenciar el proceso de limpieza.

## 2. Ejecutar el procesamiento Batch

Instalar Matplotlib si es necesario:

```bash
pip install matplotlib
```

Ejecutar:

```bash
/opt/spark/bin/spark-submit analisis_batch.py
```

El programa realiza:

- Carga del archivo CSV.
- Revisión del esquema.
- Conteo de valores nulos.
- Eliminación de duplicados.
- Eliminación de valores nulos.
- Conversión de tipos.
- Creación de alertas.
- Estadísticas generales.
- Análisis por sensor.
- Exportación del resumen.
- Generación de gráficas.

Archivos generados:

```text
temperatura_promedio_sensor.png
alertas_por_sensor.png
resultado_resumen_sensores/
```

## 3. Configurar Kafka

El topic utilizado por el proyecto es:

```text
sensor_data
```

Crear el topic:

```bash
/opt/Kafka/bin/kafka-topics.sh --create \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1 \
--topic sensor_data
```

Verificar:

```bash
/opt/Kafka/bin/kafka-topics.sh --list \
--bootstrap-server localhost:9092
```

## 4. Ejecutar el Producer

Instalar la librería:

```bash
pip install kafka-python
```

Ejecutar:

```bash
python3 kafka_producer.py
```

El Producer genera continuamente datos simulados con:

- identificador del sensor;
- temperatura;
- humedad;
- timestamp.

## 5. Ejecutar Spark Streaming

En una segunda terminal ejecutar:

```bash
/opt/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
spark_streaming_consumer.py
```

El Consumer:

- se conecta a Kafka;
- consume el topic `sensor_data`;
- interpreta los mensajes JSON;
- agrupa los datos por sensor;
- utiliza ventanas de un minuto;
- calcula temperatura y humedad promedio.

## 6. Spark UI

Mientras Spark Streaming esté en ejecución se puede consultar:

```text
http://IP-DE-LA-MAQUINA:4040
```

Si el puerto 4040 está ocupado, Spark puede utilizar 4041 u otro puerto consecutivo.

En la interfaz se pueden revisar:

- Jobs
- Stages
- Environment
- Executors

## Autor

Estudiante del curso Big Data - UNAD.
