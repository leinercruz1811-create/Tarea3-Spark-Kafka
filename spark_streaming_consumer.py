from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    from_unixtime,
    to_timestamp
)

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    LongType
)


spark = SparkSession.builder \
    .appName("SensorDataStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField(
        "sensor_id",
        IntegerType(),
        True
    ),
    StructField(
        "temperature",
        DoubleType(),
        True
    ),
    StructField(
        "humidity",
        DoubleType(),
        True
    ),
    StructField(
        "timestamp",
        LongType(),
        True
    )
])


df = spark \
    .readStream \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    ) \
    .option(
        "subscribe",
        "sensor_data"
    ) \
    .option(
        "startingOffsets",
        "latest"
    ) \
    .load()


parsed_df = df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select(
    "data.*"
)


parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(
        from_unixtime(
            col("timestamp")
        )
    )
)


windowed_stats = parsed_df \
    .groupBy(
        window(
            col("event_time"),
            "1 minute"
        ),
        "sensor_id"
    ) \
    .agg(
        avg(
            "temperature"
        ).alias(
            "avg_temperature"
        ),
        avg(
            "humidity"
        ).alias(
            "avg_humidity"
        )
    )


query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option(
        "truncate",
        False
    ) \
    .start()


query.awaitTermination()
