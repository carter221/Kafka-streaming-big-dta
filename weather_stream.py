import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, when,
    struct, to_json
)

KAFKA_BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "weather_stream"
TARGET_TOPIC = "weather_transformed"

# Nouveau schéma correspondant au payload du producteur kafka-producer-api.py
schema_json = """
latitude DOUBLE,
longitude DOUBLE,
timestamp STRING,
timestamp_epoch LONG,
weather STRUCT<
  temperature DOUBLE,
  windspeed DOUBLE,
  winddirection DOUBLE,
  weathercode INT,
  is_day INT,
  time STRING
>,
location_info STRUCT<
  timezone STRING,
  timezone_abbreviation STRING,
  elevation DOUBLE
>
"""

spark = (
    SparkSession.builder
    .appName("WeatherTransform")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.2"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Lecture du flux Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# value (bytes) -> string -> parsing JSON
parsed = raw_df.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), schema_json).alias("data")) \
    .select("data.*")

# Aplatissement des champs imbriqués weather.*
flattened = (
    parsed
    .withColumn("temperature", col("weather.temperature"))
    .withColumn("windspeed", col("weather.windspeed"))
    .withColumn("winddirection", col("weather.winddirection"))
    .withColumn("weathercode", col("weather.weathercode"))
    .withColumn("is_day", col("weather.is_day"))
    .withColumn("timezone", col("location_info.timezone"))
    .withColumn("timezone_abbreviation", col("location_info.timezone_abbreviation"))
    .withColumn("elevation", col("location_info.elevation"))
)

# event_time (priorité au champ timestamp ISO, fallback ingestion)
with_time = flattened.withColumn(
    "event_time",
    when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
    .otherwise(current_timestamp())
)

# Alertes vent
with_wind_alert = with_time.withColumn(
    "wind_alert_level",
    when(col("windspeed") > 20, "level_2")
    .when((col("windspeed") >= 10) & (col("windspeed") <= 20), "level_1")
    .otherwise("level_0")
)

# Alertes chaleur
final_df = with_wind_alert.withColumn(
    "heat_alert_level",
    when(col("temperature") > 35, "level_2")
    .when((col("temperature") >= 25) & (col("temperature") <= 35), "level_1")
    .otherwise("level_0")
)

# Construction JSON de sortie (ajoute quelques métadonnées utiles)
out_df = final_df.select(
    to_json(
        struct(
            "event_time",
            "timestamp",          # original API time
            "timestamp_epoch",    # ingestion epoch
            "latitude",
            "longitude",
            "temperature",
            "windspeed",
            "winddirection",
            "weathercode",
            "is_day",
            "wind_alert_level",
            "heat_alert_level",
            "timezone",
            "timezone_abbreviation",
            "elevation"
        )
    ).alias("value")
)

query = (
    out_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", TARGET_TOPIC)
    .option("checkpointLocation", "./chk_weather_transform")
    .outputMode("append")
    .start()
)

query.awaitTermination()
