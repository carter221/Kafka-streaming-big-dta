import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, when, struct, to_json

KAFKA_BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "weather_stream"
TARGET_TOPIC = "weather_transformed"

# Schéma JSON attendu depuis ton producteur
schema_json = """
latitude DOUBLE,
longitude DOUBLE,
timestamp STRING,
temperature DOUBLE,
windspeed DOUBLE,
winddirection DOUBLE,
raw STRUCT<
  time: STRING,
  temperature: DOUBLE,
  windspeed: DOUBLE,
  winddirection: DOUBLE
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

# Lecture flux Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "latest")   # changer en earliest si besoin
    .load()
)

# value est binaire -> string -> JSON
parsed = raw_df.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), schema_json).alias("data")) \
    .select("data.*")

# event_time à partir du champ timestamp (sinon fallback now)
with_time = parsed.withColumn(
    "event_time",
    when(col("timestamp").isNotNull(), to_timestamp(col("timestamp"))).otherwise(current_timestamp())
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

# Construction JSON de sortie
out_df = final_df.select(
    to_json(
        struct(
            "event_time",
            "latitude",
            "longitude",
            "temperature",
            "windspeed",
            "winddirection",
            "wind_alert_level",
            "heat_alert_level"
        )
    ).alias("value")
)

# Écriture vers le topic cible
query = (
    out_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", TARGET_TOPIC)
    .option("checkpointLocation", "./chk_weather_transform")  # dossier local checkpoint
    .outputMode("append")
    .start()
)

query.awaitTermination()