import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, when, lit, coalesce, to_json, struct, concat_ws,
    to_timestamp
)

KAFKA_BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "weather_stream"          # Topic source (producteur)
TARGET_TOPIC = "weather_transformed"     # Topic cible (après enrichissement)
CHECKPOINT = "./chk_weather_transform"

RAW_SCHEMA = """
city STRING,
country STRING,
latitude DOUBLE,
longitude DOUBLE,
timestamp STRING,
timestamp_epoch LONG,
weather STRUCT<
  temperature: DOUBLE,
  windspeed: DOUBLE,
  winddirection: DOUBLE,
  weathercode: INT,
  is_day: INT,
  time: STRING
>,
location_info STRUCT<
  timezone: STRING,
  timezone_abbreviation: STRING,
  elevation: DOUBLE,
  city: STRING,
  country: STRING
>
"""

def build_spark():
    return (SparkSession.builder
            .appName("WeatherTransform")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
                    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

def main(from_earliest: bool):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    offset_mode = "earliest" if from_earliest else "latest"

    # Lecture Kafka (key binaire ignorée)
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", SOURCE_TOPIC)
           .option("startingOffsets", offset_mode)
           .load())

    parsed = (raw
              .selectExpr("CAST(value AS STRING) AS json_str")
              .select(from_json(col("json_str"), RAW_SCHEMA).alias("d"))
              .select("d.*"))

    # Flatten + fallback ville/pays
    flat = (parsed
            .withColumn("city", coalesce(col("city"), col("location_info.city")))
            .withColumn("country", coalesce(col("country"), col("location_info.country")))
            .withColumn("temperature", col("weather.temperature").cast("double"))
            .withColumn("windspeed", col("weather.windspeed").cast("double"))
            .withColumn("winddirection", col("weather.winddirection").cast("double"))
            .withColumn("weathercode", col("weather.weathercode").cast("int"))
            .withColumn("is_day", col("weather.is_day").cast("int"))
            .withColumn("event_time", coalesce(col("weather.time"), col("timestamp")))
            .withColumn("timezone", col("location_info.timezone"))
            .withColumn("timezone_abbreviation", col("location_info.timezone_abbreviation"))
            .withColumn("elevation", col("location_info.elevation").cast("double"))
            .drop("weather", "location_info")
            )

    # Normalisation
    norm = (flat
            .withColumn("city", when(col("city").isNull() | (col("city") == ""), lit("UNKNOWN")).otherwise(col("city")))
            .withColumn("country", when(col("country").isNull() | (col("country") == ""), lit("UNKNOWN")).otherwise(col("country")))
            )

    # Règles alertes
    alerts = (norm
              .withColumn("wind_alert_level",
                          when(col("windspeed") >= 60, lit("level_2"))
                          .when(col("windspeed") >= 40, lit("level_1"))
                          .otherwise(lit("normal")))
              .withColumn("heat_alert_level",
                          when(col("temperature") >= 38, lit("level_2"))
                          .when(col("temperature") >= 32, lit("level_1"))
                          .otherwise(lit("normal")))
              )

    # (Optionnel) conversion timestamp interne (non envoyé)
    enriched = alerts.withColumn(
        "_event_time_ts",
        coalesce(
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm")
        )
    )

    final_order = [
        "event_time",
        "timestamp",
        "timestamp_epoch",
        "latitude", "longitude",
        "temperature", "windspeed", "winddirection", "weathercode", "is_day",
        "wind_alert_level", "heat_alert_level",
        "timezone", "timezone_abbreviation", "elevation",
        "city", "country"
    ]

    # Construction value JSON + clé string
    out_df = (enriched
              .select(*final_order)
              .select(
                  concat_ws("|",
                            coalesce(col("city"), lit("UNKNOWN")),
                            coalesce(col("country"), lit("UNKNOWN"))
                            ).cast("string").alias("key"),
                  to_json(struct(*[col(c) for c in final_order])).alias("value")
              ))

    # DEBUG (une fois) : schéma final
    out_df.printSchema()

    query = (out_df.writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("topic", TARGET_TOPIC)
             .option("checkpointLocation", CHECKPOINT)
             .outputMode("append")
             .start())

    print(f"== Transform démarré offsets={offset_mode} -> {TARGET_TOPIC} ==")
    query.awaitTermination()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Transformation weather_stream -> weather_transformed")
    p.add_argument("--from-earliest", action="store_true")
    args = p.parse_args()
    main(args.from_earliest)