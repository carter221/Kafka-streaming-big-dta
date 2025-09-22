import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window,
    avg, min as spark_min, max as spark_max, sum as spark_sum,
    when, lit, coalesce, current_timestamp,
    to_timestamp
)

KAFKA_BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "weather_transformed"

schema_json = """
event_time STRING,
timestamp STRING,
timestamp_epoch LONG,
latitude DOUBLE,
longitude DOUBLE,
temperature DOUBLE,
windspeed DOUBLE,
winddirection DOUBLE,
weathercode INT,
is_day INT,
wind_alert_level STRING,
heat_alert_level STRING,
timezone STRING,
timezone_abbreviation STRING,
elevation DOUBLE
"""

def build_spark():
    return (SparkSession.builder
            .appName("WeatherAggregatesConsole")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
                    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

def parse_event_time(df):
    # Patterns Java (Spark) avec 'T' littéral entre quotes
    patterns = [
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        "yyyy-MM-dd'T'HH:mm:ssXXX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXX",
        "yyyy-MM-dd'T'HH:mm:ssXX",
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
        "yyyy-MM-dd'T'HH:mm:ss'Z'",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm"
    ]
    # Empile les tentatives: premier non null retenu
    ts_col = None
    for p in patterns:
        cand = to_timestamp(col("event_time"), p)
        ts_col = cand if ts_col is None else coalesce(ts_col, cand)

    # Fallback epoch si fourni
    epoch_ts = when(col("timestamp_epoch").isNotNull(),
                    when(col("timestamp_epoch") > 10_000_000_000,
                         (col("timestamp_epoch")/1000).cast("timestamp"))
                    .otherwise(col("timestamp_epoch").cast("timestamp"))
                    ).otherwise(lit(None))

    ts_final = coalesce(ts_col, epoch_ts, current_timestamp())

    return df.withColumn("event_time_ts", ts_final)

def add_location(df):
    return (df
            .withColumn(
                "city",
                when(
                    (col("latitude").between(37.00, 37.15)) &
                    (col("longitude").between(-8.75, -8.60)),
                    lit("Lagos")
                ).otherwise(lit("UNKNOWN"))
            )
            .withColumn(
                "country",
                when(col("city") == "Lagos", lit("Portugal")).otherwise(lit("UNKNOWN"))
            ))

def build_aggregates(base, window_size):
    win = window(col("event_time_ts"), f"{window_size} minutes", "1 minute")

    common_aggs = [
        avg("temperature").alias("temp_avg"),
        spark_min("temperature").alias("temp_min"),
        spark_max("temperature").alias("temp_max"),
        spark_sum(when(col("wind_alert_level").isin("level_1", "level_2"), 1).otherwise(0)).alias("wind_alerts_high"),
        spark_sum(when(col("heat_alert_level").isin("level_1", "level_2"), 1).otherwise(0)).alias("heat_alerts_high")
    ]

    agg_global = (base.groupBy(win)
                  .agg(*common_aggs)
                  .select(
                      lit("global").alias("scope"),
                      col("window.start").alias("window_start"),
                      col("window.end").alias("window_end"),
                      lit(None).alias("city"),
                      lit(None).alias("country"),
                      "temp_avg", "temp_min", "temp_max",
                      "wind_alerts_high", "heat_alerts_high"
                  ))

    agg_city = (base.groupBy(win, col("city"))
                .agg(*common_aggs)
                .select(
                    lit("city").alias("scope"),
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    "city",
                    lit(None).alias("country"),
                    "temp_avg", "temp_min", "temp_max",
                    "wind_alerts_high", "heat_alerts_high"
                ))

    agg_country = (base.groupBy(win, col("country"))
                   .agg(*common_aggs)
                   .select(
                       lit("country").alias("scope"),
                       col("window.start").alias("window_start"),
                       col("window.end").alias("window_end"),
                       lit(None).alias("city"),
                       "country",
                       "temp_avg", "temp_min", "temp_max",
                       "wind_alerts_high", "heat_alerts_high"
                   ))

    return agg_global.unionByName(agg_city).unionByName(agg_country)

def main(window_size, from_earliest, fast_test):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    offset_mode = "earliest" if from_earliest else "latest"

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", SOURCE_TOPIC)
           .option("startingOffsets", offset_mode)
           .load())

    parsed = (raw
              .selectExpr("CAST(value AS STRING) AS json_str")
              .select(from_json(col("json_str"), schema_json).alias("d"))
              .select("d.*"))

    events = parse_event_time(parsed)
    events_loc = add_location(events)
    base = events_loc.withWatermark("event_time_ts", "10 minutes")

    if fast_test:
        window_size = 1
        union_df = build_aggregates(base, window_size)
        trigger_conf = "30 seconds"
        checkpoint = "./chk_weather_agg_fast"
    else:
        union_df = build_aggregates(base, window_size)
        trigger_conf = "1 minute"
        checkpoint = f"./chk_weather_agg_{window_size}m"

    query = (union_df.writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .option("numRows", 200)
             .option("checkpointLocation", checkpoint)
             .trigger(processingTime=trigger_conf)
             .start())

    print("\n=== Streaming démarré ===")
    print(f"Offsets: {offset_mode}")
    print(f"Fenêtre: {window_size} min (slide 1 min)" if not fast_test
          else "Mode test: fenêtre 1 min slide 1 min, déclenchement 30s")
    print("Ctrl+C pour arrêter.\n")
    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agrégats météo (console)")
    parser.add_argument("--window-size", type=int, default=5, choices=[1, 5],
                        help="Taille fenêtre minutes (slide 1 min)")
    parser.add_argument("--from-earliest", action="store_true", help="Lire l’historique (earliest)")
    parser.add_argument("--fast-test", action="store_true", help="Fenêtre 1 min + trigger 30s")
    args = parser.parse_args()
    main(args.window_size, args.from_earliest, args.fast_test)