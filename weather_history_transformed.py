import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, sum as ssum,
    row_number, current_timestamp, struct, lit
)
from pyspark.sql.window import Window
import traceback

# ------------------------------------------------------------------
# Config via variables d'environnement
# ------------------------------------------------------------------
RAW_PATH_ENV      = os.getenv("HIST_RAW_GLOB", "/hdfs-data/*/*/weather_history_raw/year=*/part*.json")
HDFS_NN           = os.getenv("HDFS_NN", "hdfs://namenode:9000")
RECORDS_HDFS_BASE = os.getenv("RECORDS_HDFS_PATH", "/hdfs-data")
RECORDS_TOPIC     = os.getenv("RECORDS_TOPIC", "weather_records")
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
OUTPUT_FORMAT     = os.getenv("RECORDS_OUTPUT_FORMAT", "json").lower()   # json | parquet
LOG_LEVEL         = os.getenv("SPARK_LOG_LEVEL", "WARN")
FAIL_EMPTY        = os.getenv("FAIL_IF_EMPTY", "0").lower() in ("1", "true", "yes")

# ------------------------------------------------------------------
def resolve_glob(path: str) -> str:
    """
    Si aucun schéma n'est présent, on préfixe avec le NameNode HDFS.
    """
    if "://" in path:
        return path
    return HDFS_NN.rstrip("/") + path

def build_spark():
    spark = (SparkSession.builder
             .appName("WeatherRecordsComputation")
             .config("spark.hadoop.fs.defaultFS", HDFS_NN)
             .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
             .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
             .getOrCreate())
    hc = spark._jsc.hadoopConfiguration()
    hc.set("fs.defaultFS", HDFS_NN)
    print("[DEBUG] fs.defaultFS effectif:", hc.get("fs.defaultFS"))
    return spark
# ------------------------------------------------------------------
def compute_records(df):
    # Normalisation
    df = (
        df.withColumn("event_ts", to_timestamp(col("event_time")))
           .withColumn("event_date", to_date(col("event_ts")))
           .filter(col("city").isNotNull() & col("country").isNotNull())
    )

    # Fenêtres
    w_temp_desc = Window.partitionBy("country", "city").orderBy(col("temperature_2m").desc(), col("event_ts").asc())
    w_temp_asc  = Window.partitionBy("country", "city").orderBy(col("temperature_2m").asc(),  col("event_ts").asc())
    w_wind_desc = Window.partitionBy("country", "city").orderBy(col("windspeed_10m").desc(), col("event_ts").asc())

    # Plus chaud
    hottest = (
        df.filter(col("temperature_2m").isNotNull())
          .withColumn("rn", row_number().over(w_temp_desc))
          .filter(col("rn") == 1)
          .select(
              "country", "city",
              col("event_date").alias("hottest_date"),
              col("event_ts").alias("hottest_time"),
              col("temperature_2m").alias("hottest_max_temp")
          )
    )

    # Plus froid
    coldest = (
        df.filter(col("temperature_2m").isNotNull())
          .withColumn("rn", row_number().over(w_temp_asc))
          .filter(col("rn") == 1)
          .select(
              "country", "city",
              col("event_date").alias("coldest_date"),
              col("event_ts").alias("coldest_time"),
              col("temperature_2m").alias("coldest_min_temp")
          )
    )

    # Vent le plus fort
    strongest_wind = (
        df.filter(col("windspeed_10m").isNotNull())
          .withColumn("rn", row_number().over(w_wind_desc))
          .filter(col("rn") == 1)
          .select(
              "country", "city",
              col("event_ts").alias("strongest_wind_time"),
              col("windspeed_10m").alias("strongest_windspeed_10m")
          )
    )

    # Jour le plus pluvieux (si précipitations présentes)
    if "precipitation" in df.columns:
        daily_rain = (
            df.filter(col("precipitation").isNotNull())
              .groupBy("country", "city", "event_date")
              .agg(ssum("precipitation").alias("day_precip_total"))
        )
        w_rain_desc = Window.partitionBy("country", "city").orderBy(col("day_precip_total").desc(), col("event_date").asc())
        rainiest = (
            daily_rain.withColumn("rn", row_number().over(w_rain_desc))
                      .filter(col("rn") == 1)
                      .select(
                          "country", "city",
                          col("event_date").alias("rainiest_date"),
                          col("day_precip_total").alias("rainiest_precip_total")
                      )
        )
    else:
        rainiest = (
            hottest.select("country", "city")
                   .withColumn("rainiest_date", lit(None).cast("date"))
                   .withColumn("rainiest_precip_total", lit(None).cast("double"))
        )

    # Jointure
    records = (
        hottest
        .join(coldest, ["country", "city"], "full")
        .join(strongest_wind, ["country", "city"], "full")
        .join(rainiest, ["country", "city"], "full")
        .withColumn("generated_at", current_timestamp())
    )

    # Projection finale
    out = (
        records.select(
            "country", "city",
            struct(
                col("hottest_date").alias("date"),
                col("hottest_time").alias("time"),
                col("hottest_max_temp").alias("max_temp")
            ).alias("hottest_day"),
            struct(
                col("coldest_date").alias("date"),
                col("coldest_time").alias("time"),
                col("coldest_min_temp").alias("min_temp")
            ).alias("coldest_day"),
            struct(
                col("strongest_wind_time").alias("time"),
                col("strongest_windspeed_10m").alias("windspeed_10m")
            ).alias("strongest_wind"),
            struct(
                col("rainiest_date").alias("date"),
                col("rainiest_precip_total").alias("total_precipitation")
            ).alias("rainiest_day"),
            col("generated_at")
        )
    )
    return out

# ------------------------------------------------------------------
def write_hdfs(df):
    base = os.getenv("WEATHER_RECORDS_PATH", "/hdfs-data/weather_records")
    # Force préfixe HDFS si absent
    if not base.startswith("hdfs://"):
        target = HDFS_NN.rstrip("/") + base
    else:
        target = base
    print("[DEBUG] Chemin sortie HDFS:", target)
    (df.write
       .mode("overwrite")
       .partitionBy("country", "city")
       .format("json")
       .save(target))

def write_kafka(spark, out_df):
    kafka_df = out_df.selectExpr("concat(city,'|',country) as key", "to_json(struct(*)) as value")
    (kafka_df.write
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("topic", RECORDS_TOPIC)
             .save())
    print(f"[OK] Records envoyés sur Kafka topic={RECORDS_TOPIC}")

# ------------------------------------------------------------------
def main():
    full_glob = resolve_glob(RAW_PATH_ENV)
    print(f"[INFO] Lecture historique glob={full_glob}")
    print(f"[DEBUG] fs.defaultFS attendu: {HDFS_NN}  (ne pas utiliser le port 9870)")

    spark = build_spark()
    spark.sparkContext.setLogLevel(LOG_LEVEL)

    # Lecture
    try:
        df = spark.read.json(full_glob)
    except Exception as e:
        print(f"[ERREUR] Impossible de lire le glob: {e}")
        print(f"[DEBUG] HIST_RAW_GLOB={RAW_PATH_ENV}")
        print("[DEBUG] Causes probables: port 9866 non exposé OU hostname datanode inaccessible depuis l'hôte.")
        print("[ACTION] Soit expose 9866 dans docker-compose, soit exécute ce script dans un conteneur réseau docker avec HDFS_NN=hdfs://namenode:9000")
        traceback.print_exc()
        # Vérifie le chemin ou exporte HIST_RAW_GLOB correctement.
        return

    if df.rdd.isEmpty():
        msg = "[INFO] Aucun fichier trouvé ou DataFrame vide."
        print(msg)
        if FAIL_EMPTY:
            raise SystemExit("Aucun historique disponible (FAIL_IF_EMPTY=1).")
        return

    out_df = compute_records(df)

    # Protection si aucun record (ex: données vides après filtrage)
    if out_df.rdd.isEmpty():
        print("[INFO] Aucun record calculé.")
        return

    write_hdfs(out_df)
    write_kafka(spark, out_df)

    spark.stop()

# ------------------------------------------------------------------
if __name__ == "__main__":
    main()