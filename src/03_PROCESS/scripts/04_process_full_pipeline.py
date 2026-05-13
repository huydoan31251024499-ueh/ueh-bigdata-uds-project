from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, round as spark_round,
    when, hour, dayofweek, month
)
import logging

HDFS_PROCESSED = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark():
    return SparkSession.builder \
        .appName("UDS_FULL_PIPELINE") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

def main():
    spark = get_spark()

    logger.info("Loading data...")
    orders = spark.read.parquet(f"{HDFS_PROCESSED}/orders")
    weather = spark.read.parquet(f"{HDFS_PROCESSED}/weather")
    flood = spark.read.parquet(f"{HDFS_PROCESSED}/flood_hourly")
    market = spark.read.parquet(f"{HDFS_PROCESSED}/market")

    logger.info("Renaming columns to avoid conflicts...")

    # ✅ Rename timestamp của bảng phụ (giữ full column)
    weather = weather.withColumnRenamed("timestamp", "weather_timestamp")
    flood = flood.withColumnRenamed("timestamp", "flood_timestamp")
    market = market.withColumnRenamed("timestamp", "market_timestamp")

    logger.info("Joining data...")

    df = orders.alias("o") \
        .join(weather.alias("w"), col("o.hour_timestamp") == col("w.hour_timestamp"), "left") \
        .join(flood.alias("f"), col("o.hour_timestamp") == col("f.hour_timestamp"), "left") \
        .join(market.alias("m"), col("o.hour_timestamp") == col("m.hour_timestamp"), "left") \
        .drop(col("w.hour_timestamp")) \
        .drop(col("f.hour_timestamp")) \
        .drop(col("m.hour_timestamp"))

    logger.info("Adding features...")

    df = df.withColumn(
        "actual_duration_min",
        spark_round((unix_timestamp("deliveredAt") - unix_timestamp("createdAt")) / 60, 2)
    ).withColumn(
        "expected_duration_min",
        spark_round((unix_timestamp("expectedDeliveryTime") - unix_timestamp("createdAt")) / 60, 2)
    ).withColumn(
        "delay_min",
        spark_round(col("actual_duration_min") - col("expected_duration_min"), 2)
    ).withColumn(
        "is_late",
        when(col("delay_min") > 0, 1).otherwise(0)
    )

    # Time features
    df = df.withColumn("order_hour", hour("createdAt")) \
           .withColumn("order_dow", dayofweek("createdAt")) \
           .withColumn("order_month", month("createdAt"))

    # Rain
    df = df.withColumn(
        "rain_level",
        when(col("prcp_mm").isNull() | (col("prcp_mm") == 0), "no_rain")
        .when(col("prcp_mm") < 2.5, "light")
        .when(col("prcp_mm") < 7.5, "moderate")
        .otherwise("heavy")
    )

    # Flags
    df = df.withColumn(
        "is_extreme_weather",
        when((col("prcp_mm") > 5) | (col("condition_label") == "Heavy Rain"), 1).otherwise(0)
    ).withColumn(
        "is_flooded",
        when(col("flood_avg_depth_cm").isNotNull(), 1).otherwise(0)
    ).withColumn(
        "is_high_congestion",
        when(col("traffic_congestion_index") > 6, 1).otherwise(0)
    )

    logger.info("Saving to HDFS...")

    # Save CSV for debug
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv("file:///app/data/processed/final_features_csv")
    
    df.write.mode("overwrite") \
        .parquet(f"{HDFS_PROCESSED}/final_features")

    logger.info("✅ DONE")
    spark.stop()

if __name__ == "__main__":
    main()