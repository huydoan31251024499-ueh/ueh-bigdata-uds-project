from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, round as spark_round,
    when, hour, dayofweek, month,
    avg, count
)
import logging

# =====================================================
# CONFIG
# =====================================================
HDFS_PROCESSED = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark():
    return (
        SparkSession.builder
        .appName("UDS_FULL_PROCESS_PIPELINE")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )


def main():
    spark = get_spark()

    # =================================================
    # 1. LOAD DATA
    # =================================================
    logger.info("Loading parquet datasets...")

    orders = spark.read.parquet(f"{HDFS_PROCESSED}/orders")
    weather = spark.read.parquet(f"{HDFS_PROCESSED}/weather")
    flood_raw = spark.read.parquet(f"{HDFS_PROCESSED}/flood")   # raw flood events
    market = spark.read.parquet(f"{HDFS_PROCESSED}/market")

    # =================================================
    # 2. BUILD FLOOD-HOURLY (EXTENDED)
    # =================================================
    logger.info("Aggregating flood data to hourly level...")

    flood_hourly = (
        flood_raw
        .groupBy("hour_timestamp")
        .agg(
            avg("depth_cm").alias("flood_avg_depth_cm"),
            avg("severity_score").alias("flood_avg_severity"),
            avg("duration_min").alias("flood_avg_duration"),
            avg("rainfall_trigger_mm").alias("avg_rainfall_trigger_mm"),
            count("*").alias("flood_count")
        )
        .withColumn(
            "has_flood",
            when(col("flood_count") > 0, 1).otherwise(0)
        )
    )

    # =================================================
    # 3. RENAME TIMESTAMP COLUMNS (AVOID COLLISION)
    # =================================================
    logger.info("Renaming timestamp columns...")

    weather = weather.withColumnRenamed("timestamp", "weather_timestamp")
    flood_raw = flood_raw.withColumnRenamed("timestamp", "flood_event_timestamp")
    market = market.withColumnRenamed("timestamp", "market_timestamp")

    # =================================================
    # 4. JOIN ALL SOURCES (LEFT JOIN)
    # =================================================
    logger.info("Joining datasets using hour_timestamp...")

    df = (
        orders.alias("o")
        .join(
            weather.alias("w"),
            col("o.hour_timestamp") == col("w.hour_timestamp"),
            "left"
        )
        .join(
            flood_hourly.alias("fh"),
            col("o.hour_timestamp") == col("fh.hour_timestamp"),
            "left"
        )
        .join(
            market.alias("m"),
            col("o.hour_timestamp") == col("m.hour_timestamp"),
            "left"
        )
    )

    # Remove duplicated join keys from RHS tables
    df = df.drop(
        col("w.hour_timestamp"),
        col("fh.hour_timestamp"),
        col("m.hour_timestamp")
    )

    # =================================================
    # 5. CORE DELIVERY METRICS
    # =================================================
    logger.info("Computing delivery metrics...")

    df = (
        df
        .withColumn(
            "actual_duration_min",
            spark_round(
                (unix_timestamp("deliveredAt") - unix_timestamp("createdAt")) / 60, 2
            )
        )
        .withColumn(
            "expected_duration_min",
            spark_round(
                (unix_timestamp("expectedDeliveryTime") - unix_timestamp("createdAt")) / 60, 2
            )
        )
        .withColumn(
            "delay_min",
            spark_round(
                col("actual_duration_min") - col("expected_duration_min"), 2
            )
        )
        .withColumn(
            "is_late",
            when(col("delay_min") > 0, 1).otherwise(0)
        )
    )

    # =================================================
    # 6. TEMPORAL FEATURES
    # =================================================
    logger.info("Adding temporal features...")

    df = (
        df
        .withColumn("order_hour", hour("createdAt"))
        .withColumn("order_dow", dayofweek("createdAt"))
        .withColumn("order_month", month("createdAt"))
    )

    # =================================================
    # 7. WEATHER & FLOOD FEATURES
    # =================================================
    logger.info("Adding weather and flood features...")

    df = (
        df
        .withColumn(
            "rain_level",
            when(col("prcp_mm").isNull() | (col("prcp_mm") == 0), "no_rain")
            .when(col("prcp_mm") < 2.5, "light")
            .when(col("prcp_mm") < 7.5, "moderate")
            .otherwise("heavy")
        )
        .withColumn(
            "is_extreme_weather",
            when(
                (col("prcp_mm") > 5) |
                (col("condition_label").isin("Heavy Rain", "Thunderstorm")),
                1
            ).otherwise(0)
        )
    )

    # =================================================
    # 8. TRAFFIC FLAGS
    # =================================================
    logger.info("Adding traffic flags...")

    df = df.withColumn(
        "is_high_congestion",
        when(col("traffic_congestion_index") > 6, 1).otherwise(0)
    )

    # =================================================
    # 9. SAVE OUTPUT
    # =================================================
    logger.info("Saving final_features to HDFS...")

    df.write.mode("overwrite").parquet(
        f"{HDFS_PROCESSED}/final_features"
    )

    logger.info("✅ FULL PROCESS PIPELINE COMPLETED SUCCESSFULLY")
    spark.stop()


if __name__ == "__main__":
    main()