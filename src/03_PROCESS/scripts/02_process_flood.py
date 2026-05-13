from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, from_utc_timestamp, to_timestamp,
    date_trunc
)
from pyspark.sql.types import *
import logging

# ==============================
# CONFIG
# ==============================
HDFS_RAW = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/raw"
HDFS_PROCESSED = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================
# SPARK
# ==============================
def get_spark():
    return SparkSession.builder \
        .appName("UDS_Flood_Pipeline") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

# ==============================
# SCHEMA
# ==============================
def get_flood_schema():
    return StructType([
        StructField("flood_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("street", StringType(), True),
        StructField("district", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("depth_cm", DoubleType(), True),
        StructField("duration_min", IntegerType(), True),
        StructField("flood_level", StringType(), True),
        StructField("traffic_impact", StringType(), True),
        StructField("rainfall_trigger_mm", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("verified", BooleanType(), True),
    ])

# ==============================
# TRANSFORM
# ==============================
def transform_flood(df):
    return (
        df
        # Convert timestamp UTC → UTC+7
        .withColumn(
            "timestamp",
            from_utc_timestamp(to_timestamp(col("timestamp")),
                               "Asia/Ho_Chi_Minh")
        )

        # Filter GPS HCM
        .filter(col("lat").between(10.4, 11.2))
        .filter(col("lng").between(106.3, 107.1))

        # Keep trusted data only
        .filter((col("verified") == True) | col("source").isin("iot", "station"))

        # Feature: severity score
        .withColumn(
            "severity_score",
            col("depth_cm") * 0.7 + col("duration_min") * 0.3
        )

        # Hour key for join
        .withColumn(
            "hour_timestamp",
            date_trunc("hour", col("timestamp"))
        )
    )

# ==============================
# AGGREGATION (FIXED)
# ==============================
def aggregate_flood(df):
    return (
        df.groupBy("hour_timestamp")
        .agg(
            avg("depth_cm").alias("flood_avg_depth_cm"),
            avg("severity_score").alias("flood_avg_severity"),
            avg("duration_min").alias("flood_avg_duration")
        )
    )

# ==============================
# MAIN
# ==============================
def main():
    spark = get_spark()

    try:
        logger.info("Loading RAW flood data...")

        # ✅ FIX: đọc đúng file (KHÔNG phải folder flood)
        df = spark.read.csv(
            f"{HDFS_RAW}/hcmc_flood_points_raw.csv",
            header=True,
            schema=get_flood_schema()
        )

        logger.info("Transforming flood data...")
        df_clean = transform_flood(df)

        logger.info("Aggregating flood hourly...")
        df_agg = aggregate_flood(df_clean)

        logger.info("Writing to HDFS...")

        df_clean.write.mode("overwrite").parquet(
            f"{HDFS_PROCESSED}/flood"
        )

        df_agg.write.mode("overwrite").parquet(
            f"{HDFS_PROCESSED}/flood_hourly"
        )

        # Save CSV for debug
        df_clean.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv("file:///app/data/processed/flood_csv")

        df_agg.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv("file:///app/data/processed/flood_hourly_csv")


        logger.info("✅ DONE: flood + flood_hourly saved")

    except Exception as e:
        logger.error(f"❌ Job failed: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
