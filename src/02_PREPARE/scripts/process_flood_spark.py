from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_utc_timestamp, to_timestamp,
    when, coalesce, lit, date_trunc,
    round as spark_round, unix_timestamp, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session():
    """Initialize and return Spark session connected to cluster"""
    spark = SparkSession.builder \
        .appName("FloodSparkProcess") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark


def get_flood_schema():
    """
    Define strict schema for flood points data (Schema-on-read).
    Avoid inferSchema to ensure type consistency (Veracity - MMDS).
    """
    return StructType([
        StructField("flood_id",            StringType(),  True),
        StructField("timestamp",           StringType(),  True),
        StructField("street",              StringType(),  True),
        StructField("district",            StringType(),  True),
        StructField("lat",                 DoubleType(),  True),
        StructField("lng",                 DoubleType(),  True),
        StructField("depth_cm",            DoubleType(),  True),
        StructField("duration_min",        IntegerType(), True),
        StructField("flood_level",         StringType(),  True),
        StructField("traffic_impact",      StringType(),  True),
        StructField("rainfall_trigger_mm", DoubleType(),  True),
        StructField("source",              StringType(),  True),
        StructField("verified",            BooleanType(), True),
    ])


def transform_flood(spark, flood_path):
    """
    Transform raw flood points data:
    - Parse and convert timestamps from UTC to UTC+7 (Asia/Ho_Chi_Minh)
    - Filter valid GPS coordinates within HCMC boundary
    - Filter reliable data sources (verified or IoT/monitoring station)
    - Cast numeric columns to DoubleType for computation
    - Add flood severity score feature
    - Add hour_timestamp join key compatible with orders pipeline
    """
    print("Reading raw flood points data...")

    try:
        flood_schema = get_flood_schema()

        flood_df = spark.read.option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .csv(flood_path, schema=flood_schema)

        print("Raw flood schema:")
        flood_df.printSchema()
        print("Sample flood data:")
        flood_df.show(3, truncate=False)
        print(f"Raw flood count: {flood_df.count()} rows")

        # Parse timestamp + convert UTC -> UTC+7 (Asia/Ho_Chi_Minh)
        flood_df = flood_df.withColumn(
            "timestamp",
            from_utc_timestamp(
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                "Asia/Ho_Chi_Minh"
            )
        )

        # Filter valid GPS coordinates within HCMC boundary
        # Lat: 10.4 - 11.2, Lng: 106.3 - 107.1
        flood_df = flood_df.filter(
            col("lat").between(10.4, 11.2) &
            col("lng").between(106.3, 107.1)
        )

        # Filter reliable sources only:
        # Keep verified records OR records from IoT_sensor/monitoring_station
        # Exclude unverified user_report and traffic_camera as less reliable
        flood_df = flood_df.filter(
            col("verified") |
            col("source").isin("IoT_sensor", "monitoring_station")
        )

        # Cast numeric columns to DoubleType for computation (MMDS Transformation)
        flood_df = flood_df \
            .withColumn("depth_cm",
                        col("depth_cm").cast(DoubleType())) \
            .withColumn("duration_min",
                        col("duration_min").cast(DoubleType())) \
            .withColumn("rainfall_trigger_mm",
                        col("rainfall_trigger_mm").cast(DoubleType()))

        # Add flood severity score: depth_cm * duration_min / 100
        # Higher score = more severe flood impact on traffic
        flood_df = flood_df.withColumn(
            "flood_severity_score",
            spark_round(
                col("depth_cm") * col("duration_min") / 100.0, 2
            )
        )

        # Add hour_timestamp join key:
        # Round timestamp to nearest hour using unix timestamp arithmetic
        flood_df = flood_df.withColumn(
            "hour_timestamp",
            from_unixtime(
                spark_round(unix_timestamp(col("timestamp")) / 3600) * 3600
            ).cast(TimestampType())
        )

        print("Flood transformation complete. Sample transformed data:")
        flood_df.show(5, truncate=False)
        print(f"Flood clean count: {flood_df.count()} rows")

        return flood_df

    except Exception as e:
        print(f"Error in flood transformation: {e}")
        raise


def aggregate_flood_by_hour(flood_df):
    """
    Aggregate flood points to hourly level for joining with orders.
    Orders are individual events; flood points can have multiple per hour.
    Aggregation: count, avg depth, avg severity, avg duration per hour.
    """
    print("Aggregating flood data by hour...")

    try:
        from pyspark.sql.functions import count, avg

        flood_agg = flood_df.groupBy("hour_timestamp").agg(
            count("flood_id").alias("flood_count"),
            avg("depth_cm").alias("avg_flood_depth_cm"),
            avg("flood_severity_score").alias("avg_flood_severity_score"),
            avg("duration_min").alias("avg_flood_duration_min"),
            avg("rainfall_trigger_mm").alias("avg_rainfall_trigger_mm")
        ).withColumn(
            "has_flood",
            when(col("flood_count") > 0, lit(1)).otherwise(lit(0))
        ).withColumn(
            "avg_flood_depth_cm",
            spark_round(col("avg_flood_depth_cm"), 2)
        ).withColumn(
            "avg_flood_severity_score",
            spark_round(col("avg_flood_severity_score"), 2)
        ).withColumn(
            "avg_flood_duration_min",
            spark_round(col("avg_flood_duration_min"), 2)
        ).withColumn(
            "avg_rainfall_trigger_mm",
            spark_round(col("avg_rainfall_trigger_mm"), 2)
        )

        print("Flood aggregation complete. Sample:")
        flood_agg.show(5, truncate=False)
        print(f"Flood hourly records: {flood_agg.count()} hours")

        return flood_agg

    except Exception as e:
        print(f"Error in flood aggregation: {e}")
        raise


def main():
    spark = get_spark_session()

    flood_path  = "hdfs://namenode:9000/uds/data/raw/hcmc_flood_points_raw.csv"
    output_path = "hdfs://namenode:9000/uds/data/processed/flood_clean"

    try:
        print("=" * 70)
        print("STEP 1: Flood Points Transformation")
        print("=" * 70)
        flood_df = transform_flood(spark, flood_path)

        print("\n" + "=" * 70)
        print("STEP 2: Aggregate Flood by Hour")
        print("=" * 70)
        flood_agg = aggregate_flood_by_hour(flood_df)

        print("\n" + "=" * 70)
        print("STEP 3: Save to HDFS as Parquet")
        print("=" * 70)

        # Save aggregated flood (for joining with orders)
        flood_agg.write.mode("overwrite").parquet(output_path)
        print(f"Flood clean saved to: {output_path}")

        print("\n" + "=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)
        print(f"Total hourly flood records : {flood_agg.count()}")
        print(f"Hours with flooding        : {flood_agg.filter(col('has_flood') == 1).count()}")

        print("\nFlood level distribution:")
        flood_df.groupBy("flood_level").count().orderBy("flood_level").show()

        print("\nSource distribution:")
        flood_df.groupBy("source").count().orderBy("count", ascending=False).show()

        print("\nOutput schema:")
        flood_agg.printSchema()

    except Exception as e:
        print(f"Error in PySpark job: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
