from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_utc_timestamp, to_timestamp,
    when, coalesce, lit, date_trunc,
    round as spark_round, unix_timestamp, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, TimestampType
)
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session():
    """Initialize and return Spark session connected to cluster"""
    spark = SparkSession.builder \
        .appName("MarketTrafficSparkProcess") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark


def get_market_schema():
    """
    Define strict schema for market and traffic data (Schema-on-read).
    Avoid inferSchema to ensure type consistency (Veracity - MMDS).
    """
    return StructType([
        StructField("timestamp",                StringType(),  True),
        StructField("fuel_price_vnd_liter",     LongType(),    True),
        StructField("traffic_congestion_index", DoubleType(),  True),
        StructField("avg_vehicle_speed_kmh",    DoubleType(),  True),
        StructField("delivery_fee_avg_vnd",     LongType(),    True),
        StructField("freight_cost_index",       DoubleType(),  True),
        StructField("rice_price_vnd_kg",        LongType(),    True),
        StructField("veg_price_vnd_kg",         LongType(),    True),
        StructField("active_delivery_vehicles", IntegerType(), True),
        StructField("rain_flag",                IntegerType(), True),
        StructField("road_incidents",           IntegerType(), True),
    ])


def get_congestion_level(df):
    """
    Classify traffic congestion index (0-10) into levels.
    Based on standard Traffic Congestion Index scale:
    - 0-2  : free_flow    (thong thoang)
    - 2-4  : normal       (binh thuong)
    - 4-6  : congested    (dong duc)
    - 6-8  : heavy        (un tac)
    - 8-10 : gridlock     (ket xe nang)
    """
    return df.withColumn(
        "congestion_level",
        when(col("traffic_congestion_index") < 2.0, lit("free_flow"))
        .when(col("traffic_congestion_index") < 4.0, lit("normal"))
        .when(col("traffic_congestion_index") < 6.0, lit("congested"))
        .when(col("traffic_congestion_index") < 8.0, lit("heavy"))
        .otherwise(lit("gridlock"))
    )


def transform_market(spark, market_path):
    """
    Transform raw market and traffic data:
    - Parse and convert timestamps from UTC to UTC+7 (Asia/Ho_Chi_Minh)
    - Cast all numeric columns to DoubleType for computation
    - Classify traffic congestion index into readable levels
    - Add price trend features for driver income analysis
    - Add hour_timestamp join key compatible with orders and weather pipeline
    """
    print("Reading raw market and traffic data...")

    try:
        market_schema = get_market_schema()

        market_df = spark.read.option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .csv(market_path, schema=market_schema)

        print("Raw market schema:")
        market_df.printSchema()
        print("Sample market data:")
        market_df.show(3, truncate=False)
        print(f"Raw market count: {market_df.count()} rows")

        # Parse timestamp + convert UTC -> UTC+7 (Asia/Ho_Chi_Minh)
        market_df = market_df.withColumn(
            "timestamp",
            from_utc_timestamp(
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                "Asia/Ho_Chi_Minh"
            )
        )

        # Cast all numeric columns to DoubleType (MMDS Transformation)
        market_df = market_df \
            .withColumn("fuel_price_vnd_liter",
                        col("fuel_price_vnd_liter").cast(DoubleType())) \
            .withColumn("traffic_congestion_index",
                        col("traffic_congestion_index").cast(DoubleType())) \
            .withColumn("avg_vehicle_speed_kmh",
                        col("avg_vehicle_speed_kmh").cast(DoubleType())) \
            .withColumn("delivery_fee_avg_vnd",
                        col("delivery_fee_avg_vnd").cast(DoubleType())) \
            .withColumn("freight_cost_index",
                        col("freight_cost_index").cast(DoubleType())) \
            .withColumn("rice_price_vnd_kg",
                        col("rice_price_vnd_kg").cast(DoubleType())) \
            .withColumn("veg_price_vnd_kg",
                        col("veg_price_vnd_kg").cast(DoubleType())) \
            .withColumn("active_delivery_vehicles",
                        col("active_delivery_vehicles").cast(DoubleType())) \
            .withColumn("road_incidents",
                        col("road_incidents").cast(DoubleType()))

        # Classify congestion level from index
        market_df = get_congestion_level(market_df)

        # Add fuel cost per km feature (motorbike avg: 2L per 100km = 0.02L/km)
        # Used later for driver income estimation
        FUEL_CONSUMPTION_PER_KM = 0.02
        market_df = market_df.withColumn(
            "fuel_cost_per_km_vnd",
            spark_round(
                col("fuel_price_vnd_liter") * FUEL_CONSUMPTION_PER_KM, 0
            )
        )

        # Add market pressure index:
        # Combines freight cost + road incidents + congestion
        # Higher = harder operating conditions for drivers
        market_df = market_df.withColumn(
            "market_pressure_index",
            spark_round(
                (col("freight_cost_index") * 0.4) +
                (col("traffic_congestion_index") * 0.4) +
                (col("road_incidents") * 0.2),
                2
            )
        )

        # Add hour_timestamp join key (same style as orders and weather pipeline):
        # Round timestamp to nearest hour using unix timestamp arithmetic
        market_df = market_df.withColumn(
            "hour_timestamp",
            from_unixtime(
                spark_round(unix_timestamp(col("timestamp")) / 3600) * 3600
            ).cast(TimestampType())
        )

        print("Market transformation complete. Sample transformed data:")
        market_df.show(5, truncate=False)
        print(f"Market clean count: {market_df.count()} rows")

        return market_df

    except Exception as e:
        print(f"Error in market transformation: {e}")
        raise


def add_driver_income_features(market_df):
    """
    Add features for driver income analysis:
    - Delivery fee multipliers based on congestion and rain flag
    - Estimated delivery fee adjusted for conditions
    These will be used when joined with orders for income estimation.
    """
    print("Adding driver income features...")

    try:
        # Fee multiplier based on traffic congestion
        market_df = market_df.withColumn(
            "congestion_fee_multiplier",
            when(col("congestion_level") == "gridlock",   lit(1.5))
            .when(col("congestion_level") == "heavy",     lit(1.3))
            .when(col("congestion_level") == "congested", lit(1.1))
            .otherwise(lit(1.0))
        )

        # Fee multiplier based on rain flag
        market_df = market_df.withColumn(
            "rain_fee_multiplier",
            when(col("rain_flag") == 1, lit(1.2))
            .otherwise(lit(1.0))
        )

        # Adjusted delivery fee = base fee * congestion multiplier * rain multiplier
        market_df = market_df.withColumn(
            "adjusted_delivery_fee_vnd",
            spark_round(
                col("delivery_fee_avg_vnd") *
                col("congestion_fee_multiplier") *
                col("rain_fee_multiplier"),
                0
            )
        )

        print("Driver income features added.")
        return market_df

    except Exception as e:
        print(f"Error adding driver income features: {e}")
        raise


def main():
    spark = get_spark_session()

    market_path = "hdfs://namenode:9000/uds/data/raw/hcmc_market_traffic_raw.csv"
    output_path = "hdfs://namenode:9000/uds/data/processed/market_clean"

    try:
        print("=" * 70)
        print("STEP 1: Market & Traffic Transformation")
        print("=" * 70)
        market_df = transform_market(spark, market_path)

        print("\n" + "=" * 70)
        print("STEP 2: Add Driver Income Features")
        print("=" * 70)
        market_df = add_driver_income_features(market_df)

        print("\n" + "=" * 70)
        print("STEP 3: Save to HDFS as Parquet")
        print("=" * 70)

        market_df.write.mode("overwrite").parquet(output_path)
        print(f"Market clean saved to: {output_path}")

        print("\n" + "=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)
        print(f"Total hourly records: {market_df.count()}")

        print("\nCongestion level distribution:")
        market_df.groupBy("congestion_level") \
            .count() \
            .orderBy("count", ascending=False) \
            .show()

        print("\nAvg delivery fee by congestion level:")
        market_df.groupBy("congestion_level") \
            .agg({"delivery_fee_avg_vnd": "avg",
                  "adjusted_delivery_fee_vnd": "avg",
                  "fuel_cost_per_km_vnd": "avg"}) \
            .withColumnRenamed("avg(delivery_fee_avg_vnd)",      "avg_base_fee") \
            .withColumnRenamed("avg(adjusted_delivery_fee_vnd)", "avg_adjusted_fee") \
            .withColumnRenamed("avg(fuel_cost_per_km_vnd)",      "avg_fuel_cost_km") \
            .orderBy("avg_adjusted_fee", ascending=False) \
            .show()

        print("\nOutput schema:")
        market_df.printSchema()

    except Exception as e:
        print(f"Error in PySpark job: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
