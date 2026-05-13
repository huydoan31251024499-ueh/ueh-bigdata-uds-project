from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

HDFS_RAW = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/raw"
HDFS_PROCESSED = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark():
    return SparkSession.builder \
        .appName("UDS_Market_Pipeline") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

def get_market_schema():
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("fuel_price_vnd_liter", LongType(), True),
        StructField("traffic_congestion_index", DoubleType(), True),
        StructField("avg_vehicle_speed_kmh", DoubleType(), True),
        StructField("delivery_fee_avg_vnd", LongType(), True),
        StructField("freight_cost_index", DoubleType(), True),
        StructField("rice_price_vnd_kg", LongType(), True),
        StructField("veg_price_vnd_kg", LongType(), True),
        StructField("active_delivery_vehicles", IntegerType(), True),
        StructField("rain_flag", IntegerType(), True),
    ])

def transform_market(df):
    df = df.withColumn(
        "timestamp",
        from_utc_timestamp(to_timestamp(col("timestamp")),
                           "Asia/Ho_Chi_Minh")
    )

    # congestion level
    df = df.withColumn(
        "congestion_level",
        when(col("traffic_congestion_index") < 2, "free_flow")
        .when(col("traffic_congestion_index") < 4, "normal")
        .when(col("traffic_congestion_index") < 6, "congested")
        .when(col("traffic_congestion_index") < 8, "heavy")
        .otherwise("gridlock")
    )

    # Driver income feature
    df = df.withColumn(
        "fee_multiplier",
        when(col("traffic_congestion_index") > 6, 1.3).otherwise(1.0)
    ).withColumn(
        "adjusted_fee",
        col("delivery_fee_avg_vnd") * col("fee_multiplier")
    )

    df = df.withColumn(
        "hour_timestamp",
        date_trunc("hour", col("timestamp"))
    )

    return df

def main():
    spark = get_spark()

    df = spark.read.csv(
        f"{HDFS_RAW}/hcmc_market_traffic_raw.csv",
        header=True,
        schema=get_market_schema()
    )

    df_clean = transform_market(df)

    df_clean.write.mode("overwrite").parquet(f"{HDFS_PROCESSED}/market")

    # Save CSV for debug
    df_clean.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv("file:///app/data/processed/market_csv")

    spark.stop()


if __name__ == "__main__":
    main()
