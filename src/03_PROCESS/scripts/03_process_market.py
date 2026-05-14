from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_utc_timestamp, to_timestamp, 
    date_trunc, when
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
        StructField("road_incidents", IntegerType(), True)
    ])

# ==============================
# TRANSFORM
# ==============================
def transform_market(df):
    # 1. Chuẩn hóa thời gian sang UTC+7
    df = df.withColumn(
        "timestamp",
        from_utc_timestamp(to_timestamp(col("timestamp")), "Asia/Ho_Chi_Minh")
    )

    # 2. Phân loại congestion_level (CHỈ dùng cho Visualization/Dashboard)
    # Lưu ý: Mô hình ML sẽ sử dụng cột gốc 'traffic_congestion_index' (Dạng số)
    df = df.withColumn(
        "congestion_level",
        when(col("traffic_congestion_index") < 2, "free_flow")
        .when(col("traffic_congestion_index") < 4, "normal")
        .when(col("traffic_congestion_index") < 6, "congested")
        .when(col("traffic_congestion_index") < 8, "heavy")
        .otherwise("gridlock")
    )

    # 3. LOẠI BỎ Driver Income Feature (fee_multiplier, adjusted_fee)
    # Lý do: Tránh đưa các giả định chủ quan không có tham khảo vào mô hình dự báo ETA.

    # 4. Tạo Temporal Join Key
    df = df.withColumn(
        "hour_timestamp",
        date_trunc("hour", col("timestamp"))
    )
    
    return df

# ==============================
# MAIN
# ==============================
def main():
    spark = get_spark()
    try:
        logger.info("Loading RAW market data...")
        df = spark.read.csv(
            f"{HDFS_RAW}/hcmc_market_traffic_raw.csv",
            header=True,
            schema=get_market_schema()
        )

        logger.info("Transforming market data...")
        df_clean = transform_market(df)

        logger.info("Writing Processed Market data to HDFS...")
        # Ghi bản sạch vào HDFS
        df_clean.write.mode("overwrite").parquet(f"{HDFS_PROCESSED}/market")

        # Lưu CSV phục vụ debug/kiểm tra
        df_clean.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv("file:///app/data/processed/market_csv")
            
        logger.info("DONE: Market data processed successfully")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()