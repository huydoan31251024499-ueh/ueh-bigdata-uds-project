from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, round as spark_round,
    when, hour, dayofweek, month,
    avg, count, max, min
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
        .appName("UDS_FULL_PROCESS_PIPELINE_V2")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

def main():
    spark = get_spark()
    
    # =================================================
    # 1. LOAD DATA
    # =================================================
    logger.info("Loading cleaned parquet datasets...")
    orders = spark.read.parquet(f"{HDFS_PROCESSED}/orders")
    weather = spark.read.parquet(f"{HDFS_PROCESSED}/weather")
    flood_raw = spark.read.parquet(f"{HDFS_PROCESSED}/flood") # Dữ liệu điểm ngập chi tiết
    market = spark.read.parquet(f"{HDFS_PROCESSED}/market")

    # =================================================
    # 2. BUILD FLOOD-HOURLY (QUANTITATIVE FOCUS)
    # =================================================
    logger.info("Aggregating flood data with quantitative metrics...")
    # Ưu tiên depth_cm và rainfall_trigger_mm thay vì traffic_impact nhãn chữ [User Prompt]
    flood_hourly = (
        flood_raw
        .groupBy("hour_timestamp")
        .agg(
            avg("depth_cm").alias("flood_avg_depth_cm"),
            max("depth_cm").alias("flood_max_depth_cm"),
            avg("severity_score").alias("flood_avg_severity"),
            avg("rainfall_trigger_mm").alias("avg_rainfall_trigger_mm"),
            count("flood_id").alias("flood_point_count")
        )
        .withColumn("has_flood", when(col("flood_point_count") > 0, 1).otherwise(0))
    )

    # =================================================
    # 3. RENAME & JOIN (LEFT JOIN)
    # =================================================
    logger.info("Joining sources and removing redundant labels...")
    
    # Loại bỏ các cột thu nhập/phí giả định từ market trước khi Join [User Prompt]
    market_clean = market.drop("fee_multiplier", "adjusted_fee", "congestion_level")
    
    weather = weather.withColumnRenamed("timestamp", "weather_timestamp")
    market_clean = market_clean.withColumnRenamed("timestamp", "market_timestamp")

    df = (
        orders.alias("o")
        .join(weather.alias("w"), "hour_timestamp", "left")
        .join(flood_hourly.alias("fh"), "hour_timestamp", "left")
        .join(market_clean.alias("m"), "hour_timestamp", "left")
    )

    # =================================================
    # 4. DELIVERY METRICS & TEMPORAL FEATURES
    # =================================================
    logger.info("Computing core delivery metrics...")
    df = (
        df
        .withColumn("actual_duration_min", 
            spark_round((unix_timestamp("deliveredAt") - unix_timestamp("createdAt")) / 60, 2))
        .withColumn("expected_duration_min", 
            spark_round((unix_timestamp("expectedDeliveryTime") - unix_timestamp("createdAt")) / 60, 2))
        .withColumn("delay_min", spark_round(col("actual_duration_min") - col("expected_duration_min"), 2))
        .withColumn("is_late", when(col("delay_min") > 0, 1).otherwise(0))
        .withColumn("order_hour", hour("createdAt"))
        .withColumn("order_dow", dayofweek("createdAt"))
    )

    # =================================================
    # 5. WEATHER BINS (WMO STANDARD)
    # =================================================
    logger.info("Adding WMO-standard rain levels...")
    # Giữ rain_level để xử lý tác động phi tuyến tính (mưa > 2.5mm gây trễ vọt) [User Prompt, 71]
    df = df.withColumn(
        "rain_level",
        when(col("prcp_mm").isNull() | (col("prcp_mm") == 0), "no_rain")
        .when(col("prcp_mm") < 2.5, "light")
        .when(col("prcp_mm") < 7.5, "moderate")
        .otherwise("heavy")
    )

    # LOẠI BỎ is_extreme_weather vì redundant với condition_label và prcp_mm [User Prompt]
    # SỬ DỤNG traffic_congestion_index dạng số thực cho mô hình ML để tăng độ chính xác [User Prompt]

    # =================================================
    # 6. FILL NULLS & SAVE
    # =================================================
    # Điền 0 cho các chỉ số ngập lụt ở khung giờ không ngập
    df = df.fillna({
        "flood_avg_depth_cm": 0, 
        "flood_avg_severity": 0, 
        "flood_point_count": 0, 
        "has_flood": 0,
        "avg_rainfall_trigger_mm": 0
    })

    logger.info("Saving final_features to HDFS...")
    df.write.mode("overwrite").parquet(f"{HDFS_PROCESSED}/final_features")
    
    # Xuất CSV để kiểm tra tính chính xác của tọa độ và các trường định lượng
    df.coalesce(1).limit(1000).write.mode("overwrite").option("header", "true") \
        .csv("file:///app/data/processed/final_debug_csv")

    logger.info("✅ FULL PROCESS PIPELINE COMPLETED: Focus on Quantitative & Numeric Features")
    spark.stop()

if __name__ == "__main__":
    main()