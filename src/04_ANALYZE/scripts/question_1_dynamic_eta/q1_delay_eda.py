from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, round as spark_round, sum as spark_sum
import logging

# =====================================================
# CONFIG & LOGGING
# =====================================================
HDFS_FINAL_FEATURES = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed/final_features"
OUTPUT_LOCAL_PATH = "file:///app/data/analysis/q1_delay_stats_csv"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark():
    return SparkSession.builder \
        .appName("UDS_Q1_EDA_Delay_Analysis") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

def main():
    spark = get_spark()
    try:
        logger.info("Loading final features for EDA...")
        df = spark.read.parquet(HDFS_FINAL_FEATURES)

        # 1. Lọc tập trung vào bài toán SMART 1: Dịch vụ hỏa tốc 3h & 5h
        fast_services_df = df.filter(col("serviceType").isin("3h", "5h"))

        # =====================================================
        # THỐNG KÊ 1: Tác động của Mưa lớn (>5mm) đến tỷ lệ trễ
        # =====================================================
        logger.info("Analyzing heavy rain impact (>5mm)...")
        rain_impact = fast_services_df.groupBy("serviceType", "rain_level") \
            .agg(
                count("*").alias("total_orders"),
                spark_round(avg("delay_min"), 2).alias("avg_delay_min"),
                spark_round(spark_sum("is_late") / count("*") * 100, 2).alias("late_rate_percentage"),
                spark_round(avg("prcp_mm"), 2).alias("avg_actual_prcp")
            ).orderBy("serviceType", "avg_delay_min")

        # =====================================================
        # THỐNG KÊ 2: Tác động kép (Mưa > 5mm + Ngập lụt)
        # =====================================================
        logger.info("Analyzing Compound Risk (Rain > 5mm AND Flooded)...")
        # Định nghĩa rủi ro cao dựa trên tiêu chí SMART Question
        compound_risk = fast_services_df.withColumn(
            "risk_segment",
            when((col("prcp_mm") > 5) & (col("has_flood") == 1), "CRITICAL_Rain_Flood")
            .when((col("prcp_mm") > 5), "HIGH_Rain_Only")
            .when((col("has_flood") == 1), "STRESS_Flood_Only")
            .otherwise("NORMAL")
        ).groupBy("risk_segment") \
        .agg(
            count("*").alias("order_volume"),
            spark_round(avg("delay_min"), 2).alias("avg_delay_min"),
            spark_round(avg("traffic_congestion_index"), 2).alias("avg_traffic_index"),
            spark_round(spark_sum("is_late") / count("*") * 100, 2).alias("late_probability_pct")
        ).orderBy(col("late_probability_pct").desc())

        # =====================================================
        # THỐNG KÊ 3: Thống kê "Điểm đen" trễ đơn theo Quận (District)
        # =====================================================
        logger.info("Analyzing delay hotspots by district...")
        # Sử dụng tọa độ/địa chỉ đã được sync từ flood_hourly [User Prompt 197]
        district_hotspots = fast_services_df.filter(col("is_late") == 1) \
            .groupBy("serviceType", "rain_level") \
            .agg(
                count("*").alias("late_order_count"),
                spark_round(avg("flood_avg_severity"), 2).alias("avg_flood_severity_at_delivery")
            ).orderBy(col("late_order_count").desc())

        # =====================================================
        # XUẤT KẾT QUẢ RA CSV LOCAL
        # =====================================================
        logger.info(f"Saving EDA results to {OUTPUT_LOCAL_PATH}...")
        
        # Hợp nhất các phân tích vào 1 folder kết quả
        rain_impact.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_LOCAL_PATH}/rain_impact")
        compound_risk.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_LOCAL_PATH}/compound_risk")
        district_hotspots.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_LOCAL_PATH}/hotspots")

        # Lưu kết quả vào HDFS dạng Parquet
        rain_impact.write.mode("overwrite").option("header", "true").parquet(f"hdfs://namenode:9000/user/doanquochuy/uds-project/data/analysis/eda/rain_impact") 
        compound_risk.write.mode("overwrite").option("header", "true").parquet(f"hdfs://namenode:9000/user/doanquochuy/uds-project/data/analysis/eda/compound_risk") 
        district_hotspots.write.mode("overwrite").option("header", "true").parquet(f"hdfs://namenode:9000/user/doanquochuy/uds-project/data/analysis/eda/district_hotspots") 

        logger.info("EDA COMPLETED SUCCESSFULLY")

    except Exception as e:
        logger.error(f"EDA Job failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()