from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round as spark_round
import logging

# ==============================
# CONFIG
# ==============================
HDFS_FINAL_FEATURES = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed/final_features"
OUTPUT_LOCAL = "file:///app/data/analysis/q2_economic_results_csv"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark():
    return SparkSession.builder \
        .appName("UDS_Q2_Fix_Schema_Mismatch") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

def main():
    spark = get_spark()
    try:
        logger.info("Loading final_features and applying on-the-fly segmentation...")
        df_raw = spark.read.parquet(HDFS_FINAL_FEATURES)

        # 1. TÁI CẤU TRÚC LOGIC (BRIDGE): Tạo context_segment dựa trên logic của Q1
        # Điều này đảm bảo tính nhất quán (Veracity) giữa bài toán ETA và Kinh tế [3]
        df_with_context = df_raw.withColumn(
            "context_segment",
            when((col("has_flood") == 1) & (col("is_heavy_rain") == 1), "rain_flood")
            .when((col("has_flood") == 1), "flood_only")
            .when((col("is_heavy_rain") == 1), "rain_only")
            .otherwise("normal")
        )


        # 2. Đăng ký View với Schema đã được bổ sung
        df_with_context.createOrReplaceTempView("uds_operations")

        # =========================================================
        # TRUY VẤN: PHÂN TÍCH THU NHẬP THEO PHÂN ĐOẠN (Q2 ANALYZE)
        # =========================================================
        # Sử dụng delivery_fee_avg_vnd làm biến thu nhập gốc từ market.csv [4]
        economic_penalty = spark.sql("""
            SELECT 
                context_segment,
                COUNT(*) as order_volume,
                ROUND(AVG(actual_duration_min), 2) as avg_time_mins,
                ROUND(AVG(delivery_fee_avg_vnd / actual_duration_min), 2) as income_per_min_raw,
                ROUND(AVG(traffic_congestion_index), 2) as avg_traffic
            FROM uds_operations
            GROUP BY context_segment
            ORDER BY income_per_min_raw DESC
        """)

        # =========================================================
        # MÔ PHỎNG GIÁ LINH ĐỘNG (ACT PREVIEW)
        # Hệ số nhân dựa trên độ khó vật lý thực tế: Flood (0.4) + Traffic (0.4) [History]
        # =========================================================
        simulation = spark.sql("""
            SELECT 
                context_segment,
                ROUND(AVG(delivery_fee_avg_vnd / actual_duration_min), 2) as old_income_min,
                ROUND(AVG((delivery_fee_avg_vnd * 
                    CASE 
                        WHEN context_segment = 'rain_flood' THEN 1.5
                        WHEN context_segment = 'rain_only' THEN 1.3
                        ELSE 1.0 
                    END) / actual_duration_min), 2) as new_income_min
            FROM uds_operations
            GROUP BY context_segment
        """)

        # 3. XUẤT KẾT QUẢ RA LOCAL CSV
        logger.info("Saving corrected analysis to local...")
        economic_penalty.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_LOCAL}/penalty")
        simulation.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_LOCAL}/simulation")

        logger.info("✅ Q2 Analysis fixed and completed successfully.")

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()