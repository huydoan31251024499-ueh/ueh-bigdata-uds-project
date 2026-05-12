from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round, avg, count
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import logging

# Tắt bớt log rác của Spark
logging.getLogger("py4j").setLevel(logging.ERROR)

def main():
    print("="*60)
    print("BẮT ĐẦU CHẠY PHÂN TÍCH VÀ MÔ HÌNH (GIAI ĐOẠN ANALYZE)")
    print("="*60)

    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("UDS_Analyze_And_Model") \
        .master("local[*]") \
        .getOrCreate()

    # 2. Đọc dữ liệu đã được làm sạch từ bước PROCESS
    input_path = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed/weather_orders_joined.parquet"
    print(f"Đang đọc dữ liệu từ: {input_path}")
    df = spark.read.parquet(input_path)

    # BƯỚC 1: TÍNH TOÁN BIẾN SỐ (FEATURE ENGINEERING)
    
    # 1.1 Tính thời gian giao hàng thực tế (bằng giờ)
    df = df.withColumn("delivery_time_hours", 
                       (unix_timestamp("deliveredAt") - unix_timestamp("createdAt")) / 3600.0)
    
    # Lọc bỏ các dòng lỗi (thời gian giao < 0 hoặc quãng đường = 0)
    df = df.filter((col("delivery_time_hours") > 0) & (col("shippingDistance") > 0))
    
    # 1.2 Tính V_delivery (Tốc độ giao hàng trung bình - km/h)
    df = df.withColumn("V_delivery", round(col("shippingDistance") / col("delivery_time_hours"), 2))
    
    # 1.3 Tính WIS (Weather Impact Score)
    # Lưu ý: Do dữ liệu hiện tại không có flood_risk, nhóm thống nhất dùng trọng số:
    alpha, gamma = 0.7, 0.3
    df = df.withColumn("WIS", round((col("prcp_mm") * alpha) + (col("wspd_kmh") * gamma), 2))

    print("\n[ĐÃ XONG] Tính toán 2 biến số V_delivery và WIS. 5 dòng đầu tiên:")
    df.select("createdAt", "shippingDistance", "delivery_time_hours", "V_delivery", "WIS", "is_extreme_weather").show(5)

    # BƯỚC 2: PHÂN TÍCH QUY LUẬT (PATTERNS)
    print("\n=== SO SÁNH HIỆU SUẤT: TRỜI ĐẸP vs THỜI TIẾT CỰC ĐOAN ===")
    pattern_df = df.groupBy("is_extreme_weather").agg(
        round(avg("delivery_time_hours"), 2).alias("avg_delivery_hours"),
        round(avg("V_delivery"), 2).alias("avg_velocity_kmh"),
        count("*").alias("total_orders")
    )
    pattern_df.show()

    # BƯỚC 3: MÔ HÌNH HỌC MÁY (LINEAR REGRESSION)
    print("\n=== HUẤN LUYỆN MÔ HÌNH DỰ BÁO THỜI GIAN GIAO HÀNG ===")
    
    # Loại bỏ giá trị Null trước khi đưa vào mô hình
    ml_df = df.dropna(subset=["prcp_mm", "wspd_kmh", "shippingDistance", "delivery_time_hours"])

    # Gom các đặc trưng (features) thành 1 vector
    assembler = VectorAssembler(
        inputCols=["prcp_mm", "wspd_kmh", "shippingDistance"],
        outputCol="features"
    )
    data_assembled = assembler.transform(ml_df)
    
    # Chia tập dữ liệu (80% Train, 20% Test)
    train_data, test_data = data_assembled.randomSplit([0.8, 0.2], seed=42)

    # Khởi tạo và chạy mô hình
    lr = LinearRegression(featuresCol="features", labelCol="delivery_time_hours")
    lr_model = lr.fit(train_data)

    # Đánh giá trên tập Test
    predictions = lr_model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="delivery_time_hours", predictionCol="prediction", metricName="rmse")
    
    rmse = evaluator.evaluate(predictions)
    r2 = lr_model.summary.r2

    print(f"Trọng số (Hệ số tác động của Mưa, Gió, Khoảng cách): {lr_model.coefficients}")
    print(f"Hằng số (Intercept): {lr_model.intercept:.4f}")
    print(f"Độ lỗi dự báo (RMSE): {rmse:.4f} giờ")
    print(f"Mức độ giải thích mô hình (R-squared): {r2:.4f}")
    
    spark.stop()

if __name__ == "__main__":
    main()
