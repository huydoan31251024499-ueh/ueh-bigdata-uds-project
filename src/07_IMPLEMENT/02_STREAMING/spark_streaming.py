import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, date_format
from pyspark.sql.types import StructType

# Thêm đường dẫn Desktop để Python tìm thấy schemas.py
sys.path.append("/home/dntt/Desktop")
from schemas import order_schema

# 1. Khởi tạo Spark Session cấu hình Real-time
spark = SparkSession.builder \
    .appName("RealTime_ModelGating_Inference") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Đọc luồng dữ liệu Unbounded từ thư mục phân vùng
input_dir = "file:///home/dntt/Desktop/stream_input"

stream_df = spark.readStream \
    .schema(order_schema) \
    .json(input_dir) # Ép kiểu cấu trúc Schema-on-read trực tiếp

# LƯU Ý KỸ THUẬT: Đọc giả lập thêm thông tin thời tiết đi kèm (Đã Join ở tầng Producer)
from schemas import weather_schema
full_stream_df = spark.readStream \
    .schema(StructType(order_schema.fields + weather_schema.fields)) \
    .json(input_dir)

# 3. ON-THE-FLY FEATURE ENGINEERING (Tính toán đặc trưng phi tuyến trên RAM)
# Bình phương chỉ số tắc nghẽn để tạo hình phạt phạt thời gian (traffic_penalty)
processed_stream = full_stream_df.withColumn(
    "traffic_penalty", col("traffic_congestion_index") * col("traffic_congestion_index")
)

# 4. CƠ CHẾ MODEL GATING (Điều hướng nhánh trọng số mô hình động)
# Trọng số nhánh bình thường: Hệ số khoảng cách = 2.5, Phạt giao thông = 0.05
# Trọng số nhánh thiên tai: Hệ số khoảng cách = 4.0, Phạt mưa ngập tích hợp = 1.2
adaptive_eta_df = processed_stream.withColumn(
    "weather_adaptive_eta",
    when(
        col("avg_flood_depth_cm") > 15.0, 
        (col("distance_km") * 4.0) + (col("traffic_penalty") * 0.008) + (col("prcp_mm") * 0.6) + (col("avg_flood_depth_cm") * 0.9)
    ).otherwise(
        (col("distance_km") * 2.5) + (col("traffic_penalty") * 0.003)
    )
)

# 5. Đóng gói kết quả payload đầu ra chuẩn hóa bàn giao
output_stream = adaptive_eta_df.select(
    col("id").alias("order_id"),
    col("condition_label").alias("current_context"),
    col("distance_km").alias("distance"),
    (col("distance_km") * 2.5).alias("original_eta"), # ETA gốc trong điều kiện lý tưởng
    col("weather_adaptive_eta").alias("weather_adaptive_eta") # ETA co giãn động theo thời gian thực
)

# 6. Đẩy luồng dữ liệu ra Console (Phục vụ lấy minh chứng log kiểm thử)
query = output_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print("=== SYSTEM ACTIVE: SPARK STREAMING & MODEL GATING RUNNING ===")
query.awaitTermination()
