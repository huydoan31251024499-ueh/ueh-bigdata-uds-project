import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_json, struct
from pyspark.sql.types import StructType

sys.path.append("/home/dntt/Desktop")
from schemas import order_schema, weather_schema

spark = SparkSession.builder \
    .appName("RealTime_Kafka_Inference") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

input_dir = "file:///home/dntt/Desktop/stream_input"

full_stream_df = spark.readStream \
    .schema(StructType(order_schema.fields + weather_schema.fields)) \
    .json(input_dir)

# Tính toán đặc trưng phi tuyến và Model Gating
processed_stream = full_stream_df.withColumn("traffic_penalty", col("traffic_congestion_index") * col("traffic_congestion_index"))

adaptive_eta_df = processed_stream.withColumn(
    "weather_adaptive_eta",
    when(col("avg_flood_depth_cm") > 15.0, 
         (col("distance_km") * 4.0) + (col("traffic_penalty") * 0.008) + (col("prcp_mm") * 0.6) + (col("avg_flood_depth_cm") * 0.9)
    ).otherwise((col("distance_km") * 2.5) + (col("traffic_penalty") * 0.003))
)

# 5. CHUẨN HÓA ĐẦU RA THEO ĐÚNG ĐẶC TẢ KAFKA (Bắt buộc phải có cột 'value' kiểu String)
# Gộp các trường lại thành cấu trúc JSON payload để bàn giao cho Tiên
kafka_output_df = adaptive_eta_df.select(
    to_json(struct(
        col("id").alias("order_id"),
        col("distance_km").alias("distance"),
        (col("distance_km") * 2.5).alias("original_eta"),
        col("weather_adaptive_eta").alias("weather_adaptive_eta"),
        col("condition_label").alias("current_context")
    )).alias("value")
)

# 6. ĐẨY DỮ LIỆU ĐỘNG VÀO KAFKA TOPIC SẠCH 'uds-predicted-eta'
query_kafka = kafka_output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "uds-predicted-eta") \
    .option("checkpointLocation", "file:///home/dntt/Desktop/kafka_checkpoint") \
    .start()

# Đồng thời in ra màn hình máy của Tú để chụp ảnh báo cáo bài tiểu luận
query_console = adaptive_eta_df.select(
    col("id").alias("order_id"),
    col("condition_label").alias("context"),
    col("weather_adaptive_eta").alias("adaptive_eta")
).writeStream.format("console").start()

print("=== SYSTEM ACTIVE: SPARK STREAMING IS PUSHING TO KAFKA TOPIC ===")
spark.streams.awaitAnyTermination()
