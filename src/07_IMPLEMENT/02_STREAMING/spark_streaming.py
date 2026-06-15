"""
Spark Structured Streaming - UDS Real-Time ETA System (FINAL ML CLEAN)

Pipeline:
Kafka → Parse → Watermark → Stream-Stream Join → Feature → ML ETA → Kafka
"""
import sys
sys.path.append("/app/src/07_IMPLEMENT/02_STREAMING")
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct,
    when, expr, to_timestamp, lit
)
from pyspark.sql.functions import to_timestamp
from schemas import order_schema, weather_schema
from pyspark.ml import PipelineModel

# ✅ LOAD MODEL (giữ nguyên vị trí đầu)
model_3h = PipelineModel.load("hdfs://namenode:9000/user/spark/models/eta/3h_normal")
model_5h = PipelineModel.load("hdfs://namenode:9000/user/spark/models/eta/5h_normal")

# ========================================
# 0. SPARK SESSION
# ========================================
spark = SparkSession.builder \
    .appName("UDS_ETA_Streaming_FINAL_CLEAN") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_SERVER = "kafka:9092"

# ========================================
# 1. ORDER STREAM (GIỮ NGUYÊN)
# ========================================
raw_order = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", "order_stream") \
    .option("startingOffsets", "latest") \
    .load()

order_df = raw_order.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), order_schema).alias("data")) \
    .select(
        col("data.*"),
        lit(1).alias("join_key")
    )

from pyspark.sql.functions import to_timestamp

order_df = order_df.withColumn(
    "createdAt",
    to_timestamp("createdAt")
)

order_df = order_df.withWatermark("event_time", "10 minutes")

# ========================================
# 2. WEATHER STREAM (GIỮ NGUYÊN)
# ========================================
raw_weather = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", "weather_realtime") \
    .option("startingOffsets", "latest") \
    .load()

weather_df = raw_weather.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select(
        col("data.*"),
        lit(1).alias("join_key")
    )

weather_df = weather_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

weather_df = weather_df.withWatermark("event_time", "10 minutes")

# ========================================
# 3. ✅ JOIN (FIX NHẸ: MỞ RỘNG WINDOW)
# ========================================
joined_df = order_df.alias("o").join(
    weather_df.alias("w"),
    (
        (col("o.join_key") == col("w.join_key")) &
        (col("o.event_time") >= col("w.event_time") - expr("INTERVAL 10 MINUTES")) &   # ✅ FIX QUAN TRỌNG
        (col("o.event_time") <= col("w.event_time") + expr("INTERVAL 10 MINUTES"))
    )
)

joined_df = joined_df.select(
    col("o.order_id"),
    col("o.distance_km"),
    col("o.traffic_congestion_index"),
    col("o.serviceType"),
    col("o.event_time"),
    col("o.join_key"),
    col("w.prcp_mm")
)

# ========================================
# 4. ✅ FEATURE ENGINEERING (FIX MATCH OFFLINE)
# ========================================
df = joined_df.withColumn(
    "traffic_penalty",
    col("traffic_congestion_index") * col("traffic_congestion_index")
)

df = df.withColumn(
    "is_peak_hour",
    when(
        (expr("hour(event_time) BETWEEN 7 AND 9") |
         expr("hour(event_time) BETWEEN 16 AND 19")),
        1
    ).otherwise(0)
)

# ✅ QUAN TRỌNG: rename để MATCH offline model
df = df.withColumnRenamed("distance_km", "shipping_km")

# ✅ thêm feature mà model cần
df = df.withColumn("flood_severity", lit(0.0))
df = df.withColumn("flood_peak_interaction", lit(0))

# ========================================
# 5. CONTEXT (GIỮ NGUYÊN - không bắt buộc ML)
# ========================================
df = df.withColumn(
    "context",
    when(col("prcp_mm") > 10, "rain")
    .otherwise("normal")
)

# ========================================
# 6. ✅ ETA MODEL (THAY NGUYÊN BLOCK NÀY)
# ========================================

# ❌ XOÁ toàn bộ rule-based cũ

# ✅ SPLIT theo service
df_3h = df.filter(col("serviceType") == "3h")
df_5h = df.filter(col("serviceType") == "5h")

# ✅ APPLY MODEL
pred_3h = model_3h.transform(df_3h)
pred_5h = model_5h.transform(df_5h)

df = pred_3h.unionByName(pred_5h)

# ✅ OUTPUT ETA từ prediction
df = df.withColumn("eta_min", col("prediction"))

# ✅ cleanup
df = df.drop("prediction", "join_key")

# ========================================
# 7. OUTPUT Kafka (GIỮ NGUYÊN)
# ========================================
output_df = df.select(
    to_json(struct(
        col("order_id"),
        col("serviceType"),
        col("shipping_km"),
        col("eta_min"),
        col("context")
    )).alias("value")
)

query_kafka = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("topic", "uds-predicted-eta") \
    .option("checkpointLocation", "/app/checkpoints/eta_final") \
    .outputMode("append") \
    .start()

# ========================================
# 8. DEBUG CONSOLE (GIỮ NGUYÊN)
# ========================================
query_console = df.select(
    "order_id",
    "context",
    "eta_min"
).writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✅ SYSTEM RUNNING: ML STREAMING FINAL ✅")

spark.streams.awaitAnyTermination()