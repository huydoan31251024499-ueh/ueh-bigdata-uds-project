from pyspark.sql.types import *


# 1. Schema nghiêm ngặt cho luồng dữ liệu đơn hàng (order_stream)
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("createdAt", StringType()),  # ✅ FIX
    StructField("distance_km", DoubleType()),
    StructField("traffic_congestion_index", DoubleType()),
    StructField("weight", DoubleType()),
    StructField("sender_lat", DoubleType()),
    StructField("sender_lng", DoubleType()),
    StructField("receiver_lat", DoubleType()),
    StructField("receiver_lng", DoubleType()),
    StructField("serviceType", StringType())
])

# 2. Schema nghiêm ngặt cho luồng dữ liệu thời tiết thực tế (weather_realtime)
weather_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("prcp_mm", DoubleType(), True),
    StructField("temp", DoubleType(), True),
    StructField("wspd_kmh", DoubleType(), True),
    StructField("condition_label", StringType(), True)
])

flood_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("depth_cm", DoubleType(), True)
])
