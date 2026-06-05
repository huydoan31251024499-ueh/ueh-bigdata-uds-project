from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

# 1. Schema nghiêm ngặt cho luồng dữ liệu đơn hàng (order_stream)
order_schema = StructType([
    StructField("id", StringType(), True),
    StructField("createdAt", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("traffic_congestion_index", DoubleType(), True),
    StructField("order_hour", IntegerType(), True)
])

# 2. Schema nghiêm ngặt cho luồng dữ liệu thời tiết thực tế (weather_realtime)
weather_schema = StructType([
    StructField("hour_timestamp", StringType(), True),
    StructField("prcp_mm", DoubleType(), True),
    StructField("avg_flood_depth_cm", DoubleType(), True),
    StructField("condition_label", StringType(), True)
])
