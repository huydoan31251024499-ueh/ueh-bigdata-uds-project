from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import (
    col, date_trunc, to_timestamp, regexp_replace,
    from_utc_timestamp, coalesce, create_map, lit,
    unix_timestamp, from_unixtime, round as spark_round, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
import logging

# LOGGER
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# KHOI TAO SPARK SESSION
spark = SparkSession.builder \
    .appName("UDS_Process_Pipeline_v2") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# DUONG DAN HDFS
HDFS_RAW       = "hdfs://namenode:9000/uds/data/raw"
HDFS_PROCESSED = "hdfs://namenode:9000/uds/data/processed"

# MAPPING MA THOI TIET -> NHAN
condition_map = {
    1: "Clear", 2: "Fair", 3: "Cloudy", 4: "Overcast",
    5: "Foggy", 6: "Freezing Fog", 7: "Light Rain", 8: "Rain",
    9: "Heavy Rain", 10: "Freezing Rain", 11: "Heavy Freezing Rain",
    12: "Sleet", 13: "Heavy Sleet", 14: "Light Snowfall",
    15: "Snowfall", 16: "Heavy Snowfall", 17: "Rain Shower",
    18: "Heavy Rain Shower", 19: "Sleet Shower", 20: "Heavy Sleet Shower",
    21: "Snow Shower", 22: "Heavy Snow Shower", 23: "Lightning",
    24: "Hail", 25: "Thunderstorm", 26: "Heavy Thunderstorm", 27: "Storm",
}
mapping_expr = create_map(*[lit(x) for x in sum(condition_map.items(), ())])


# STEP 1: WEATHER TRANSFORMATION

print("=" * 60)
print("STEP 1: Weather Transformation")
print("=" * 60)

# Schema-on-read: Dinh nghia StructType de toi uu performance (Veracity)
weather_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("temp",      DoubleType(),    True),
    StructField("rhum",      IntegerType(),   True),
    StructField("prcp",      DoubleType(),    True),
    StructField("wdir",      IntegerType(),   True),
    StructField("wspd",      DoubleType(),    True),
    StructField("cldc",      IntegerType(),   True),
    StructField("pres",      DoubleType(),    True),
    StructField("coco",      IntegerType(),   True),
])

df_weather = spark.read.option("header", "true") \
    .csv(f"{HDFS_RAW}/hcmc_weather_raw.csv", schema=weather_schema)

print(f"Weather raw: {df_weather.count()} dong")
df_weather.printSchema()
df_weather.show(3, truncate=False)

# Chuyen UTC -> UTC+7 (Asia/Ho_Chi_Minh)
df_weather = df_weather.withColumn(
    "timestamp",
    from_utc_timestamp(col("timestamp"), "Asia/Ho_Chi_Minh")
)

# Them nhan dieu kien thoi tiet tu ma coco
df_weather = df_weather.withColumn(
    "condition_label",
    coalesce(mapping_expr[col("coco").cast(IntegerType())], lit("Unknown"))
)

# fillna(0) cho cac cot luong mua va may (MMDS Cleaning)
df_weather_clean = df_weather \
    .withColumn("prcp", coalesce(col("prcp"), lit(0.0))) \
    .withColumn("cldc", coalesce(col("cldc"), lit(0))) \
    .withColumn("wdir", coalesce(col("wdir"), lit(0))) \
    .withColumn("wspd", coalesce(col("wspd"), lit(0.0)))

# cast(DoubleType()) tuong minh cho cac chi so khi tuong (MMDS Transformation)
df_weather_clean = df_weather_clean \
    .withColumn("temp", col("temp").cast(DoubleType())) \
    .withColumn("rhum", col("rhum").cast(DoubleType())) \
    .withColumn("prcp", col("prcp").cast(DoubleType())) \
    .withColumn("wdir", col("wdir").cast(DoubleType())) \
    .withColumn("wspd", col("wspd").cast(DoubleType())) \
    .withColumn("cldc", col("cldc").cast(DoubleType())) \
    .withColumn("pres", col("pres").cast(DoubleType()))

# Rename cot kem don vi
df_weather_clean = df_weather_clean \
    .withColumnRenamed("temp", "temp_c") \
    .withColumnRenamed("rhum", "rhum_pct") \
    .withColumnRenamed("prcp", "prcp_mm") \
    .withColumnRenamed("wdir", "wdir_deg") \
    .withColumnRenamed("wspd", "wspd_kmh") \
    .withColumnRenamed("cldc", "cldc_pct") \
    .withColumnRenamed("pres", "pres_hpa") \
    .withColumnRenamed("coco", "coco_code")

# Temporal Join Key: date_trunc("hour") de khop voi don hang
df_weather_clean = df_weather_clean.withColumn(
    "hour_timestamp",
    date_trunc("hour", col("timestamp"))
)

print(f"Weather clean: {df_weather_clean.count()} dong")
print("Sample weather transformed:")
df_weather_clean.show(5, truncate=False)


# STEP 2: ORDERS TRANSFORMATION

print("=" * 60)
print("STEP 2: Orders Transformation")
print("=" * 60)

orders_schema = StructType([
    StructField("id",                   StringType(), True),
    StructField("createdAt",            StringType(), True),
    StructField("deliveredAt",          StringType(), True),
    StructField("expectedDeliveryTime", StringType(), True),
    StructField("mdh",                  StringType(), True),
    StructField("package_name",         StringType(), True),
    StructField("orderStatus",          StringType(), True),
    StructField("senderAddress",        StringType(), True),
    StructField("senderLat",            DoubleType(), True),
    StructField("senderLng",            DoubleType(), True),
    StructField("receiverAddress",      StringType(), True),
    StructField("receiverLat",          DoubleType(), True),
    StructField("receiverLng",          DoubleType(), True),
    StructField("shippingDistance",     DoubleType(), True),
    StructField("shipper",              StringType(), True),
    StructField("weight",               DoubleType(), True),
    StructField("serviceType",          StringType(), True),
    StructField("image",                StringType(), True),
])

df_orders = spark.read.option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .csv(f"{HDFS_RAW}/uds_orders.csv", schema=orders_schema)

print(f"Orders raw: {df_orders.count()} dong")
df_orders.show(3, truncate=False)

# Loc toa do GPS hop le vung TP.HCM
df_orders_clean = df_orders.filter(
    col("senderLat").between(10.4, 11.2)   &
    col("senderLng").between(106.3, 107.1) &
    col("receiverLat").between(10.4, 11.2) &
    col("receiverLng").between(106.3, 107.1)
)

# Xu ly NULL -- xoa cac dong thieu du lieu quan trong
df_orders_clean = df_orders_clean \
    .filter(col("shipper").isNotNull()) \
    .filter(col("deliveredAt").isNotNull()) \
    .filter(col("expectedDeliveryTime").isNotNull())

# regexp_replace: loai bo ky tu rac trong dia chi (MMDS Cleaning)
df_orders_clean = df_orders_clean \
    .withColumn("senderAddress",
                regexp_replace(col("senderAddress"), r"[^\w\s,./]", "")) \
    .withColumn("receiverAddress",
                regexp_replace(col("receiverAddress"), r"[^\w\s,./]", ""))

# Parse timestamps + chuyen UTC -> UTC+7
df_orders_clean = df_orders_clean \
    .withColumn("createdAt",
                from_utc_timestamp(
                    to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                    "Asia/Ho_Chi_Minh")) \
    .withColumn("deliveredAt",
                from_utc_timestamp(
                    to_timestamp(col("deliveredAt"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                    "Asia/Ho_Chi_Minh")) \
    .withColumn("expectedDeliveryTime",
                from_utc_timestamp(
                    to_timestamp(col("expectedDeliveryTime"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                    "Asia/Ho_Chi_Minh"))

# Dong bo don vi: Distance m->km, Weight cast DoubleType (MMDS Transformation)
df_orders_clean = df_orders_clean \
    .withColumn("shippingDistance_km",
                col("shippingDistance").cast(DoubleType()) / 1000.0) \
    .withColumn("weight_kg",
                col("weight").cast(DoubleType()))

# Temporal Join Key: date_trunc("hour", createdAt) khop voi weather
df_orders_clean = df_orders_clean.withColumn(
    "hour_timestamp",
    from_unixtime(
    spark_round(unix_timestamp(col("createdAt")) / 3600) * 3600
).cast(TimestampType())
)

print(f"Orders clean: {df_orders_clean.count()} dong")
print(f"  (da loai : {df_orders.count() - df_orders_clean.count()} dong NULL/GPS loi)")
print("Sample orders transformed:")
df_orders_clean.show(5, truncate=False)


# STEP 3: GHI KET QUA RA HDFS DANG PARQUET

print("=" * 60)
print("STEP 3: Ghi Parquet -> HDFS")
print("=" * 60)

df_weather_clean.write.mode("overwrite").parquet(f"{HDFS_PROCESSED}/weather_clean")
df_orders_clean.write.mode("overwrite").parquet(f"{HDFS_PROCESSED}/orders_clean")

print(f"Da ghi weather_clean -> {HDFS_PROCESSED}/weather_clean")
print(f"Da ghi orders_clean  -> {HDFS_PROCESSED}/orders_clean")
print("process_pipeline_v2.py HOAN TAT")

spark.stop()