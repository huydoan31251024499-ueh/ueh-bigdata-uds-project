from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, round as spark_round,
    when, lit, hour, dayofweek, month
)
from pyspark.sql.types import DoubleType

# KHOI TAO SPARK SESSION
spark = SparkSession.builder \
    .appName("UDS_Join_FeatureEngineering_v2") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HDFS_PROCESSED = "hdfs://namenode:9000/uds/data/processed"
HDFS_JOINED    = "hdfs://namenode:9000/uds/data/joined"


# STEP 1: DOC DU LIEU DA CLEANED (output tu process_pipeline_v2.py)

print("=" * 60)
print("STEP 1: Doc Parquet da cleaned")
print("=" * 60)

df_weather = spark.read.parquet(f"{HDFS_PROCESSED}/weather_clean")
df_orders  = spark.read.parquet(f"{HDFS_PROCESSED}/orders_clean")

print(f"Weather clean: {df_weather.count()} dong")
print(f"Orders clean : {df_orders.count()} dong")

print("Weather schema:")
df_weather.printSchema()
print("Orders schema:")
df_orders.printSchema()


# STEP 2: TEMPORAL JOIN theo hour_key
# Join Key: date_trunc("hour", createdAt) == date_trunc("hour", timestamp)

print("=" * 60)
print("STEP 2: Temporal JOIN Weather x Orders")
print("=" * 60)

# Doi ten hour_key cua weather truoc khi join de tranh ambiguous
df_weather_j = df_weather.withColumnRenamed("hour_key", "weather_hour_key")

# Left Join: giu toan bo don hang, ke ca khi khong co du lieu thoi tiet
df_joined = df_orders.join(
    df_weather_j,
    df_orders["hour_key"] == df_weather_j["weather_hour_key"],
    how="left"
).drop("weather_hour_key")

total_joined = df_joined.count()
missing_weather = df_joined.filter(col("temp_c").isNull()).count()
print(f"Sau JOIN         : {total_joined} dong")
print(f"Don thieu thoi tiet: {missing_weather} dong")


# STEP 3: FEATURE ENGINEERING

print("=" * 60)
print("STEP 3: Feature Engineering")
print("=" * 60)

#  3.1 Thoi gian giao hang thuc te (phut) 
df_feat = df_joined.withColumn(
    "actual_duration_min",
    spark_round(
        (unix_timestamp(col("deliveredAt")) - unix_timestamp(col("createdAt"))) / 60.0, 2
    )
)

#  3.2 Thoi gian giao hang ky vong (phut) 
df_feat = df_feat.withColumn(
    "expected_duration_min",
    spark_round(
        (unix_timestamp(col("expectedDeliveryTime")) - unix_timestamp(col("createdAt"))) / 60.0, 2
    )
)

#  3.3 Do tre giao hang (phut)  duong = tre, am = som 
df_feat = df_feat.withColumn(
    "delay_min",
    spark_round(col("actual_duration_min") - col("expected_duration_min"), 2)
)

#  3.4 Nhan tre (label) 
df_feat = df_feat.withColumn(
    "is_late",
    when(col("delay_min") > 0, lit(1)).otherwise(lit(0))
)

#  3.5 Khoang cach km (da co san tu process_pipeline_v2) 
df_feat = df_feat.withColumn(
    "distance_km",
    spark_round(col("shippingDistance_km"), 3)
)

#  3.6 Loc outlier khoang cach (> 200km -> sai GPS) 
df_feat = df_feat.filter(col("distance_km") <= 200.0)

#  3.7 Temporal features tu thoi diem tao don 
df_feat = df_feat \
    .withColumn("order_hour",  hour(col("createdAt"))) \
    .withColumn("order_dow",   dayofweek(col("createdAt"))) \
    .withColumn("order_month", month(col("createdAt")))

#  3.8 Phan loai ca giao hang theo gio 
df_feat = df_feat.withColumn(
    "time_slot",
    when((col("order_hour") >= 6)  & (col("order_hour") < 11), lit("sang"))
    .when((col("order_hour") >= 11) & (col("order_hour") < 13), lit("trua"))
    .when((col("order_hour") >= 13) & (col("order_hour") < 18), lit("chieu"))
    .when((col("order_hour") >= 18) & (col("order_hour") < 22), lit("toi"))
    .otherwise(lit("dem"))
)

#  3.9 Muc do mua (rain_level) theo WMO 
# Dung ten cot moi: prcp_mm (tu process_pipeline_v2)
df_feat = df_feat.withColumn(
    "rain_level",
    when(col("prcp_mm").isNull() | (col("prcp_mm") == 0), lit("no_rain"))
    .when(col("prcp_mm") < 2.5,                           lit("light"))
    .when(col("prcp_mm") < 7.5,                           lit("moderate"))
    .otherwise(                                            lit("heavy"))
)

#  3.10 Chi so cam giac nong (Heat Index) 
# Dung ten cot moi: temp_c, rhum_pct (tu process_pipeline_v2)
df_feat = df_feat.withColumn(
    "heat_index",
    spark_round(
        -8.78
        + 1.61  * col("temp_c")
        + 2.34  * (col("rhum_pct") / 100.0)
        - 0.146 * col("temp_c") * (col("rhum_pct") / 100.0),
        2
    )
)

#  3.11 Toc do gio - phan loai Beaufort 
# Dung ten cot moi: wspd_kmh (tu process_pipeline_v2)
df_feat = df_feat.withColumn(
    "wind_category",
    when(col("wspd_kmh") < 1.5,  lit("calm"))
    .when(col("wspd_kmh") < 5.5,  lit("light_breeze"))
    .when(col("wspd_kmh") < 10.8, lit("gentle_breeze"))
    .when(col("wspd_kmh") < 17.2, lit("moderate_breeze"))
    .otherwise(                    lit("strong_wind"))
)

#  3.12 Co thoi tiet cuc doan (tu code nhom truong) 
extreme_conditions = ["Heavy Rain", "Thunderstorm", "Heavy Thunderstorm", "Storm"]
df_feat = df_feat.withColumn(
    "is_extreme_weather",
    when(
        (col("prcp_mm") > 5) | (col("condition_label").isin(extreme_conditions)),
        lit(1)
    ).otherwise(lit(0))
)

# STEP 4: GHI OUTPUT PHAN VUNG THEO THANG

print("=" * 60)
print("STEP 4: Ghi Parquet phan vung theo thang")
print("=" * 60)

df_feat.write \
    .partitionBy("order_month") \
    .mode("overwrite") \
    .parquet(f"{HDFS_JOINED}/uds_logistics_features")

total_final = df_feat.count()
total_late  = df_feat.filter(col("is_late") == 1).count()
total_extreme = df_feat.filter(col("is_extreme_weather") == 1).count()

print(f"Tong dong hop le       : {total_final}")
print(f"Don tre (is_late=1)    : {total_late} ({round(total_late/total_final*100,1)}%)")
print(f"Thoi tiet cuc doan     : {total_extreme} ({round(total_extreme/total_final*100,1)}%)")
print(f"Da ghi -> {HDFS_JOINED}/uds_logistics_features")

# STEP 5: QUICK STATS

print("=" * 60)
print("STEP 5: Quick Stats")
print("=" * 60)

print("-- Delay trung binh theo rain_level --")
df_feat.groupBy("rain_level") \
    .agg({"delay_min": "avg", "is_late": "avg"}) \
    .withColumnRenamed("avg(delay_min)", "avg_delay_min") \
    .withColumnRenamed("avg(is_late)",   "late_rate") \
    .orderBy("rain_level") \
    .show()

print("-- Delay trung binh theo time_slot --")
df_feat.groupBy("time_slot") \
    .agg({"delay_min": "avg", "is_late": "avg"}) \
    .withColumnRenamed("avg(delay_min)", "avg_delay_min") \
    .withColumnRenamed("avg(is_late)",   "late_rate") \
    .orderBy("time_slot") \
    .show()

print("-- Don hang theo condition_label --")
df_feat.groupBy("condition_label") \
    .agg({"is_late": "avg", "delay_min": "avg"}) \
    .withColumnRenamed("avg(is_late)",   "late_rate") \
    .withColumnRenamed("avg(delay_min)", "avg_delay_min") \
    .orderBy("late_rate", ascending=False) \
    .show()

print("join_feature_pipeline_v2.py HOAN TAT")
spark.stop()