from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, col, round

# ==============================
# SPARK SESSION
# ==============================
spark = SparkSession.builder \
    .appName("UDS_Q1_Delay_EDA") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# ==============================
# LOAD FINAL FEATURES
# ==============================
HDFS_FINAL = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed/final_features"

df = spark.read.parquet(HDFS_FINAL)

# ==============================
# EDA 1: Delay theo rain_level
# ==============================
rain_delay_df = (
    df.groupBy("rain_level")
      .agg(
          count("*").alias("total_orders"),
          round(avg("delay_min"), 2).alias("avg_delay_min"),
          round((sum(col("is_late")) / count("*")), 3).alias("late_rate")
      )
      .orderBy(col("avg_delay_min").desc())
)

print("=== Delay theo rain_level ===")
rain_delay_df.show(truncate=False)

# ==============================
# EDA 2: Delay khi có ngập
# ==============================
flood_delay_df = (
    df.groupBy("is_flooded")
      .agg(
          count("*").alias("total_orders"),
          round(avg("delay_min"), 2).alias("avg_delay_min")
      )
)

print("=== Delay khi có ngập ===")
flood_delay_df.show(truncate=False)

# ==============================
# STOP SPARK
# ==============================
spark.stop()