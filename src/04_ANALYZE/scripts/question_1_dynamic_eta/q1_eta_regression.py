"""
UDS – SMART Q1
Dynamic ETA Regression with Extended Features
(Academic & Applied Version)

Scope:
- ETA prediction is applied ONLY to SLA-critical services: 3h, 5h
- Truck services (ban_tai_*) and storage (luu_kho) are excluded
- Objective: evaluate whether temporal, weather, flood, and market
  features can improve ETA prediction under real operating conditions

Key Assumption:
- High RMSE reflects operational instability if SLA-violating orders exist
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


# ==================================================
# 1. Spark Session
# ==================================================
spark = SparkSession.builder \
    .appName("UDS_Q1_Dynamic_ETA_Extended_Features") \
    .getOrCreate()


# ==================================================
# 2. Load Data
# ==================================================
df = spark.read.parquet(
    "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed/final_features"
)

# Distance normalization
df = df.withColumn(
    "shippingDistance_km",
    col("shippingDistance") / 1000.0
)


# ==================================================
# 3. PROCESS – Data Filtering (Business Logic)
# ==================================================

# 3.1 Remove extreme operational outliers (> 24h)
df = df.filter(col("actual_duration_min") <= 1440)

# 3.2 Keep ONLY SLA-critical services
SLA_SERVICES = ["3h", "5h"]
df = df.filter(col("serviceType").isin(SLA_SERVICES))


# ==================================================
# 4. PROCESS – Feature Engineering
# ==================================================

# ---- Temporal features ----
df = df.withColumn("is_peak_hour",
    when(
        (col("order_hour").between(7, 9)) |
        (col("order_hour").between(16, 19)),
        1
    ).otherwise(0)
)

# ---- Weather risk encoding ----
df = df.withColumn("is_heavy_rain",
    when(col("condition_label").isin("Heavy Rain", "Thunderstorm"), 1)
    .otherwise(0)
)

# ---- Flood encoding ----
df = df.withColumn("has_flood",
    when(col("flood_count") > 0, 1).otherwise(0)
)


# ==================================================
# 5. Feature Set Definition
# ==================================================
FEATURE_COLS = [
    # Distance & load
    "shippingDistance_km",
    "weight",

    # Temporal
    "order_hour",
    "order_dow",
    "is_peak_hour",

    # Traffic & market
    "traffic_congestion_index",
    "avg_vehicle_speed_kmh",
    "active_delivery_vehicles",

    # Weather
    "prcp_mm",
    "wspd_kmh",
    "is_heavy_rain",

    # Flood
    "flood_avg_depth_cm",
    "has_flood"
]


assembler = VectorAssembler(
    inputCols=FEATURE_COLS,
    outputCol="raw_features",
    handleInvalid="skip"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)


# ==================================================
# 6. Train & Evaluate per ServiceType
# ==================================================
def train_and_evaluate(service_type: str):
    print(f"\n===== ETA MODEL FOR SERVICE TYPE: {service_type} =====")

    service_df = df.filter(col("serviceType") == service_type)

    df_ml = assembler.transform(service_df)
    df_ml = scaler.fit(df_ml).transform(df_ml)

    df_ml = df_ml.select(
        col("features"),
        col("actual_duration_min").alias("label")
    )

    if df_ml.count() < 50:
        print("⚠️ Not enough data for reliable training. Skipping.")
        return

    train_df, test_df = df_ml.randomSplit([0.7, 0.3], seed=42)

    model = LinearRegression(
        featuresCol="features",
        labelCol="label"
    ).fit(train_df)

    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse"
    )

    rmse = evaluator.evaluate(predictions)

    print(f"✅ RMSE ({service_type}): {rmse:.2f} minutes")


# ==================================================
# 7. Run Experiments
# ==================================================
for st in SLA_SERVICES:
    train_and_evaluate(st)


# ==================================================
# 8. Stop Spark
# ==================================================
spark.stop()