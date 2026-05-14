"""
Q1 – Improved ETA Regression
----------------------------------
Objective:
- Improve ETA prediction accuracy (RMSE) using EXISTING data only
- Focus on:
  (1) Data segmentation by operational context
  (2) Explainable feature engineering (thresholds & interaction terms)
  (3) Context-aware vector representation
  (4) SLA-compliant training for controlled RMSE

Target:
- Services: 3h, 5h only
- Label: actual_duration_min
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# =========================================================
# 1. Spark Session
# =========================================================
spark = SparkSession.builder \
    .appName("UDS_Q1_ETA_Improved_Model") \
    .getOrCreate()

# =========================================================
# 2. Load FINAL dataset (CSV)
# =========================================================
df = spark.read.parquet(
    "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed/final_features"
)

# =========================================================
# 3. BUSINESS FILTERING (CLEAN DATA)
# =========================================================
# 3.1 Only SLA-critical services
df = df.filter(col("serviceType").isin("3h", "5h"))

# 3.2 Remove extreme operational failures (> 24h)
df = df.filter(col("actual_duration_min") <= 1440)

# 3.3 Normalize distance (meters → km)
df = df.withColumn("shipping_km", col("shippingDistance") / 1000.0)

# =========================================================
# 4. FEATURE ENGINEERING (EXPLAINABLE)
# =========================================================

# ---- Temporal context ----
df = df.withColumn(
    "is_peak_hour",
    when(
        (col("order_hour").between(7, 9)) |
        (col("order_hour").between(16, 19)),
        1
    ).otherwise(0)
)

# ---- Weather threshold encoding ----
# Threshold rationale:
# From EDA: delay increases sharply when prcp_mm > 5
df = df.withColumn(
    "is_heavy_rain",
    when(col("prcp_mm") > 5, 1).otherwise(0)
)

# ---- Flood encoding ----
df = df.withColumn(
    "has_flood",
    when(col("flood_avg_depth_cm") > 0, 1).otherwise(0)
)

# ---- Traffic non-linearity ----
# Rationale:
# Traffic impact is non-linear (gridlock much worse than moderate congestion)
df = df.withColumn(
    "traffic_penalty",
    col("traffic_congestion_index") * col("traffic_congestion_index")
)

# ---- Interaction feature (context amplification) ----
# Rationale:
# Flood + peak hour amplifies delay disproportionately
df = df.withColumn(
    "flood_peak_interaction",
    col("has_flood") * col("is_peak_hour")
)

# =========================================================
# 5. DATA SEGMENTATION (KEY RMSE IMPROVEMENT)
# =========================================================
# Segment by operational context
df = df.withColumn(
    "context_segment",
    when((col("is_heavy_rain") == 0) & (col("has_flood") == 0), "normal")
    .when((col("is_heavy_rain") == 1) & (col("has_flood") == 0), "rain_only")
    .otherwise("rain_flood")
)

# =========================================================
# 6. SLA-COMPLIANT SUBSET (CONTROLLED RMSE)
# =========================================================
# Motivation:
# RMSE explodes if extreme SLA violations are mixed into training

df = df.withColumn(
    "sla_compliant",
    when(
        ((col("serviceType") == "3h") & (col("actual_duration_min") <= 240)) |
        ((col("serviceType") == "5h") & (col("actual_duration_min") <= 360)),
        1
    ).otherwise(0)
)

# =========================================================
# 7. FEATURE REPRESENTATION (WEIGHTED VECTOR)
# =========================================================
FEATURE_COLS = [
    # Distance & load
    "shipping_km",
    "weight",

    # Temporal
    "order_hour",
    "is_peak_hour",

    # Weather
    "prcp_mm",
    "is_heavy_rain",

    # Flood
    "flood_avg_depth_cm",
    "has_flood",
    "flood_peak_interaction",

    # Traffic
    "avg_vehicle_speed_kmh",
    "traffic_penalty",

    # Market
    "active_delivery_vehicles"
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

# =========================================================
# 8. TRAINING FUNCTION (PER SERVICE & CONTEXT)
# =========================================================
def train_model(service, context):
    print(f"\n=== ETA MODEL | Service={service} | Context={context} ===")

    subset = df.filter(
        (col("serviceType") == service) &
        (col("context_segment") == context) &
        (col("sla_compliant") == 1)
    )

    raw_count = subset.count()
    print(f"Raw records after business filters: {raw_count}")

    if raw_count < 50:
        print("⚠️ Not enough raw records, skipped.")
        return

    # Assemble first
    assembled = assembler.transform(subset)

    # Count valid rows AFTER assembler (handleInvalid=skip)
    valid_count = assembled.select("raw_features").count()
    print(f"Valid records after feature assembling: {valid_count}")

    if valid_count < 30:
        print("⚠️ Not enough valid feature rows, skipped.")
        return

    # Fit scaler SAFELY
    scaler_model = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True
    ).fit(assembled)

    data = scaler_model.transform(assembled)

    train, test = data.randomSplit([0.8, 0.2], seed=42)

    lr = LinearRegression(
        featuresCol="features",
        labelCol="actual_duration_min",
        maxIter=50,
        regParam=0.1,
        elasticNetParam=0.5
    )

    model = lr.fit(train)

    preds = model.transform(test)

    rmse = RegressionEvaluator(
        labelCol="actual_duration_min",
        predictionCol="prediction",
        metricName="rmse"
    ).evaluate(preds)

    print(f"✅ RMSE = {rmse:.2f} minutes")

# =========================================================
# 9. RUN EXPERIMENTS
# =========================================================
for svc in ["3h", "5h"]:
    for ctx in ["normal", "rain_only", "rain_flood"]:
        train_model(svc, ctx)

# =========================================================
# 10. STOP SPARK
# =========================================================
spark.stop()