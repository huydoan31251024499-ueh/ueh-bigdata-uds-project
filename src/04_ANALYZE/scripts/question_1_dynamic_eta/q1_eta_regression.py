"""
Q1 – Context-aware ETA Regression (Improved)

Objective:
- Improve ETA prediction accuracy using data-centric ML
- Emphasize flood impact over rainfall based on EDA
- Maintain explainability and SLA-controlled training

Target:
- Services: 3h, 5h
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
spark = (
    SparkSession.builder
    .appName("UDS_Q1_ETA_Context_Aware")
    .getOrCreate()
)

# =========================================================
# 2. Load FINAL dataset (PARQUET)
# =========================================================
df = spark.read.parquet(
    "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed/final_features"
)

# =========================================================
# 3. BUSINESS FILTERING (CLEAN DATA)
# =========================================================
# SLA-critical services only
df = df.filter(col("serviceType").isin("3h", "5h"))

# Remove extreme failures (> 24h)
df = df.filter(col("actual_duration_min") <= 1440)

# Normalize distance (meters → km)
df = df.withColumn("shipping_km", col("shippingDistance") / 1000.0)

# =========================================================
# 4. FEATURE ENGINEERING (EDA-DRIVEN)
# =========================================================

# ---- Peak hour (traffic structure) ----
df = df.withColumn(
    "is_peak_hour",
    when(
        (col("order_hour").between(7, 9)) |
        (col("order_hour").between(16, 19)),
        1
    ).otherwise(0)
)

# ---- Rain (secondary signal) ----
df = df.withColumn(
    "is_heavy_rain",
    when(col("prcp_mm") > 5, 1).otherwise(0)
)

# ---- Flood (primary risk driver from EDA) ----
df = df.withColumn(
    "has_flood",
    when(col("flood_avg_depth_cm") > 0, 1).otherwise(0)
)

# ---- Flood severity (important) ----
df = df.withColumn(
    "flood_severity",
    col("flood_avg_severity")
)

# ---- Traffic non-linearity ----
df = df.withColumn(
    "traffic_penalty",
    col("traffic_congestion_index") * col("traffic_congestion_index")
)

# ---- Interaction: flood × peak hour ----
df = df.withColumn(
    "flood_peak_interaction",
    col("has_flood") * col("is_peak_hour")
)

# =========================================================
# 5. DATA SEGMENTATION (EDA-ALIGNED)
# =========================================================
# Flood dominates rain → segmentation prioritizes flood
df = df.withColumn(
    "context_segment",
    when(col("has_flood") == 1, "flood")
    .when(col("is_heavy_rain") == 1, "rain_only")
    .otherwise("normal")
)

# =========================================================
# 6. SLA-COMPLIANT SUBSET
# =========================================================
df = df.withColumn(
    "sla_compliant",
    when(
        ((col("serviceType") == "3h") & (col("actual_duration_min") <= 240)) |
        ((col("serviceType") == "5h") & (col("actual_duration_min") <= 360)),
        1
    ).otherwise(0)
)

# =========================================================
# 7. FEATURE VECTOR (CONTEXT-AWARE)
# =========================================================
FEATURE_COLS = [
    # Distance & load
    "shipping_km",
    "weight",

    # Traffic & market
    "traffic_penalty",
    "avg_vehicle_speed_kmh",
    "active_delivery_vehicles",

    # Flood (primary)
    "has_flood",
    "flood_severity",
    "flood_peak_interaction",

    # Temporal
    "is_peak_hour",
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

    df_sub = (
        df.filter(col("serviceType") == service)
          .filter(col("context_segment") == context)
          .filter(col("sla_compliant") == 1)
    )

    raw_count = df_sub.count()
    if raw_count < 30:
        print(f"SKIP: Not enough data (count={raw_count})")
        return

    pipeline_df = scaler.fit(
        assembler.transform(df_sub)
    ).transform(
        assembler.transform(df_sub)
    )

    train_df, test_df = pipeline_df.randomSplit([0.8, 0.2], seed=42)

    lr = LinearRegression(
        featuresCol="features",
        labelCol="actual_duration_min",
        regParam=0.1,
        elasticNetParam=0.5
    )

    model = lr.fit(train_df)
    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(
        labelCol="actual_duration_min",
        predictionCol="prediction",
        metricName="rmse"
    )

    rmse = evaluator.evaluate(predictions)
    print(f"RMSE: {rmse:.2f} minutes")

# =========================================================
# 9. RUN EXPERIMENTS
# =========================================================
for svc in ["3h", "5h"]:
    for ctx in ["normal", "rain_only", "flood"]:
        train_model(svc, ctx)

# =========================================================
# 10. STOP SPARK
# =========================================================
spark.stop()