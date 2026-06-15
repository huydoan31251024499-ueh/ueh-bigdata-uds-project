"""
Q1 – Context-aware ETA Regression (FINAL PRODUCTION)

✅ Train + Save ML model for real-time streaming inference
✅ Context-aware segmentation
✅ Feature consistency with streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import os

# =========================================================
# 1. Spark Session
# =========================================================
spark = (
    SparkSession.builder
    .appName("UDS_ETA_Offline_Training_FINAL")
    .getOrCreate()
)

MODEL_BASE_PATH = "hdfs://namenode:9000/user/doanquochuy/uds-project/models/eta"

# =========================================================
# 2. LOAD DATA
# =========================================================
df = spark.read.parquet(
    "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed/final_features"
)

# =========================================================
# 3. CLEANING
# =========================================================
df = df.filter(col("serviceType").isin("3h", "5h"))
df = df.filter(col("actual_duration_min") <= 1440)

df = df.withColumn("shipping_km", col("shippingDistance") / 1000.0)

# =========================================================
# 4. FEATURE ENGINEERING (MUST MATCH STREAMING)
# =========================================================

df = df.withColumn(
    "is_peak_hour",
    when(
        (col("order_hour").between(7, 9)) |
        (col("order_hour").between(16, 19)),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "has_flood",
    when(col("flood_avg_depth_cm") > 0, 1).otherwise(0)
)

df = df.withColumn(
    "flood_severity",
    col("flood_avg_severity")
)

df = df.withColumn(
    "traffic_penalty",
    col("traffic_congestion_index") * col("traffic_congestion_index")
)

df = df.withColumn(
    "flood_peak_interaction",
    col("has_flood") * col("is_peak_hour")
)

# =========================================================
# 5. CONTEXT SEGMENT
# =========================================================
df = df.withColumn(
    "context_segment",
    when(col("has_flood") == 1, "flood_only")
    .otherwise("normal")
)

# =========================================================
# 6. SLA FILTER
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
# 7. FEATURE VECTOR (IMPORTANT: SAME AS STREAMING)
# =========================================================
FEATURE_COLS = [
    "shipping_km",
    "traffic_penalty",
    "flood_severity",
    "flood_peak_interaction",
    "is_peak_hour"
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

lr = LinearRegression(
    featuresCol="features",
    labelCol="actual_duration_min",
    regParam=0.1,
    elasticNetParam=0.5
)

pipeline = Pipeline(stages=[assembler, scaler, lr])

# =========================================================
# 8. TRAIN FUNCTION + SAVE MODEL
# =========================================================
def train_and_save(service, context):
    print(f"\n=== TRAINING MODEL | {service} | {context} ===")

    df_sub = (
        df.filter(col("serviceType") == service)
          .filter(col("context_segment") == context)
          .filter(col("sla_compliant") == 1)
    )

    count = df_sub.count()
    if count < 50:
        print(f"SKIP: not enough data ({count})")
        return

    train_df, test_df = df_sub.randomSplit([0.8, 0.2], seed=42)

    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(
        labelCol="actual_duration_min",
        predictionCol="prediction",
        metricName="rmse"
    )

    rmse = evaluator.evaluate(predictions)
    print(f"RMSE: {rmse:.2f}")

    # ✅ SAVE MODEL (CRITICAL)
    model_path = f"{MODEL_BASE_PATH}/{service}_{context}"

    print(f"Saving model to: {model_path}")

    model.write().overwrite().save(model_path)


# =========================================================
# 9. RUN TRAINING
# =========================================================
for svc in ["3h", "5h"]:
    for ctx in ["normal", "flood_only"]:
        train_and_save(svc, ctx)

spark.stop()