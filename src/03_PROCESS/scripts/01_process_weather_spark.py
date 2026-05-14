from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_utc_timestamp,
    to_timestamp,
    when,
    coalesce,
    create_map,
    lit,
    date_trunc
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType
)
import logging

# ==============================
# LOGGER
# ==============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HDFS_RAW = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/raw"
HDFS_PROCESSED = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed"

# ==============================
# SPARK SESSION
# ==============================
def get_spark_session():
    return (
        SparkSession.builder
        .appName("Process_Orders_Weather")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

# ==============================
# WEATHER TRANSFORM
# ==============================
def transform_weather(spark, path):

    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("temp", DoubleType(), True),
        StructField("rhum", IntegerType(), True),
        StructField("prcp", DoubleType(), True),
        StructField("wdir", IntegerType(), True),
        StructField("wspd", DoubleType(), True),
        StructField("cldc", IntegerType(), True),
        StructField("pres", DoubleType(), True),
        StructField("coco", IntegerType(), True),
    ])

    logger.info("Reading weather raw data...")
    df = spark.read.csv(path, header=True, schema=schema)

    # ==============================
    # TIMEZONE FIX
    # ==============================
    df = df.withColumn(
        "timestamp",
        from_utc_timestamp(col("timestamp"), "Asia/Ho_Chi_Minh")
    )

    # ==============================
    # WEATHER CONDITION LABEL
    # (Keep for traceability / rule-based logic)
    # ==============================
    coco_mapping = create_map(
        *[lit(x) for x in [
            1, "Clear",
            2, "Fair",
            3, "Cloudy",
            4, "Overcast",
            5, "Foggy",
            6, "Freezing Fog",
            7, "Light Rain",
            8, "Rain",
            9, "Heavy Rain",
            10, "Freezing Rain",
            11, "Heavy Freezing Rain",
            12, "Sleet",
            13, "Heavy Sleet",
            14, "Light Snowfall",
            15, "Snowfall",
            16, "Heavy Snowfall",
            17, "Rain Shower",
            18, "Heavy Rain Shower",
            19, "Sleet Shower",
            20, "Heavy Sleet Shower",
            21, "Snow Shower",
            22, "Heavy Snow Shower",
            23, "Lightning",
            24, "Hail",
            25, "Thunderstorm",
            26, "Heavy Thunderstorm",
            27, "Storm"
        ]]
    )

    df = df.withColumn(
        "condition_label",
        coalesce(coco_mapping[col("coco")], lit("Unknown"))
    )

    # ==============================
    # NULL HANDLING (NUMERIC ONLY)
    # ==============================
    df = df.withColumn(
        "prcp",
        coalesce(col("prcp"), lit(0.0))
    )

    # ==============================
    # RENAME COLUMNS (ML-FRIENDLY)
    # ==============================
    df = (
        df
        .withColumnRenamed("temp", "temp_c")
        .withColumnRenamed("rhum", "rhum_pct")
        .withColumnRenamed("prcp", "prcp_mm")
        .withColumnRenamed("wdir", "wdir_deg")
        .withColumnRenamed("wspd", "wspd_kmh")
        .withColumnRenamed("cldc", "cldc_pct")
        .withColumnRenamed("pres", "pres_hpa")
        .withColumnRenamed("coco", "coco_code")
    )

    # ==============================
    # HEAVY RAIN FLAG (EDA-DRIVEN)
    # ==============================
    df = df.withColumn(
    "is_heavy_rain",
    when(
        (col("prcp_mm") > 5) | 
        (col("condition_label").isin("Heavy Rain", "Heavy Rain Shower", "Light Rain", "Rain", "Rain Shower")), 
        1
    ).otherwise(0)
)

    # ==============================
    # JOIN KEY (HOURLY)
    # ==============================
    df = df.withColumn(
        "hour_timestamp",
        date_trunc("hour", col("timestamp"))
    )

    return df


# ==============================
# ORDERS TRANSFORM
# ==============================
def transform_orders(spark, path):

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("deliveredAt", TimestampType(), True),
        StructField("expectedDeliveryTime", TimestampType(), True),
        StructField("mdh", StringType(), True),
        StructField("package_name", StringType(), True),
        StructField("orderStatus", StringType(), True),
        StructField("senderAddress", StringType(), True),
        StructField("senderLat", DoubleType(), True),
        StructField("senderLng", DoubleType(), True),
        StructField("receiverAddress", StringType(), True),
        StructField("receiverLat", DoubleType(), True),
        StructField("receiverLng", DoubleType(), True),
        StructField("shippingDistance", DoubleType(), True),
        StructField("shipper", StringType(), True),
        StructField("weight", DoubleType(), True),
        StructField("serviceType", StringType(), True),
        StructField("image", StringType(), True),
    ])

    logger.info("Reading orders raw data...")
    df = spark.read.csv(path, header=True, schema=schema)

    # ==============================
    # TIMEZONE FIX
    # ==============================
    df = (
        df
        .withColumn(
            "createdAt",
            from_utc_timestamp(to_timestamp(col("createdAt")), "Asia/Ho_Chi_Minh")
        )
        .withColumn(
            "deliveredAt",
            from_utc_timestamp(to_timestamp(col("deliveredAt")), "Asia/Ho_Chi_Minh")
        )
        .withColumn(
            "expectedDeliveryTime",
            from_utc_timestamp(to_timestamp(col("expectedDeliveryTime")), "Asia/Ho_Chi_Minh")
        )
    )

    # ==============================
    # JOIN KEY (ORDER CREATION HOUR)
    # ==============================
    df = df.withColumn(
        "hour_timestamp",
        date_trunc("hour", col("createdAt"))
    )

    return df


# ==============================
# MAIN
# ==============================
def main():

    spark = get_spark_session()

    weather_path = f"{HDFS_RAW}/hcmc_weather_raw.csv"
    orders_path = f"{HDFS_RAW}/uds_orders.csv"

    logger.info("STEP 1: PROCESS WEATHER")
    weather = transform_weather(spark, weather_path)

    logger.info("STEP 2: PROCESS ORDERS")
    orders = transform_orders(spark, orders_path)

    logger.info(f"Weather count: {weather.count()}")
    logger.info(f"Orders count: {orders.count()}")

    # ==============================
    # SAVE PARQUET (NO JOIN HERE)
    # ==============================
    weather.write.mode("overwrite").parquet(
        f"{HDFS_PROCESSED}/weather"
    )

    orders.write.mode("overwrite").parquet(
        f"{HDFS_PROCESSED}/orders"
    )

    # SAVE TO LOCAL (FOR DEBUGGING)
    weather.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv("file:///app/data/processed/weather_csv")
    orders.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv("file:///app/data/processed/orders_csv")

    logger.info("PROCESS WEATHER & ORDERS DONE")
    spark.stop()


if __name__ == "__main__":
    main()