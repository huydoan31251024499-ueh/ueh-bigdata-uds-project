from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_utc_timestamp, to_timestamp, round, 
    when, coalesce, create_map, lit, unix_timestamp, from_unixtime, date_trunc
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session():
    spark = SparkSession.builder \
    .appName("WeatherSparkProcess") \
    .master("local[*]") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()
    return spark

def get_condition_mapping():
    """Return mapping of weather condition codes to labels"""
    return {
        1: 'Clear',
        2: 'Fair',
        3: 'Cloudy',
        4: 'Overcast',
        5: 'Foggy',
        6: 'Freezing Fog',
        7: 'Light Rain',
        8: 'Rain',
        9: 'Heavy Rain',
        10: 'Freezing Rain',
        11: 'Heavy Freezing Rain',
        12: 'Sleet',
        13: 'Heavy Sleet',
        14: 'Light Snowfall',
        15: 'Snowfall',
        16: 'Heavy Snowfall',
        17: 'Rain Shower',
        18: 'Heavy Rain Shower',
        19: 'Sleet Shower',
        20: 'Heavy Sleet Shower',
        21: 'Snow Shower',
        22: 'Heavy Snow Shower',
        23: 'Lightning',
        24: 'Hail',
        25: 'Thunderstorm',
        26: 'Heavy Thunderstorm',
        27: 'Storm',
    }

def transform_weather(spark, weather_path):
    """
    Transform raw weather data:
    - Parse Asia/Ho_Chi_Minh timestamps (already converted in raw ingestion)
    - Add weather condition labels
    - Clean null values in precipitation
    - Rename columns with units
    """
    print("Reading raw weather data...")
    
    try:
        # Read CSV from HDFS
        weather_schema = StructType([
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

        weather_df = spark.read.option("header", "true") \
            .csv(weather_path, schema=weather_schema)
        
        print(f"Raw weather schema:")
        weather_df.printSchema()
        print("Sample weather data:")
        weather_df.show(3, truncate=False)
        
        # Parse timestamp (already in Asia/Ho_Chi_Minh from raw ingestion, not UTC)
        weather_df = weather_df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        
        # Create condition label mapping
        condition_map = get_condition_mapping()
        mapping_expr = create_map(*[lit(x) for x in sum(condition_map.items(), ())])

        weather_df = weather_df.withColumn(
            "condition_label",
            coalesce(mapping_expr[col("coco").cast(IntegerType())], lit("Unknown"))
        )
        
        # Fill nulls in precipitation with 0.0
        weather_df = weather_df.withColumn(
            "prcp",
            coalesce(col("prcp"), lit(0.0))
        )
        
        # Rename columns to include units
        weather_df = weather_df \
            .withColumnRenamed("temp", "temp_c") \
            .withColumnRenamed("rhum", "rhum_pct") \
            .withColumnRenamed("prcp", "prcp_mm") \
            .withColumnRenamed("wdir", "wdir_deg") \
            .withColumnRenamed("wspd", "wspd_kmh") \
            .withColumnRenamed("cldc", "cldc_pct") \
            .withColumnRenamed("pres", "pres_hpa") \
            .withColumnRenamed("coco", "coco_code")
        
        # Truncate timestamp to the hour for joining
        weather_df = weather_df.withColumn(
            "hour_timestamp",
            date_trunc("hour", col("timestamp"))
        )
        
        print("Weather transformation complete. Sample transformed data:")
        weather_df.show(5, truncate=False)
        
        return weather_df
    
    except Exception as e:
        print(f"Error in weather transformation: {e}")
        raise

def transform_orders(spark, orders_path):
    """
    Transform orders data:
    - Read UDS orders with lenient parsing
    - Round createdAt to nearest hour for joining
    """
    print("Reading UDS orders...")
    
    try:
        # Read CSV with lenient parsing to handle various formats
        orders_df = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .csv(orders_path)
        
        print(f"Orders schema:")
        orders_df.printSchema()
        print("Sample orders data:")
        orders_df.show(3, truncate=False)
        
        # Parse expectedDeliveryTime if it's a string (handle both timestamp and string formats)
        orders_df = orders_df.withColumn(
            "expectedDeliveryTime",
            to_timestamp(col("expectedDeliveryTime"))
        )
        
        # Round expectedDeliveryTime to nearest hour (use unix timestamp arithmetic)
        orders_df = orders_df.withColumn(
            "hour_timestamp",
            from_unixtime(
                round(unix_timestamp(col("expectedDeliveryTime")) / 3600) * 3600
            ).cast("timestamp")
        )
        
        print("Orders transformation complete.")
        print("Sample transformed orders data:")
        orders_df.show(5, truncate=False)
        
        return orders_df
    
    except Exception as e:
        print(f"Error in orders transformation: {e}")
        raise

def get_orders_schema():
    """Define the schema for UDS orders"""
    return StructType([
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

def join_weather_orders(weather_df, orders_df):
    """
    Perform left join between orders and weather on hourly timestamp
    """
    print("Joining weather and orders data...")
    
    try:
        # Alias columns to avoid ambiguity and perform left join on hour_timestamp
        joined_df = orders_df.alias("orders").join(
            weather_df.alias("weather"),
            on=col("orders.hour_timestamp") == col("weather.hour_timestamp"),
            how="left"
        ).select(
            col("orders.*"),
            col("weather.temp_c"),
            col("weather.rhum_pct"),
            col("weather.prcp_mm"),
            col("weather.wdir_deg"),
            col("weather.wspd_kmh"),
            col("weather.cldc_pct"),
            col("weather.pres_hpa"),
            col("weather.coco_code"),
            col("weather.condition_label")
        )
        
        print("Join complete")
        return joined_df
    
    except Exception as e:
        print(f"Error in join operation: {e}")
        raise

def add_extreme_weather_flag(joined_df):
    """
    Create is_extreme_weather flag:
    - TRUE if prcp_mm > 5 OR condition_label in ['Heavy Rain', 'Thunderstorm']
    """
    print("Adding extreme weather flag...")
    
    try:
        extreme_conditions = ["Heavy Rain", "Thunderstorm", "Heavy Thunderstorm", "Storm"]
        
        joined_df = joined_df.withColumn(
            "is_extreme_weather",
            when(
                (col("prcp_mm") > 5) | (col("condition_label").isin(extreme_conditions)),
                True
            ).otherwise(False)
        )
        
        print("Extreme weather flag added")
        return joined_df
    
    except Exception as e:
        print(f"Error adding extreme weather flag: {e}")
        raise

def main():
    # Initialize Spark
    spark = get_spark_session()
    
    # Define paths (use HDFS paths for production, local paths for testing):
    weather_path = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/raw/hcmc_weather_raw.csv"
    orders_path = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/raw/uds_orders.csv"
    
    # For local testing, use:
    # weather_path = "file:///Users/doanquochuy/bigdata-ueh/data/raw/hcmc_weather_raw.csv"
    # orders_path = "file:///Users/doanquochuy/bigdata-ueh/data/raw/uds_orders.csv"
    
    try:
        # Step 1: Transform Weather Data
        print("=" * 70)
        print("STEP 1: Weather Transformation")
        print("=" * 70)
        weather_df = transform_weather(spark, weather_path)
        
        # Step 2: Transform Orders Data
        print("\n" + "=" * 70)
        print("STEP 2: Orders Transformation")
        print("=" * 70)
        orders_df = transform_orders(spark, orders_path)
        
        # Step 3: Join Weather and Orders
        print("\n" + "=" * 70)
        print("STEP 3: Join Weather and Orders")
        print("=" * 70)
        joined_df = join_weather_orders(weather_df, orders_df)
        
        # Step 4: Add Extreme Weather Flag
        print("\n" + "=" * 70)
        print("STEP 4: Add Extreme Weather Analysis")
        print("=" * 70)
        final_df = add_extreme_weather_flag(joined_df)
        
        # Step 5: Show Results
        print("\n" + "=" * 70)
        print("FINAL RESULT: First 20 Rows")
        print("=" * 70)
        final_df.show(20, truncate=False)
        
        # Summary Statistics
        print("\n" + "=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)
        print(f"Total rows after join: {final_df.count()}")
        print(f"Orders with extreme weather: {final_df.filter(col('is_extreme_weather') == True).count()}")
        
        # Schema
        print("\n" + "=" * 70)
        print("OUTPUT SCHEMA")
        print("=" * 70)
        final_df.printSchema()
        
    except Exception as e:
        print(f"Error in PySpark job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()