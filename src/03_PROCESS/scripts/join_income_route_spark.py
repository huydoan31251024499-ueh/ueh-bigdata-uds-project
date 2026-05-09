from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, round as spark_round, unix_timestamp,
    when, lit, hour, dayofweek, month,
    count, avg, sum as spark_sum
)
from pyspark.sql.types import DoubleType
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session():
    """Initialize and return Spark session connected to cluster"""
    spark = SparkSession.builder \
        .appName("UDS_Income_Route_Analysis") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark


def load_processed_data(spark, hdfs_processed):
    """
    Load all 4 cleaned datasets from HDFS processed directory.
    Returns: orders, weather, flood, market dataframes.
    """
    print("Loading all processed datasets...")

    try:
        df_orders  = spark.read.parquet(f"{hdfs_processed}/orders_clean")
        df_weather = spark.read.parquet(f"{hdfs_processed}/weather_clean")
        df_flood   = spark.read.parquet(f"{hdfs_processed}/flood_clean")
        df_market  = spark.read.parquet(f"{hdfs_processed}/market_clean")

        print(f"Orders : {df_orders.count()} rows")
        print(f"Weather: {df_weather.count()} rows")
        print(f"Flood  : {df_flood.count()} hourly rows")
        print(f"Market : {df_market.count()} rows")

        return df_orders, df_weather, df_flood, df_market

    except Exception as e:
        print(f"Error loading processed data: {e}")
        raise


def join_all_sources(df_orders, df_weather, df_flood, df_market):
    """
    Perform multi-source temporal LEFT JOIN on hour_timestamp.
    Base table: orders (2,290 rows) - every order enriched with
    weather, market, and flood data from the same hour.
    All joins are LEFT to preserve all order rows.
    """
    print("Performing multi-source temporal JOIN on hour_timestamp...")

    try:
        df_w = df_weather.withColumnRenamed("hour_timestamp", "w_hour")
        df_f = df_flood.withColumnRenamed("hour_timestamp",   "f_hour")
        df_m = df_market.withColumnRenamed("hour_timestamp",  "m_hour")

        # orders x weather
        df_joined = df_orders.join(
            df_w,
            df_orders["hour_timestamp"] == df_w["w_hour"],
            how="left"
        ).drop("w_hour")

        # + market
        df_joined = df_joined.join(
            df_m,
            df_joined["hour_timestamp"] == df_m["m_hour"],
            how="left"
        ).drop("m_hour")

        # + flood
        df_joined = df_joined.join(
            df_f,
            df_joined["hour_timestamp"] == df_f["f_hour"],
            how="left"
        ).drop("f_hour")

        # Drop duplicate timestamp columns from weather and market after join
        df_joined = df_joined.drop("timestamp")
        # Fill NULL for hours with no flood
        df_joined = df_joined.fillna({
            "flood_count":              0,
            "has_flood":                0,
            "avg_flood_depth_cm":       0.0,
            "avg_flood_severity_score": 0.0,
            "avg_flood_duration_min":   0.0,
            "avg_rainfall_trigger_mm":  0.0,
        })

        total = df_joined.count()
        print(f"After JOIN: {total} rows")
        print(f"  With flood   : {df_joined.filter(col('has_flood') == 1).count()}")
        print(f"  Missing market: {df_joined.filter(col('traffic_congestion_index').isNull()).count()}")
        print(f"  Missing weather: {df_joined.filter(col('temp_c').isNull()).count()}")

        return df_joined

    except Exception as e:
        print(f"Error in multi-source join: {e}")
        raise


def add_base_delivery_features(df):
    """
    Add base delivery features from orders data:
    - actual_duration_min  : real delivery time (minutes)
    - expected_duration_min: expected delivery time (minutes)
    - delay_min            : positive=late, negative=early
    - is_late              : 1 if delayed, 0 if on time
    - distance_km          : filtered outliers > 200km
    - order_hour, order_dow, order_month, time_slot
    - rain_level, is_extreme_weather
    """
    print("Adding base delivery features...")

    try:
        df = df \
            .withColumn("actual_duration_min", spark_round(
                (unix_timestamp(col("deliveredAt")) -
                 unix_timestamp(col("createdAt"))) / 60.0, 2)) \
            .withColumn("expected_duration_min", spark_round(
                (unix_timestamp(col("expectedDeliveryTime")) -
                 unix_timestamp(col("createdAt"))) / 60.0, 2)) \
            .withColumn("delay_min", spark_round(
                col("actual_duration_min") - col("expected_duration_min"), 2)) \
            .withColumn("is_late",
                when(col("delay_min") > 0, lit(1)).otherwise(lit(0))) \
            .withColumn("distance_km",
                spark_round(col("shippingDistance_km"), 3)) \
            .filter(col("distance_km") >= 0.5) \
            .filter(col("distance_km") <= 200.0) \
            .withColumn("order_hour",  hour(col("createdAt"))) \
            .withColumn("order_dow",   dayofweek(col("createdAt"))) \
            .withColumn("order_month", month(col("createdAt"))) \
            .withColumn("time_slot",
                when((col("order_hour") >= 6)  & (col("order_hour") < 11), lit("sang"))
                .when((col("order_hour") >= 11) & (col("order_hour") < 13), lit("trua"))
                .when((col("order_hour") >= 13) & (col("order_hour") < 18), lit("chieu"))
                .when((col("order_hour") >= 18) & (col("order_hour") < 22), lit("toi"))
                .otherwise(lit("dem"))) \
            .withColumn("rain_level",
                when(col("prcp_mm").isNull() | (col("prcp_mm") == 0), lit("no_rain"))
                .when(col("prcp_mm") < 2.5,  lit("light"))
                .when(col("prcp_mm") < 7.5,  lit("moderate"))
                .otherwise(lit("heavy"))) \
            .withColumn("is_extreme_weather",
                when(
                    (col("prcp_mm") > 5) |
                    col("condition_label").isin(
                        "Heavy Rain", "Thunderstorm",
                        "Heavy Thunderstorm", "Storm"),
                    lit(1)
                ).otherwise(lit(0)))

        print(f"Base features added. Rows after distance filter: {df.count()}")
        return df

    except Exception as e:
        print(f"Error adding base delivery features: {e}")
        raise


def add_driver_income_features(df):
    """
    Add driver income features for analysis question:
    'Thu nhap tai xe bi anh huong boi thoi tiet, ngap, ket xe nhu the nao?'

    Logic:
    - Base fee   : delivery_fee_avg_vnd from market data (actual market rate)
    - Multipliers: weather + flood + congestion + time-of-day adjustments
    - Fuel cost  : distance_km x 0.02L/km x fuel_price_vnd_liter
    - Net income : adjusted fee - fuel cost
    - Per-km     : net income / distance_km (efficiency metric)
    - Per-min    : net income / actual_duration_min (time efficiency metric)
    """
    print("Adding driver income features...")

    try:
        # Multiplier 1: rain condition from weather data
        df = df.withColumn(
            "weather_fee_multiplier",
            when(col("rain_level") == "heavy",    lit(1.5))
            .when(col("rain_level") == "moderate", lit(1.3))
            .when(col("rain_level") == "light",    lit(1.1))
            .otherwise(lit(1.0))
        )

        # Multiplier 2: flood severity at order hour
        df = df.withColumn(
            "flood_fee_multiplier",
            when(col("avg_flood_severity_score") > 20, lit(1.4))
            .when(col("avg_flood_severity_score") > 10, lit(1.2))
            .when(col("has_flood") == 1,                lit(1.1))
            .otherwise(lit(1.0))
        )

        # Multiplier 3: traffic congestion from market data
        df = df.withColumn(
            "congestion_fee_multiplier",
            when(col("congestion_level") == "gridlock",   lit(1.5))
            .when(col("congestion_level") == "heavy",     lit(1.3))
            .when(col("congestion_level") == "congested", lit(1.1))
            .otherwise(lit(1.0))
        )

        # Multiplier 4: time of day
        df = df.withColumn(
            "time_fee_multiplier",
            when(col("time_slot") == "dem",  lit(1.3))
            .when(col("time_slot") == "sang", lit(1.1))
            .otherwise(lit(1.0))
        )

        # Fix #2: Base fee theo distance_km thực tế (thay vì dùng market average cho mọi đơn)
        # Tiered pricing: đơn ngắn phí thấp hơn đơn dài
        df = df.withColumn(
            "base_delivery_fee_vnd",
            when(col("distance_km") < 3,   lit(15000.0))
            .when(col("distance_km") < 6,  lit(20000.0))
            .when(col("distance_km") < 10, lit(25000.0))
            .when(col("distance_km") < 15, lit(30000.0))
            .when(col("distance_km") < 20, lit(35000.0))
            .otherwise(lit(40000.0))
        )

        # Estimated delivery fee = tiered base x all multipliers
        df = df.withColumn(
            "estimated_delivery_fee_vnd",
            spark_round(
                col("base_delivery_fee_vnd") *
                col("weather_fee_multiplier") *
                col("flood_fee_multiplier") *
                col("congestion_fee_multiplier") *
                col("time_fee_multiplier"),
                0
            )
        )

        # Fuel cost: motorbike 2L per 100km = 0.02L/km (VEAA standard)
        FUEL_PER_KM = 0.02
        df = df.withColumn(
            "estimated_fuel_cost_vnd",
            spark_round(
                col("distance_km") * FUEL_PER_KM * col("fuel_price_vnd_liter"),
                0
            )
        )

        # Net driver income per trip
        df = df.withColumn(
            "estimated_driver_income_vnd",
            spark_round(
                col("estimated_delivery_fee_vnd") - col("estimated_fuel_cost_vnd"),
                0
            )
        )

        # Income per km: trip efficiency by distance
        df = df.withColumn(
            "income_per_km",
            spark_round(
                col("estimated_driver_income_vnd") / col("distance_km"), 0
            )
        )

        # Income per minute: trip efficiency by time
        df = df.withColumn(
            "income_per_min",
            spark_round(
                col("estimated_driver_income_vnd") / col("actual_duration_min"), 2
            )
        )

        print("Driver income features added.")
        return df

    except Exception as e:
        print(f"Error adding driver income features: {e}")
        raise


def add_route_optimization_features(df):
    """
    Add route optimization features for analysis question:
    'Quang duong nao toi uu nhat cho tai xe UDS?'

    Logic:
    - route_difficulty_score : flood(0.4) + congestion(0.4) + rain(0.2)
    - route_difficulty_level : easy/moderate/hard/very_hard
    - estimated_actual_speed : market speed adjusted for flood and rain
    - effective_distance_km  : real path longer than straight line
    - route_optimization_score: combines income/km + difficulty + lateness
    """
    print("Adding route optimization features...")

    try:
        # Route difficulty: weighted flood + congestion + rain
        df = df.withColumn(
            "route_difficulty_score",
            spark_round(
                (col("avg_flood_severity_score") * 0.4) +
                (col("traffic_congestion_index") * 0.4) +
                (col("prcp_mm") * 0.2),
                2
            )
        )

        # Difficulty classification
        df = df.withColumn(
            "route_difficulty_level",
            when(col("route_difficulty_score") < 2.0,  lit("easy"))
            .when(col("route_difficulty_score") < 5.0,  lit("moderate"))
            .when(col("route_difficulty_score") < 10.0, lit("hard"))
            .otherwise(lit("very_hard"))
        )

        # Real speed adjusted for flood and rain conditions
        df = df.withColumn(
            "estimated_actual_speed_kmh",
            spark_round(
                col("avg_vehicle_speed_kmh") *
                when(col("has_flood") == 1, lit(0.6)).otherwise(lit(1.0)) *
                when(col("rain_level") == "heavy",    lit(0.7))
                .when(col("rain_level") == "moderate", lit(0.85))
                .otherwise(lit(1.0)),
                2
            )
        )

        # Effective distance: detour penalty from difficulty
        df = df.withColumn(
            "effective_distance_km",
            spark_round(
                col("distance_km") *
                (lit(1.0) + col("route_difficulty_score") / 10.0),
                3
            )
        )

        # Route optimization score: high income/km + easy route + no delay
        df = df.withColumn(
            "route_optimization_score",
            spark_round(
                (col("income_per_km") / lit(1000.0)) *
                when(col("route_difficulty_level") == "easy",      lit(1.3))
                .when(col("route_difficulty_level") == "moderate",  lit(1.0))
                .when(col("route_difficulty_level") == "hard",      lit(0.7))
                .otherwise(lit(0.4)) *
                when(col("is_late") == 0, lit(1.1)).otherwise(lit(0.9)),
                2
            )
        )

        print("Route optimization features added.")
        return df

    except Exception as e:
        print(f"Error adding route optimization features: {e}")
        raise


def print_income_analysis(df):
    """
    Print driver income analysis results answering:
    'Thu nhap tai xe bi anh huong boi thoi tiet, ngap, ket xe nhu the nao?'
    """
    print("\n" + "=" * 70)
    print("PHAN TICH 1: THU NHAP TAI XE")
    print("=" * 70)

    total = df.count()
    print(f"Tong don hang phan tich: {total}")

    print("\n-- Thu nhap theo time_slot --")
    df.groupBy("time_slot") \
        .agg(
            count("id").alias("so_don"),
            avg("estimated_driver_income_vnd").alias("thu_nhap_tb_vnd"),
            avg("income_per_km").alias("thu_nhap_per_km"),
            avg("income_per_min").alias("thu_nhap_per_min"),
            avg("estimated_fuel_cost_vnd").alias("chi_phi_xang_tb")
        ) \
        .withColumn("thu_nhap_tb_vnd",  spark_round(col("thu_nhap_tb_vnd"),  0)) \
        .withColumn("thu_nhap_per_km",  spark_round(col("thu_nhap_per_km"),  0)) \
        .withColumn("thu_nhap_per_min", spark_round(col("thu_nhap_per_min"), 2)) \
        .withColumn("chi_phi_xang_tb",  spark_round(col("chi_phi_xang_tb"),  0)) \
        .orderBy("thu_nhap_tb_vnd", ascending=False) \
        .show()

    print("\n-- Thu nhap theo rain_level --")
    df.groupBy("rain_level") \
        .agg(
            count("id").alias("so_don"),
            avg("estimated_driver_income_vnd").alias("thu_nhap_tb_vnd"),
            avg("income_per_km").alias("thu_nhap_per_km"),
            avg("is_late").alias("ty_le_tre")
        ) \
        .withColumn("thu_nhap_tb_vnd", spark_round(col("thu_nhap_tb_vnd"), 0)) \
        .withColumn("thu_nhap_per_km", spark_round(col("thu_nhap_per_km"), 0)) \
        .withColumn("ty_le_tre",       spark_round(col("ty_le_tre"), 3)) \
        .orderBy("thu_nhap_tb_vnd", ascending=False) \
        .show()

    print("\n-- Thu nhap theo congestion_level --")
    df.groupBy("congestion_level") \
        .agg(
            count("id").alias("so_don"),
            avg("estimated_driver_income_vnd").alias("thu_nhap_tb_vnd"),
            avg("income_per_km").alias("thu_nhap_per_km"),
            avg("is_late").alias("ty_le_tre")
        ) \
        .withColumn("thu_nhap_tb_vnd", spark_round(col("thu_nhap_tb_vnd"), 0)) \
        .withColumn("thu_nhap_per_km", spark_round(col("thu_nhap_per_km"), 0)) \
        .withColumn("ty_le_tre",       spark_round(col("ty_le_tre"), 3)) \
        .orderBy("thu_nhap_tb_vnd", ascending=False) \
        .show()

    print("\n-- Thu nhap theo has_flood --")
    df.groupBy("has_flood") \
        .agg(
            count("id").alias("so_don"),
            avg("estimated_driver_income_vnd").alias("thu_nhap_tb_vnd"),
            avg("income_per_km").alias("thu_nhap_per_km"),
            avg("is_late").alias("ty_le_tre")
        ) \
        .withColumn("thu_nhap_tb_vnd", spark_round(col("thu_nhap_tb_vnd"), 0)) \
        .withColumn("thu_nhap_per_km", spark_round(col("thu_nhap_per_km"), 0)) \
        .withColumn("ty_le_tre",       spark_round(col("ty_le_tre"), 3)) \
        .show()

    print("\n-- Thu nhap theo thang (order_month) --")
    df.groupBy("order_month") \
        .agg(
            count("id").alias("so_don"),
            avg("estimated_driver_income_vnd").alias("thu_nhap_tb_vnd"),
            avg("estimated_fuel_cost_vnd").alias("chi_phi_xang_tb")
        ) \
        .withColumn("thu_nhap_tb_vnd", spark_round(col("thu_nhap_tb_vnd"), 0)) \
        .withColumn("chi_phi_xang_tb", spark_round(col("chi_phi_xang_tb"), 0)) \
        .orderBy("order_month") \
        .show()


def print_route_analysis(df):
    """
    Print route optimization analysis results answering:
    'Quang duong nao toi uu nhat cho tai xe UDS?'
    """
    print("\n" + "=" * 70)
    print("PHAN TICH 2: QUANG DUONG TOI UU")
    print("=" * 70)

    print("\n-- Route difficulty level distribution --")
    df.groupBy("route_difficulty_level") \
        .agg(
            count("id").alias("so_don"),
            avg("distance_km").alias("kc_tb_km"),
            avg("effective_distance_km").alias("kc_thuc_te_km"),
            avg("estimated_actual_speed_kmh").alias("toc_do_tb_kmh"),
            avg("route_optimization_score").alias("diem_toi_uu"),
            avg("income_per_km").alias("thu_nhap_per_km")
        ) \
        .withColumn("kc_tb_km",       spark_round(col("kc_tb_km"),       2)) \
        .withColumn("kc_thuc_te_km",  spark_round(col("kc_thuc_te_km"),  2)) \
        .withColumn("toc_do_tb_kmh",  spark_round(col("toc_do_tb_kmh"),  2)) \
        .withColumn("diem_toi_uu",    spark_round(col("diem_toi_uu"),    2)) \
        .withColumn("thu_nhap_per_km",spark_round(col("thu_nhap_per_km"),0)) \
        .orderBy("diem_toi_uu", ascending=False) \
        .show()

    print("\n-- Quang duong toi uu theo time_slot --")
    df.groupBy("time_slot") \
        .agg(
            count("id").alias("so_don"),
            avg("distance_km").alias("kc_tb_km"),
            avg("estimated_actual_speed_kmh").alias("toc_do_tb_kmh"),
            avg("route_optimization_score").alias("diem_toi_uu"),
            avg("income_per_km").alias("thu_nhap_per_km"),
            avg("is_late").alias("ty_le_tre")
        ) \
        .withColumn("kc_tb_km",       spark_round(col("kc_tb_km"),       2)) \
        .withColumn("toc_do_tb_kmh",  spark_round(col("toc_do_tb_kmh"),  2)) \
        .withColumn("diem_toi_uu",    spark_round(col("diem_toi_uu"),    2)) \
        .withColumn("thu_nhap_per_km",spark_round(col("thu_nhap_per_km"),0)) \
        .withColumn("ty_le_tre",      spark_round(col("ty_le_tre"),      3)) \
        .orderBy("diem_toi_uu", ascending=False) \
        .show()

    print("\n-- Top 10 don hang toi uu nhat (route_optimization_score cao nhat) --")
    df.select(
        "id", "distance_km", "effective_distance_km",
        "time_slot", "congestion_level", "rain_level",
        "has_flood", "estimated_driver_income_vnd",
        "income_per_km", "route_optimization_score"
    ) \
        .orderBy("route_optimization_score", ascending=False) \
        .limit(10) \
        .show(truncate=False)

    print("\n-- Khoang cach toi uu: income/km theo distance bucket (median, khong bi keo boi outlier) --")
    from pyspark.sql.functions import percentile_approx
    df.withColumn(
        "distance_bucket",
        when(col("distance_km") < 3,   lit("0-3km"))
        .when(col("distance_km") < 6,   lit("3-6km"))
        .when(col("distance_km") < 10,  lit("6-10km"))
        .when(col("distance_km") < 15,  lit("10-15km"))
        .when(col("distance_km") < 20,  lit("15-20km"))
        .otherwise(lit(">20km"))
    ).groupBy("distance_bucket") \
        .agg(
            count("id").alias("so_don"),
            percentile_approx("income_per_km", 0.5).alias("thu_nhap_per_km_median"),
            avg("income_per_km").alias("thu_nhap_per_km_mean"),
            percentile_approx("route_optimization_score", 0.5).alias("diem_toi_uu_median"),
            avg("is_late").alias("ty_le_tre")
        ) \
        .withColumn("thu_nhap_per_km_median", spark_round(col("thu_nhap_per_km_median"), 0)) \
        .withColumn("thu_nhap_per_km_mean",   spark_round(col("thu_nhap_per_km_mean"),   0)) \
        .withColumn("diem_toi_uu_median",     spark_round(col("diem_toi_uu_median"),     2)) \
        .withColumn("ty_le_tre",              spark_round(col("ty_le_tre"),              3)) \
        .orderBy("thu_nhap_per_km_median", ascending=False) \
        .show()


def main():
    spark = get_spark_session()

    HDFS_PROCESSED = "hdfs://namenode:9000/uds/data/processed"
    HDFS_JOINED    = "hdfs://namenode:9000/uds/data/joined"

    try:
        # STEP 1: Load all processed data
        print("=" * 70)
        print("STEP 1: Load Processed Data")
        print("=" * 70)
        df_orders, df_weather, df_flood, df_market = load_processed_data(
            spark, HDFS_PROCESSED
        )

        # STEP 2: Multi-source temporal JOIN
        print("\n" + "=" * 70)
        print("STEP 2: Multi-source Temporal JOIN")
        print("=" * 70)
        df_joined = join_all_sources(df_orders, df_weather, df_flood, df_market)

        # STEP 3: Base delivery features
        print("\n" + "=" * 70)
        print("STEP 3: Base Delivery Features")
        print("=" * 70)
        df_feat = add_base_delivery_features(df_joined)

        # STEP 4: Driver income features
        print("\n" + "=" * 70)
        print("STEP 4: Driver Income Features")
        print("=" * 70)
        df_feat = add_driver_income_features(df_feat)

        # STEP 5: Route optimization features
        print("\n" + "=" * 70)
        print("STEP 5: Route Optimization Features")
        print("=" * 70)
        df_feat = add_route_optimization_features(df_feat)

        # STEP 6: Print income analysis
        print_income_analysis(df_feat)

        # STEP 7: Print route analysis
        print_route_analysis(df_feat)

        # STEP 8: Save output
        print("\n" + "=" * 70)
        print("STEP 8: Save to HDFS")
        print("=" * 70)

        output_path = f"{HDFS_JOINED}/income_route_features"
        df_feat.write \
            .partitionBy("order_month") \
            .mode("overwrite") \
            .parquet(output_path)

        print(f"Output saved to: {output_path}")
        print(f"Total rows     : {df_feat.count()}")

        print("\n" + "=" * 70)
        print("OUTPUT SCHEMA")
        print("=" * 70)
        df_feat.printSchema()
        # STEP 9: Export to CSV
        print("\n" + "=" * 70)
        print("STEP 9: Export to CSV")
        print("=" * 70)

        OUTPUT_CSV = "hdfs://namenode:9000/uds/data/joined/income_route_features_csv"

        df_feat.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(OUTPUT_CSV)

        print(f"CSV saved to: {OUTPUT_CSV}")

    except Exception as e:
        print(f"Error in PySpark job: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()