from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import logging

# ==============================
# 1. CONFIG
# ==============================
HDFS_FINAL_FEATURES = "hdfs://namenode:9000/user/doanquochuy/uds-project/data/processed/final_features"
OUTPUT_LOCAL = "file:///app/data/analysis/q3_spatial_results_csv"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark():
    return (
        SparkSession.builder
        .appName("UDS_Q3_Spatial_Relationship_Analysis")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )


def main():
    spark = get_spark()

    try:
        # ==============================
        # LOAD DATA
        # ==============================
        logger.info("Loading final_features dataset for Spatial Analyze...")
        df_raw = spark.read.parquet(HDFS_FINAL_FEATURES)

        # ==============================
        # 2. CONTEXT SEGMENTATION (NEW – CORRECT)
        # ==============================
        logger.info("Creating context_segment...")

        df_with_context = df_raw.withColumn(
            "context_segment",
            when((col("has_flood") == 1) & (col("prcp_mm") > 5), "rain_flood")
            .when(col("has_flood") == 1, "flood_only")
            .when((col("prcp_mm") > 5) | col("condition_label").isin("Heavy Rain", "Thunderstorm"), "rain_only")
            .otherwise("normal")
        )

        df_with_context.createOrReplaceTempView("uds_spatial_data")

        # ==============================
        # QUERY 1: CORE SUMMARY (DISTANCE – TIME – EFFICIENCY)
        # ==============================
        logger.info("Running core Q3 analysis...")

        core_summary = spark.sql("""
            SELECT
                context_segment,
                COUNT(*) AS volume,

                ROUND(AVG(shippingDistance), 2) AS avg_distance_km,

                ROUND(AVG(actual_duration_min), 2) AS avg_duration_min,

                ROUND(AVG(shippingDistance / actual_duration_min), 4) AS avg_efficiency,

                ROUND(AVG(traffic_congestion_index), 2) AS avg_traffic
            FROM uds_spatial_data
            WHERE actual_duration_min > 0
            GROUP BY context_segment
            ORDER BY avg_efficiency DESC
        """)

        core_summary.show(truncate=False)

        # ==============================
        # QUERY 2: DISTANCE COMPARISON
        # ==============================
        logger.info("Analyzing distance consistency...")

        distance_check = spark.sql("""
            SELECT
                context_segment,
                ROUND(AVG(shippingDistance), 2) AS avg_distance_km
            FROM uds_spatial_data
            GROUP BY context_segment
        """)

        distance_check.show()

        # ==============================
        # QUERY 3: DURATION COMPARISON
        # ==============================
        logger.info("Analyzing duration differences...")

        duration_check = spark.sql("""
            SELECT
                context_segment,
                ROUND(AVG(actual_duration_min), 2) AS avg_duration_min
            FROM uds_spatial_data
            WHERE actual_duration_min > 0
            GROUP BY context_segment
        """)

        duration_check.show()

        # ==============================
        # QUERY 4: EFFICIENCY
        # ==============================
        logger.info("Analyzing route efficiency...")

        efficiency_check = spark.sql("""
            SELECT
                context_segment,
                ROUND(AVG(shippingDistance / actual_duration_min), 4) AS efficiency
            FROM uds_spatial_data
            WHERE actual_duration_min > 0
            GROUP BY context_segment
        """)

        efficiency_check.show()

        # ==============================
        # QUERY 5: CORRELATION (FLOOD → TIME)
        # ==============================
        logger.info("Computing flood severity correlation...")

        stats_evidence = spark.sql("""
            SELECT
                ROUND(CORR(flood_avg_severity, actual_duration_min), 3) AS corr_severity_duration
            FROM uds_spatial_data
            WHERE has_flood = 1
        """)

        stats_evidence.show()

        # ==============================
        # SAVE RESULTS
        # ==============================
        logger.info(f"Saving results to {OUTPUT_LOCAL}...")

        core_summary.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{OUTPUT_LOCAL}/summary")

        distance_check.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{OUTPUT_LOCAL}/distance")

        duration_check.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{OUTPUT_LOCAL}/duration")

        efficiency_check.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{OUTPUT_LOCAL}/efficiency")

        stats_evidence.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{OUTPUT_LOCAL}/correlation")

        logger.info("Q3 Spatial Analysis COMPLETED SUCCESSFULLY")

    except Exception as e:
        logger.error(f"Analysis failed: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()