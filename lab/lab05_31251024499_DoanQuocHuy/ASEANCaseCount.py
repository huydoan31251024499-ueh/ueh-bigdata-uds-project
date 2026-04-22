from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, regexp_replace
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

def main():
    spark = SparkSession.builder.appName("Lab05_ASEAN_DataFrame").getOrCreate()

    file_path = "hdfs://namenode:9000/user/doanquochuy/lab05/input/" \
    "WHO-COVID-19-20210601-213841.tsv"

    schema = StructType([
        StructField("Name", StringType(), True),
        StructField("WHO Region", StringType(), True),
        StructField("Cases - cumulative total", DoubleType(), True),
    ])

    df = spark.read.option("header", "true") \
        .option("sep", "\t") \
        .option("inferSchema", "true") \
        .csv(file_path)
    # df.printSchema()
    df = df.withColumn('Cases - cumulative total', \
                       regexp_replace(col('Cases - cumulative total'), ',', '') \
                       .cast('double'))
    # df.printSchema()

    asean_df = df.filter(col("WHO Region") == "South-East Asia")

    asean_df.select(sum("Cases - cumulative total")).show()

    max_country = asean_df.orderBy(col("Cases - cumulative total").desc()) \
        .select("Name", "Cases - cumulative total") \
        .first()
    print(f"The country with the maximum number of cumulative total cases: {max_country['Name']} \
          - {max_country['Cases - cumulative total']}")

    min_3_countries = asean_df.orderBy(col("Cases - cumulative total").asc()) \
        .select("Name", "Cases - cumulative total") \
        .limit(3)
    min_3_countries.show()
    print("- The top 3 countries with the lowest number of cumulative cases -")

    spark.stop()

main()