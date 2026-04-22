from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("UDS_Analysis") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema cho USD Orders
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

# 3. Nạp dữ liệu từ HDFS
df = spark.read.csv("hdfs://namenode:9000/user/data/uds-orders.csv", 
                        header=True, 
                        schema=schema)

### Tính toán cơ bản với DataFrame và Spark SQL
df.show()
print(f"Total orders: {df.count()}")
df.selectExpr("avg(shippingDistance) as avg_distance").show()
heavy_packages = df.filter(df.weight > 5)
heavy_packages.show(5)

### Tạo Temporary View Table để dùng với Spark SQL
df.createOrReplaceTempView("uds_orders")
spark.sql("SELECT id, weight,shippingDistance FROM uds_orders WHERE weight > 6").show(5)


### Nạp dữ liệu local
# df_local = spark.read.csv("/Users/doanquochuy/hadoop-ueh/data/uds-orders-aug2024.csv",
#                           header = True,
#                           schema=schema)
# df_local.show(20)