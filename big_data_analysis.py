from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum, hour
from pyspark.sql.types import DoubleType

# Create Spark Session (FORCE LOCAL)
spark = SparkSession.builder \
    .appName("CODTECH_TASK1_BIG_DATA") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session Created")

# Load Dataset (FIXED PATH)
df = spark.read.csv(
    r"D:\TASK1_BIG_DATA_ANALYSIS\data\yellow_tripdata_2015-01.csv",
    header=True,
    inferSchema=True
)

print("✅ Data Loaded Successfully")
print("📊 Total Rows:", df.count())
print("📊 Total Columns:", len(df.columns))

df.show(5, truncate=False)

# Drop null rows
df = df.dropna()

# Clean fare_amount
df = df.withColumn(
    "fare_amount_clean",
    when(col("fare_amount").rlike(r"^-?\d+(\.\d+)?$"),
         col("fare_amount").cast(DoubleType()))
)

# Clean total_amount
df = df.withColumn(
    "total_amount_clean",
    when(col("total_amount").rlike(r"^-?\d+(\.\d+)?$"),
         col("total_amount").cast(DoubleType()))
)

# Average Fare
print("📈 Average Fare:")
df.select(avg("fare_amount_clean").alias("Average Fare")).show()

# Revenue by Payment Type
print("💰 Revenue by Payment Type:")
df.groupBy("payment_type") \
    .sum("total_amount_clean") \
    .orderBy("sum(total_amount_clean)", ascending=False) \
    .show()

# Peak Hour Analysis
print("⏰ Peak Pickup Hours:")
df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
    .groupBy("pickup_hour") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

spark.stop()
print("🛑 Spark Session Stopped")
