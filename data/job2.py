from pyspark.sql import SparkSession

print("\n========== STARTING SPARK SESSION ==========\n")

spark = SparkSession.builder \
    .appName("Delta_Parquet_Job") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# READ FROM HDFS
# =========================
print("\n========== READING INPUT FROM HDFS ==========\n")

df = spark.read.csv(
    "hdfs://namenode:9000/data/data.csv",
    header=True,
    inferSchema=True
)

df.printSchema()
df.show(5)

# =========================
# PARQUET PIPELINE (amount > 45)
# =========================
print("\n========== WRITING PARQUET (amount > 45) ==========\n")

parquet_df = df.filter(df["amount"] > 45)

parquet_df.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/output/parquet_output")

print("\n========== PARQUET WRITE COMPLETE ==========\n")



# =========================
# VERIFY PARQUET READ
# =========================
print("\n========== READING PARQUET DATA ==========\n")

parquet_read = spark.read.parquet(
    "hdfs://namenode:9000/data/output/parquet_output"
)

parquet_read.show(5)

# =========================
# DELTA PIPELINE (amount > 47)
# =========================
print("\n========== WRITING DELTA (amount > 47) ==========\n")

delta_df = df.filter(df["amount"] > 47)

delta_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("hdfs://namenode:9000/data/output/delta_output")

print("\n========== DELTA WRITE COMPLETE ==========\n")
# =========================
# VERIFY DELTA READ
# =========================
print("\n========== READING DELTA DATA ==========\n")

delta_read = spark.read.format("delta").load(
    "hdfs://namenode:9000/data/output/delta_output"
)

delta_read.show(5)

# =========================
# STOP SPARK
# =========================
print("\n========== STOPPING SPARK ==========\n")

spark.stop()
