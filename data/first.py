from pyspark.sql import SparkSession

# =========================
# START SPARK SESSION
# =========================
print("\n========== STARTING SPARK SESSION ==========\n")

spark = SparkSession.builder \
    .appName("Hive_Job_Clean") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/data/hivetables") \
    .enableHiveSupport() \
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

df.show(5)

# =========================
# WRITE AS HIVE TABLE
# =========================
print("\n========== WRITING TO HIVE TABLE ==========\n")

# IMPORTANT: drop old broken table
spark.sql("DROP TABLE IF EXISTS filtered_transactions")

# write table
# df.write.mode("overwrite").saveAsTable("filtered_transactions")
df.write.mode("overwrite").parquet("hdfs://namenode:9000/data/filtered_transactions")
# =========================
# VERIFY TABLE LOCATION
# =========================
print("\n========== TABLE METADATA ==========\n")

# spark.sql("DESCRIBE FORMATTED filtered_transactions").show(truncate=False)

# =========================
# QUERY THE TABLE
# =========================
print("\n========== RUNNING QUERY ==========\n")

# result = spark.sql("""
#     SELECT fraud_type, COUNT(*) AS count
#     FROM filtered_transactions
#     GROUP BY fraud_type
#     ORDER BY count DESC
# """)
# 
# result.show()

df2 = spark.read.parquet("hdfs://namenode:9000/data/filtered_transactions")

df2.createOrReplaceTempView("transactions")

spark.sql("""
    SELECT fraud_type, COUNT(*) 
    FROM transactions 
    GROUP BY fraud_type
""").show()

# =========================
# STOP SPARK
# =========================
print("\n========== STOPPING SPARK SESSION ==========\n")

spark.stop()
