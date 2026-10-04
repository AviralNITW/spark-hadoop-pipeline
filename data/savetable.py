from pyspark.sql import SparkSession

# =========================
# START SPARK SESSION (HIVE ENABLED)
# =========================
print("\n========== STARTING SPARK SESSION ==========\n")

# spark = SparkSession.builder \
#     .appName("Hive_Persistent_Table") \
#     .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
#     .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
#     .enableHiveSupport() \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("Hive_Working") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
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

df.printSchema()
df.show(5)

# =========================
# TRANSFORMATION
# =========================
print("\n========== APPLYING FILTER ==========\n")

filtered_df = df.filter(df["amount"] > 43)

filtered_df.show()

# =========================
# WRITE AS HIVE TABLE (IMPORTANT CHANGE)
# =========================
print("\n========== WRITING TO HIVE TABLE ==========\n")

# Drop if exists (clean run)
spark.sql("DROP TABLE IF EXISTS filtered_transactions")

filtered_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("filtered_transactions")

# =========================
# VERIFY TABLE
# =========================
print("\n========== VERIFYING HIVE TABLE ==========\n")

spark.sql("SHOW TABLES").show()

spark.sql("SELECT * FROM filtered_transactions LIMIT 5").show()

spark.sql("DESCRIBE FORMATTED filtered_transactions").show(truncate=False)

# =========================
# STOP SPARK
# =========================
print("\n========== STOPPING SPARK SESSION ==========\n")

spark.stop()
