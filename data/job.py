# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# 
# spark = SparkSession.builder \
#     .appName("DeepUnderstanding") \
#     .getOrCreate()
# 
# data = [
#     ("Alice", 25, "IT"),
#     ("Bob", 32, "HR"),
#     ("Cathy", 29, "IT"),
#     ("David", 35, "HR"),
#     ("Eve", 40, "Finance")
# ]
# 
# columns = ["name", "age", "dept"]
# 
# df = spark.createDataFrame(data, columns)
# 
# print("\n=== Original Data ===")
# df.show()
# 
# df_filtered = df.filter(col("age") > 30).select("name", "dept")
# 
# print("\n=== Filtered Data ===")
# df_filtered.show()
# 
# df_grouped = df.groupBy("dept").count()
# 
# print("\n=== Grouped Data ===")
# df_grouped.show()
# 
# print("\n=== Execution Plan ===")
# df_grouped.explain(True)
# 
# spark.stop()

####################################################### code 2 working 

# from pyspark.sql import SparkSession
# 
# # Create Spark Session (entry point)
# spark = SparkSession.builder \
#     .appName("Simple_YARN_Job") \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")
# # Read data from HDFS
# df = spark.read.csv("hdfs://namenode:9000/data/data.csv", header=True, inferSchema=True)
# 
# 
# df.printSchema()
# df.show(5)
# # Transformation
# filtered_df = df.filter(df["amount"] > 43)
# 
# # Show result (for debugging)
# filtered_df.show()
# 
# # Write back to HDFS
# filtered_df.write.mode("overwrite").csv("hdfs://namenode:9000/data/output")
# 
# #printing filtered data 
# 
# # Stop session
# spark.stop()

#####################################################code 3
from pyspark.sql import SparkSession

# =========================
# START SPARK SESSION
# =========================
print("\n========== STARTING SPARK SESSION ==========\n")

# spark = SparkSession.builder \
#     .appName("Simple_YARN_Job") \
#     .getOrCreate()

#  working with hive now ----------------------------------------------------------
# spark = SparkSession.builder \
#     .appName("Simple_YARN_Job") \
#     .enableHiveSupport() \
#     .getOrCreate()


# spark = SparkSession.builder \
#     .appName("Hive_Job") \
#     .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
#     .enableHiveSupport() \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("Hive_Persistent_Table") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

#----------------------------------------------------------------------------------
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
# WRITE TO HDFS
# =========================
print("\n========== WRITING FILTERED DATA TO HDFS ==========\n")

filtered_df.write.mode("overwrite").csv(
    "hdfs://namenode:9000/data/output",
    header=True
)


## saving the tabels into hive metastore --------------------------------------

filtered_df.write.mode("overwrite").saveAsTable("filtered_transactions")
## ----------------------------------------------------------------------------
# =========================
# READ BACK WRITTEN DATA
# =========================
print("\n========== READING BACK WRITTEN DATA ==========\n")

df_output = spark.read.csv(
    "hdfs://namenode:9000/data/output",
    header=True,
    inferSchema=True
)

df_output.show(5)

# =========================
# CREATE TEMP VIEW
# =========================
print("\n========== CREATING TEMP VIEW ==========\n")

df_output.createOrReplaceTempView("transactions")

# =========================
# RUN SQL QUERY
# =========================
print("\n========== RUNNING SQL QUERY (fraud_type count) ==========\n")

result = spark.sql("""
    SELECT fraud_type, COUNT(*) AS count
    FROM transactions
    GROUP BY fraud_type
    ORDER BY count DESC
""")

result.show()
# -------------------querying the hive datastore ------------------------------------
result = spark.sql("""
    SELECT fraud_type, COUNT(*) AS count
    FROM filtered_transactions
    GROUP BY fraud_type
    ORDER BY count ASC
""")
print("\n========== RUNNING hive sql QUERY (fraud_type count asc ) ==========\n")
result.show()
print("\n========== DESCRIBE FORMATTED filtered_transactions  ==========\n")
spark.sql("DESCRIBE FORMATTED filtered_transactions").show(truncate=False)
## --------------------------------------------------------------------------------
# =========================
# STOP SPARK
# =========================
print("\n========== STOPPING SPARK SESSION ==========\n")

spark.stop()
