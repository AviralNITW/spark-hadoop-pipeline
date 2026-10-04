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
from pyspark.sql import SparkSession

# Create Spark Session (entry point)
spark = SparkSession.builder \
    .appName("Simple_YARN_Job") \
    .getOrCreate()

# Read data from HDFS
df = spark.read.csv("hdfs://namenode:9000/data/data.csv", header=True, inferSchema=True)

# Transformation
filtered_df = df.filter(df["amount"] > 25)

# Show result (for debugging)
filtered_df.show()

# Write back to HDFS
filtered_df.write.mode("overwrite").csv("hdfs://namenode:9000/data/output")

# Stop session
spark.stop()
