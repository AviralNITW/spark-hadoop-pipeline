
#####################################################code 3
from pyspark.sql import SparkSession

# =========================
# START SPARK SESSION
# =========================
print("\n========== STARTING SPARK SESSION ==========\n")

spark = SparkSession.builder \
    .appName("Simple_YARN_Job") \
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
# WRITE TO HDFS
# =========================
print("\n========== WRITING FILTERED DATA TO HDFS ==========\n")

filtered_df.write.mode("overwrite").csv(
    "hdfs://namenode:9000/data/output",
    header=True
)

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

# =========================
# STOP SPARK
# =========================
print("\n========== STOPPING SPARK SESSION ==========\n")

spark.stop()
