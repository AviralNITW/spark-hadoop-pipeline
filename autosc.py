import subprocess
import os

# CONFIG
namenode_container = "namenode"
spark_container = "spark"

host_file = os.path.expanduser("~/Downloads/sample_transactions.csv")
host_hive_conf = os.path.expanduser("~/sparkonly/hive-site.xml")

container_tmp_path = "/tmp/data.csv"
container_conf_path = "/opt/spark/conf/hive-site.xml"
jdbc_jar = "postgresql-42.6.0.jar"

def run(cmd):
    print(f"\nRunning: {cmd}")
    result = subprocess.run(cmd, shell=True, text=True)
    if result.returncode != 0:
        print("❌ Error occurred")
        exit(1)

# =========================
# 1. Copy CSV → namenode
# =========================
run(f"docker cp {host_file} {namenode_container}:{container_tmp_path}")


# =========================
# Create conf directory in Spark container
# =========================
run(f"docker exec {spark_container} mkdir -p /opt/spark/conf")


# =========================
# 2. Copy hive-site.xml → spark
# =========================
run(f"docker cp {host_hive_conf} {spark_container}:{container_conf_path}")




# =========================
# 3. Download JDBC driver (HOST)
# =========================
run(f"wget -nc https://jdbc.postgresql.org/download/{jdbc_jar}")

# =========================
# 4. Copy JDBC driver → spark container
# =========================
run(f"docker cp {jdbc_jar} {spark_container}:/opt/spark/jars/")

# =========================
# 5. Create HDFS directories
# =========================
run(f"docker exec {namenode_container} hdfs dfs -mkdir -p /data")
run(f"docker exec {namenode_container} hdfs dfs -mkdir -p /user/hive/warehouse")

# =========================
# 6. Fix permissions (important)
# =========================
run(f"docker exec {namenode_container} hdfs dfs -chmod -R 777 /data")
run(f"docker exec {namenode_container} hdfs dfs -chmod -R 777 /user/hive")

# =========================
# 7. Put file into HDFS
# =========================
run(f"docker exec {namenode_container} hdfs dfs -put -f {container_tmp_path} /data/data.csv")

# =========================
# 8. Restart Spark (VERY IMPORTANT)
# =========================
run(f"docker restart {spark_container}")

print("\n✅ Setup complete! Now run your Spark job.")
