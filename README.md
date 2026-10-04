# Apache Spark and Hadoop Distributed Data Pipeline

## Overview

This repository provides an enterprise-grade, containerized Big Data processing pipeline. It integrates Apache Spark 3.5, Hadoop HDFS 3.2.1, YARN Resource Manager, Hive Metastore 3.1.3, PostgreSQL 13, Delta Lake 3.1.0, and Apache Parquet into a unified distributed computing platform.

The system includes automated cluster orchestration via Docker Compose and an automated Python infrastructure manager (`autosc.py`) that handles HDFS initialization, dataset ingestion, dependency injection (PostgreSQL JDBC driver), and metastore synchronization.

---

## Technical Architecture

The architecture is divided into three primary tiers: Storage & Metastore Tier, Distributed Compute & Execution Tier, and Automation & Ingestion Tier.

```
+-----------------------------------------------------------------------------------+
|                            Host Orchestrator (autosc.py)                          |
+-----------------------------------------+-----------------------------------------+
                                          |
                        Automated Provisioning & Data Sync
                                          |
          +-------------------------------+-------------------------------+
          |                               |                               |
          v                               v                               v
+-------------------+           +-------------------+           +-------------------+
|  Hadoop NameNode  |           |   Apache Spark    |           |   Hive Metastore  |
|   (HDFS Master)   |           |    Client Node    |           | (Metastore Daemon)|
|  hdfs://namenode  |<=========>| (PySpark Runtime) |<=========>| (Thrift Port 9083)|
+---------+---------+           +---------+---------+           +---------+---------+
          |                               |                               |
          | Block Store                   | YARN Compute                  | JDBC Sync
          v                               v                               v
+-------------------+           +-------------------+           +-------------------+
|  Hadoop DataNode  |           |  YARN Resource    |           |   PostgreSQL DB   |
| (Storage Daemon)  |           |     Manager       |           |  (Metastore DB)   |
+-------------------+           +-------------------+           +-------------------+
```

### Component Details

1. **HDFS Storage Layer**:
   - **NameNode**: Serves as the master directory service for HDFS. Maintains directory trees and block locations across the cluster. Accessible on RPC port `9000` and HTTP Web UI port `9870`.
   - **DataNode**: Manages physical block storage attached to the node. Handles block replication and read/write requests from clients. Exposed on port `9864`.

2. **Compute & Scheduling Layer**:
   - **Apache Spark Client Container**: Serves as the execution engine for PySpark applications. Equipped with Python 3, PySpark binaries, Delta Lake packages (`delta-spark_2.12-3.1.0.jar`), and PostgreSQL JDBC connectors.
   - **YARN ResourceManager & NodeManager**: Provides cluster resource management, allocating CPU cores and memory containers across nodes for distributed workload scheduling (Web UI on port `8088`).

3. **Metadata & Table Management Layer**:
   - **Hive Metastore Service**: Runs standalone Hive Metastore 3.1.3 over Thrift RPC protocol (port `9083`). Decouples table schema metadata from underlying physical storage files.
   - **PostgreSQL Database**: Serves as the underlying relational storage engine for Hive Metastore. Replaces embedded Apache Derby to enable concurrent connection handling and persistent metadata state across container lifecycles.

---

## Infrastructure Configuration & Docker Topology

All services are defined in `docker-compose.yml`. Below is the complete infrastructure mapping:

| Service Name | Base Docker Image | Network Configuration & Ports | Internal Environment Settings & Role |
| :--- | :--- | :--- | :--- |
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | `9000:9000` (RPC)<br>`9870:9870` (Web UI) | Configured with `CORE_CONF_fs_defaultFS=hdfs://namenode:9000` and `HDFS_CONF_dfs_namenode_name_dir`. Acts as primary master node. |
| `datanode` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | `9864:9864` (Data Web UI) | `HDFS_CONF_dfs_datanode_use_datanode_hostname=true`. Handles raw block operations. |
| `spark` | `apache/spark:3.5.0` | Container Shell Access | Volume mounts `./jars/` to `/extrajars` and `./data` to `/data`. Sets `HADOOP_CONF_DIR` and `SPARK_HOME`. |
| `postgres` | `postgres:13` | `5432:5432` | Database: `metastore`, User: `hive`, Password: `hive`. Stores catalog metadata schema. |
| `hive-metastore` | `apache/hive:3.1.3` | `9083:9083` (Thrift) | Connects to PostgreSQL via `HIVE_DB_JDBC_URL=jdbc:postgresql://HiveMetastoreDB_postgres:5432/metastore`. |
| `resourcemanager` | `bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8` | `8088:8088` (YARN Web UI) | Master daemon allocating compute capacity across the cluster. |
| `nodemanager` | `bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8` | Dynamic Container Ports | Worker daemon executing application tasks assigned by ResourceManager. |

---

## Project Directory & File Layout

```
spark-hadoop-pipeline/
├── autosc.py                  # Python deployment manager for HDFS & container initialization
├── docker-compose.yml         # Container cluster topology specification
├── hive-site.xml              # Global Hive Metastore connection parameters (Thrift URI & Warehouse DIR)
├── postgresql-42.6.0.jar      # PostgreSQL Type-4 JDBC Driver for metastore connectivity
├── spark_test.py              # Verification script for HDFS read and write operations
├── manual_spark_delta_lake_pyscript_working.odt # Detailed manual execution guide
├── data/
│   ├── data.csv               # Input transactional dataset
│   ├── first.py               # Basic CSV ingestion script
│   ├── job.py                 # Core pipeline script (HDFS Read -> Filter -> Hive Table -> Spark SQL)
│   ├── job2.py                # Storage format pipeline (Parquet & Delta Lake exports)
│   ├── job3.py                # SQL analytics and aggregation pipeline
│   ├── savetable.py           # Persistent Hive table creation using Parquet format
│   └── hive-site.xml          # Container metastore configuration
└── jars/
    └── delta-spark_2.12-3.1.0.jar # Delta Lake 3.1 runtime library
```

---

## Automated Deployment Engine (`autosc.py`)

The `autosc.py` script automates system initialization. It executes nine distinct operations to prepare the environment for Spark execution:

1. **Ingest Raw Data to NameNode Container**: Copies `sample_transactions.csv` from host storage to container temporary directory `/tmp/data.csv`.
2. **Directory Creation**: Ensures Spark configuration folder `/opt/spark/conf` exists.
3. **Hive Configuration Sync**: Copies `hive-site.xml` to `/opt/spark/conf/hive-site.xml` to allow Spark SQL to discover the Thrift metastore service.
4. **JDBC Driver Acquisition**: Downloads `postgresql-42.6.0.jar` from PostgreSQL repository if missing locally.
5. **JDBC Driver Injection**: Copies PostgreSQL JDBC driver into `/opt/spark/jars/` to support Hive Metastore database communication.
6. **HDFS Storage Provisioning**: Executes `hdfs dfs -mkdir -p /data` and `hdfs dfs -mkdir -p /user/hive/warehouse` on the NameNode.
7. **Permission Enforcement**: Executes `hdfs dfs -chmod -R 777 /data` and `hdfs dfs -chmod -R 777 /user/hive` to eliminate permission errors during Spark write operations.
8. **HDFS File Transfer**: Loads container temporary file `/tmp/data.csv` into HDFS path `hdfs://namenode:9000/data/data.csv`.
9. **Runtime Refresh**: Restarts the Spark container to re-initialize classpath configurations and environment settings.

---

## Execution Guide

### Step 1: Start the Distributed Cluster

Launch all containers in detached mode:

```bash
docker-compose up -d
```

Verify that all seven containers are running without errors:

```bash
docker-compose ps
```

---

### Step 2: Run Automated Infrastructure Setup

Execute the Python setup script to configure HDFS and metastore dependencies:

```bash
python autosc.py
```

Expected output confirms complete creation of `/data`, `/user/hive/warehouse`, permission assignments, dataset transfer, and Spark service restart.

---

### Step 3: Run Spark Processing Jobs

#### Pipeline 1: Basic HDFS Read and Write Test (`spark_test.py`)
Tests connection to HDFS NameNode, reads raw CSV data, filters rows where `amount > 25`, and outputs results back to HDFS.

```bash
docker exec -it spark spark-submit /data/spark_test.py
```

#### Pipeline 2: Full Ingestion, Hive Metastore Sync & SQL Aggregation (`data/job.py`)
Reads dataset from `hdfs://namenode:9000/data/data.csv`, applies transformation logic (`amount > 43`), saves output to HDFS, registers persistent table `filtered_transactions` into Hive Metastore via Thrift (`thrift://hive-metastore:9083`), creates in-memory temporary views, and executes analytical SQL queries.

```bash
docker exec -it spark spark-submit \
  --jars /extrajars/delta-spark_2.12-3.1.0.jar,/opt/spark/jars/postgresql-42.6.0.jar \
  /data/job.py
```

#### Pipeline 3: Apache Parquet and Delta Lake Storage Formats (`data/job2.py`)
Demonstrates multi-format data lake capabilities. Processes input data and writes to two separate storage systems:
- **Parquet Storage**: `hdfs://namenode:9000/data/output/parquet_output`
- **Delta Lake Storage**: `hdfs://namenode:9000/data/output/delta_output` (with transaction log validation)

```bash
docker exec -it spark spark-submit \
  --jars /extrajars/delta-spark_2.12-3.1.0.jar \
  /data/job2.py
```

#### Pipeline 4: Persistent Parquet-Backed Hive Tables (`data/savetable.py`)
Registers `filtered_transactions` table directly in Hive catalog using Parquet file format. Performs schema verification (`DESCRIBE FORMATTED filtered_transactions`) and table query validation.

```bash
docker exec -it spark spark-submit \
  --jars /opt/spark/jars/postgresql-42.6.0.jar \
  /data/savetable.py
```

---

## Detailed Data Processing Pipeline Mechanics

### Data Ingestion and Transformation Workflow

```
Raw CSV Dataset (data.csv)
       |
       v
HDFS Ingestion (hdfs://namenode:9000/data/data.csv)
       |
       v
Spark DataFrame Reader (Header Infer, Type Inference)
       |
       +-----------------------+-----------------------+
       |                       |                       |
       v                       v                       v
Filter: amount > 43     Filter: amount > 45     Filter: amount > 47
       |                       |                       |
       v                       v                       v
CSV Export              Parquet Export          Delta Lake Export
(HDFS Output Path)      (Columnar Compression)  (ACID Transaction Log)
       |                                               |
       v                                               v
Hive Metastore Sync                             Delta Log Manifest
(PostgreSQL Catalog)                            (_delta_log/)
```

### Technical Concept Specifications

1. **Thrift Protocol Communication**: Spark applications register metadata using Apache Thrift RPC calls sent to `hive-metastore:9083`. The Hive Metastore service translates these requests into SQL DDL commands against the PostgreSQL database.
2. **Parquet Columnar Format**: Parquet organizes data by column rather than row, enabling projection pushdown (reading only required columns) and predicate pushdown (skipping file row groups using min/max statistics).
3. **Delta Lake Protocol**: Delta Lake creates a transaction log folder (`_delta_log/`) containing JSON commit files. This log tracks atomic commits, enables time travel queries, and guarantees serializable isolation during concurrent updates.
4. **Hive Metastore Decoupling**: By running Hive Metastore outside the Spark runtime and storing metadata in PostgreSQL, table definitions persist independently of Spark sessions or cluster restarts.

---

## System Monitoring & Web Interfaces

| Interface | Access URL | Technical Purpose |
| :--- | :--- | :--- |
| **HDFS NameNode UI** | `http://localhost:9870` | Monitors HDFS cluster health, total block count, active DataNodes, and filesystem directory tree. |
| **YARN ResourceManager UI** | `http://localhost:8088` | Displays active cluster computing jobs, application masters, memory utilization, and node health status. |
| **DataNode Web Interface** | `http://localhost:9864` | Displays detailed statistics for individual storage nodes and volume mount capacity. |

---

## Verification & Diagnostic Commands

### HDFS Filesystem Inspection

```bash
# List files in input directory
docker exec -it namenode hdfs dfs -ls /data

# List generated outputs
docker exec -it namenode hdfs dfs -ls /data/output

# Check Hive warehouse directory
docker exec -it namenode hdfs dfs -ls /user/hive/warehouse
```

### PostgreSQL Metastore Inspection

```bash
# Connect to metastore database and list metadata tables
docker exec -it HiveMetastoreDB_postgres psql -U hive -d metastore -c "\dt"

# Query registered Hive tables catalog
docker exec -it HiveMetastoreDB_postgres psql -U hive -d metastore -c "SELECT * FROM \"TBLS\";"
```

---

## Maintainer Information

Repository maintained by **[AviralNITW](https://github.com/AviralNITW)**.
