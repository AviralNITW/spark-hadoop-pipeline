# ⚡ Apache Spark & Hadoop Distributed Data Pipeline with Auto-HDFS Automation

An end-to-end, production-grade **Big Data Engineering Pipeline** leveraging **Apache Spark 3.5**, **Hadoop HDFS 3.2**, **YARN Resource Manager**, **Hive Metastore 3.1**, **PostgreSQL**, **Delta Lake 3.1**, and **Apache Parquet**. Includes an automated Python orchestrator script (`autosc.py`) for automated HDFS directory provisioning, dataset ingest, JDBC driver deployment, and container configuration.

---

## 🚀 Key Features & Architectural Highlights

- 🐳 **Containerized Distributed Ecosystem**: Fully orchestrated multi-service architecture using Docker Compose (NameNode, DataNode, Spark 3.5, Hive Metastore, PostgreSQL, YARN ResourceManager, NodeManager).
- 🤖 **Auto-HDFS Ingestion & Setup (`autosc.py`)**: Automated Python script to copy raw data into HDFS (`/data/data.csv`), provision Hive warehouse directories (`/user/hive/warehouse`), apply permissions (`chmod 777`), download JDBC connectors, and sync configuration files (`hive-site.xml`).
- 📊 **Multi-Storage Format Pipelines**:
  - **Apache Parquet**: Columnar storage export with schema enforcement for fast analytics.
  - **Delta Lake 3.1**: ACID transactions, time travel, and scalable data lake storage (`delta-spark_2.12-3.1.0.jar`).
- 🗄️ **Persistent Hive Metastore (PostgreSQL Backed)**: Replaces default embedded Apache Derby with a containerized PostgreSQL database (`HiveMetastoreDB_postgres`) connected via Thrift RPC (`thrift://hive-metastore:9083`).
- ⚡ **PySpark Batch Processing & SQL Queries**: Spark SQL jobs (`job.py`, `job2.py`, `job3.py`, `savetable.py`) executing filtering, aggregate aggregations, temp view generation, and persistent table creation.

---

## 🏗️ System Architecture

```
                    ┌────────────────────────────────────────────────────────┐
                    │             Host Orchestrator (autosc.py)               │
                    └──────────────────────────┬─────────────────────────────┘
                                               │
                                      Auto-Deploy & Ingest
                                               │
               ┌───────────────────────────────┼──────────────────────────────┐
               ▼                               ▼                              ▼
    ┌────────────────────┐          ┌────────────────────┐         ┌────────────────────┐
    │  Hadoop NameNode   │          │   Apache Spark     │         │   Hive Metastore   │
    │   (HDFS Master)    │◄────────►│   Client Node      │◄───────►│  (Thrift Port 9083)│
    │  hdfs://namenode   │          │ (PySpark Executer) │         └─────────┬──────────┘
    └──────────┬─────────┘          └──────────┬─────────┘                   │
               │                               │                   Meta Sync │
               ▼                               ▼                             ▼
    ┌────────────────────┐          ┌────────────────────┐         ┌────────────────────┐
    │  Hadoop DataNode   │          │  YARN Resource     │         │   PostgreSQL DB    │
    │ (HDFS Block Store) │          │     Manager        │         │ (Metastore Engine) │
    └────────────────────┘          └────────────────────┘         └────────────────────┘
```

---

## 🛠️ Tech Stack & Service Matrix

| Component | Technology / Docker Image | Container Name | Port Mapping | Function / Role |
| :--- | :--- | :--- | :--- | :--- |
| **NameNode** | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | `namenode` | `9000`, `9870` | HDFS Master, RPC Server & Web UI |
| **DataNode** | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | `datanode` | `9864` | Distributed HDFS Block Data Storage |
| **Spark Engine** | `apache/spark:3.5.0` | `spark` | Volume Mounts | PySpark Execution & Spark-Submit Client |
| **Hive Metastore**| `apache/hive:3.1.3` | `hive-metastore` | `9083` | Metadata Management over Thrift Protocol |
| **Metastore DB** | `postgres:13` | `HiveMetastoreDB_postgres` | `5432` | Relational Metadata Store for Hive |
| **ResourceManager**| `bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1` | `resourcemanager` | `8088` | YARN Cluster Resource Allocator & Web UI |
| **NodeManager** | `bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1` | `nodemanager` | Dynamic | YARN Worker Node Container Manager |

---

## 📁 Repository Structure

```
spark-hadoop-pipeline/
├── autosc.py                  # Automated setup, HDFS directory provisioning & ingest script
├── docker-compose.yml         # Multi-container cluster configuration file
├── hive-site.xml              # Hive Metastore Thrift & Warehouse connection configuration
├── postgresql-42.6.0.jar      # PostgreSQL JDBC Driver for Hive Metastore
├── spark_test.py              # Test script to verify HDFS reads & writes via PySpark
├── manual_spark_delta_lake_pyscript_working.odt # Technical documentation guide
├── data/
│   ├── data.csv               # Input sample transaction dataset
│   ├── first.py               # Initial PySpark CSV ingestion script
│   ├── job.py                 # Full pipeline: HDFS Read -> Filter -> Hive Table -> Spark SQL
│   ├── job2.py                # Dual pipeline: Parquet & Delta Lake table export
│   ├── job3.py                # PySpark filtering and SQL aggregation job
│   ├── savetable.py           # Spark SQL persistent Hive table creation script
│   └── hive-site.xml          # Container-specific Hive Metastore configuration
└── jars/
    └── delta-spark_2.12-3.1.0.jar # Delta Lake package for PySpark
```

---

## ⚡ Quickstart & Execution Guide

### 1. Prerequisite Requirements
- **Docker** & **Docker Compose** installed on your system.
- **Python 3.x** installed locally.
- Sample dataset (`sample_transactions.csv`) available or use the included `data/data.csv`.

---

### 2. Start the Distributed Cluster
Launch all cluster services using Docker Compose:

```bash
docker-compose up -d
```

Check the status of running containers:
```bash
docker-compose ps
```

---

### 3. Run Automated Setup (`autosc.py`)
Run the Python orchestrator script to automatically configure Hive Metastore, install JDBC drivers, create HDFS directories, set permissions, and copy sample data into HDFS:

```bash
python autosc.py
```

*What `autosc.py` performs under the hood:*
1. Copies local dataset to NameNode `/tmp/data.csv`.
2. Downloads `postgresql-42.6.0.jar` JDBC driver and syncs `hive-site.xml` to Spark container.
3. Provisions HDFS directories `/data` and `/user/hive/warehouse` with full permissions (`777`).
4. Pushes raw CSV into HDFS (`hdfs://namenode:9000/data/data.csv`).
5. Restarts the Spark container for a clean runtime state.

---

### 4. Execute Spark Jobs

#### A. Basic HDFS Read / Write Test (`spark_test.py`)
```bash
docker exec -it spark spark-submit /data/spark_test.py
```

#### B. Full Data Pipeline & Hive Metastore Ingest (`data/job.py`)
```bash
docker exec -it spark spark-submit \
  --jars /extrajars/delta-spark_2.12-3.1.0.jar,/opt/spark/jars/postgresql-42.6.0.jar \
  /data/job.py
```

#### C. Export to Parquet & Delta Lake Formats (`data/job2.py`)
```bash
docker exec -it spark spark-submit \
  --jars /extrajars/delta-spark_2.12-3.1.0.jar \
  /data/job2.py
```

#### D. Create & Query Persistent Hive Tables (`data/savetable.py`)
```bash
docker exec -it spark spark-submit \
  --jars /opt/spark/jars/postgresql-42.6.0.jar \
  /data/savetable.py
```

---

## 🌐 Web Dashboards & UI Links

| Service | Address / URL | Description |
| :--- | :--- | :--- |
| **HDFS NameNode UI** | `http://localhost:9870` | HDFS File System Browser & Cluster Health |
| **YARN ResourceManager UI** | `http://localhost:8088` | Spark & MapReduce Application Status |
| **DataNode Web UI** | `http://localhost:9864` | DataNode Block Stats |

---

## 🔍 Verification & Inspection Commands

### Check Files in HDFS
```bash
docker exec -it namenode hdfs dfs -ls /data
docker exec -it namenode hdfs dfs -ls /data/output
docker exec -it namenode hdfs dfs -ls /user/hive/warehouse
```

### Inspect PostgreSQL Hive Metastore
```bash
docker exec -it HiveMetastoreDB_postgres psql -U hive -d metastore -c "\dt"
```

---

## 🤝 Contributing & Author
Developed and maintained by **[AviralNITW](https://github.com/AviralNITW)**.
