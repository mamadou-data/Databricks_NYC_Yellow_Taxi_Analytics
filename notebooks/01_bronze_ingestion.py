# Databricks notebook source
from pyspark.sql.functions import (
    col,
    current_timestamp,
    regexp_extract,
    count as f_count
)
import re

# COMMAND ----------

BRONZE_DB = "bronze_db"
BRONZE_TABLE = f"{BRONZE_DB}.raw_trips"
LOG_TABLE = f"{BRONZE_DB}.ingestion_log"

SOURCE_FOLDER = "dbfs:/Workspace/Users/mdiedhio@gmail.com/NYC_Yellow_Taxi_Analytics/nyc_taxi_data/bronze_data/"

# COMMAND ----------

all_files = [
    f for f in dbutils.fs.ls(SOURCE_FOLDER)
    if f.name.startswith("yellow_tripdata_2025-") and f.name.endswith(".parquet")
]

pattern = re.compile(r"^yellow_tripdata_2025-\d{2}\.parquet$")
valid_files = [f for f in all_files if pattern.match(f.name)]

print("Valides :", len(valid_files))
print([f.name for f in valid_files])

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ CELL 1 — SQL : Base + Tables

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit, input_file_name, regexp_extract
from pyspark.sql.types import *
import re

BRONZE_DB = "bronze_db"
BRONZE_TABLE = f"{BRONZE_DB}.raw_trips"
LOG_TABLE = f"{BRONZE_DB}.ingestion_log"
source_folder = "dbfs:/Workspace/Users/mdiedhio@gmail.com/NYC_Yellow_Taxi_Analytics/nyc_taxi_data/bronze_data/"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
  VendorID INT,
  tpep_pickup_datetime TIMESTAMP_NTZ,
  tpep_dropoff_datetime TIMESTAMP_NTZ,
  passenger_count BIGINT,
  trip_distance DOUBLE,
  RatecodeID BIGINT,
  store_and_fwd_flag STRING,
  PULocationID INT,
  DOLocationID INT,
  payment_type BIGINT,
  fare_amount DOUBLE,
  extra DOUBLE,
  mta_tax DOUBLE,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  improvement_surcharge DOUBLE,
  total_amount DOUBLE,
  congestion_surcharge DOUBLE,
  Airport_fee DOUBLE,
  cbd_congestion_fee DOUBLE,
  ingestion_timestamp TIMESTAMP_NTZ,
  file_name STRING,
  file_path STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
  file_name STRING,
  ingested_at TIMESTAMP_NTZ,
  row_count BIGINT
) USING DELTA
""")

# COMMAND ----------


%fs ls /Workspace/Users/mdiedhio@gmail.com/NYC_Yellow_Taxi_Analytics/nyc_taxi_data/bronze_data/

# COMMAND ----------

# DBTITLE 1,Cell 5
if spark.catalog.tableExists(LOG_TABLE):
    ingested = set(
        r["file_name"]
        for r in spark.table(LOG_TABLE).select("file_name").distinct().collect()
    )
else:
    ingested = set()

to_process = [f for f in valid_files if f.name not in ingested]

print("Déjà ingérés :", len(ingested))
print("À traiter :", len(to_process))
print([f.name for f in to_process])

# COMMAND ----------

if to_process:
    paths = [f.path for f in to_process]

    df = spark.read.parquet(*paths)

    # Normalisation des noms (sécurité)
    if "vendor_id" in df.columns and "VendorID" not in df.columns:
        df = df.withColumnRenamed("vendor_id", "VendorID")
    if "PULocation_ID" in df.columns and "PULocationID" not in df.columns:
        df = df.withColumnRenamed("PULocation_ID", "PULocationID")
    if "DOLocation_ID" in df.columns and "DOLocationID" not in df.columns:
        df = df.withColumnRenamed("DOLocation_ID", "DOLocationID")

    # UC-friendly metadata
    df = (df
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("file_path", col("_metadata.file_path"))
          .withColumn("file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+$)", 1))
    )

    df.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, expr

# ✅ Métadonnées UC + CAST NTZ (CRITIQUE)

df = (df
      .withColumn("ingestion_timestamp", current_timestamp())
      # ✅ force en TIMESTAMP_NTZ pour matcher la table
      .withColumn("ingestion_timestamp", expr("CAST(ingestion_timestamp AS TIMESTAMP_NTZ)"))
      .withColumn("file_path", col("_metadata.file_path"))
      .withColumn("file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+$)", 1))
)

# COMMAND ----------

#Écriture Bronze

(df.write.format("delta")
 .mode("append")
 .option("mergeSchema", "false")
 .saveAsTable(BRONZE_TABLE))


print("✅ Bronze ingéré + log mis à jour")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_db.raw_trips;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_total FROM bronze_db.raw_trips;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT file_name) AS files FROM bronze_db.raw_trips;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_db.ingestion_log ORDER BY file_name;

# COMMAND ----------

# MAGIC %md
# MAGIC