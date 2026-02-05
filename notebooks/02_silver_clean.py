# Databricks notebook source
from pyspark.sql.functions import (
    col, expr, when, lit,
    to_date, hour, round as f_round,
    unix_timestamp
)

BRONZE_TABLE = "bronze_db.raw_trips"
SILVER_TABLE = "trips_clean"   #

# COMMAND ----------

# MAGIC %md
# MAGIC Lecture Bronze + sélection colonnes (normalisation)

# COMMAND ----------

df = spark.table(BRONZE_TABLE)

# Normalisation : garder uniquement les colonnes attendues
silver = df.select(
    col("VendorID").cast("int").alias("VendorID"),
    col("tpep_pickup_datetime").alias("pickup_ts"),
    col("tpep_dropoff_datetime").alias("dropoff_ts"),
    col("passenger_count").cast("bigint").alias("passenger_count"),
    col("trip_distance").cast("double").alias("trip_distance"),
    col("RatecodeID").cast("bigint").alias("RatecodeID"),
    col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
    col("PULocationID").cast("int").alias("PULocationID"),
    col("DOLocationID").cast("int").alias("DOLocationID"),
    col("payment_type").cast("bigint").alias("payment_type"),
    col("fare_amount").cast("double").alias("fare_amount"),
    col("extra").cast("double").alias("extra"),
    col("mta_tax").cast("double").alias("mta_tax"),
    col("tip_amount").cast("double").alias("tip_amount"),
    col("tolls_amount").cast("double").alias("tolls_amount"),
    col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
    col("total_amount").cast("double").alias("total_amount"),
    col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
    col("Airport_fee").cast("double").alias("Airport_fee"),
    col("cbd_congestion_fee").cast("double").alias("cbd_congestion_fee"),
    col("file_name").cast("string").alias("file_name"),
    col("ingestion_timestamp").alias("ingestion_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Colonnes dérivées + règles qualité

# COMMAND ----------

# MAGIC %md
# MAGIC A) Durée

# COMMAND ----------

silver = silver.withColumn(
    "trip_duration_min",
    f_round((unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 60.0, 2)
)

# COMMAND ----------

# MAGIC %md
# MAGIC B) Date / heures utiles

# COMMAND ----------

silver = (silver
          .withColumn("pickup_date", to_date(col("pickup_ts")))
          .withColumn("pickup_hour", hour(col("pickup_ts")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC C) Règles qualité

# COMMAND ----------

silver = (silver
    .withColumn("is_valid_time", when(col("trip_duration_min") > 0, lit(1)).otherwise(lit(0)))
    .withColumn("is_valid_distance", when(col("trip_distance") >= 0, lit(1)).otherwise(lit(0)))
    .withColumn("is_valid_amount", when(col("total_amount") > 0, lit(1)).otherwise(lit(0)))
)

silver = silver.withColumn(
    "is_valid_trip",
    when(
        (col("is_valid_time") == 1) &
        (col("is_valid_distance") == 1) &
        (col("is_valid_amount") == 1),
        lit(1)
    ).otherwise(lit(0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 4) Écriture Silver (overwrite)

# COMMAND ----------

(silver.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(SILVER_TABLE))

print("✅ Silver créée :", SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC 5) Contrôles rapides

# COMMAND ----------

print("Rows Silver:", spark.table(SILVER_TABLE).count())
spark.table(SILVER_TABLE).select("is_valid_trip").groupBy("is_valid_trip").count().show()
display(spark.table(SILVER_TABLE).limit(10))