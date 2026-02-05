# Databricks notebook source
# MAGIC %md
# MAGIC 1) Imports & paramètres

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, date_format, year, month, dayofmonth, weekofyear, dayofweek,
    when, lit, hour, round as f_round, unix_timestamp,
    monotonically_increasing_id, avg as f_avg, sum as f_sum, count as f_count
)

SILVER_TABLE = "trips_clean"

GOLD_DB = "gold_db"
FACT_TABLE = f"{GOLD_DB}.fact_trips"
DIM_DATE = f"{GOLD_DB}.dim_date"
DIM_ZONE = f"{GOLD_DB}.dim_zone"
DIM_PAY = f"{GOLD_DB}.dim_payment_type"
KPI_DAILY = f"{GOLD_DB}.kpi_daily"

# COMMAND ----------

# MAGIC %md
# MAGIC 2) Lecture Silver + filtre qualité

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

df = spark.table(SILVER_TABLE)

# On garde uniquement les trajets valides
df = df.filter(col("is_valid_trip") == 1)

print("Rows Silver valides :", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC 3) Dimension DATE

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.functions import min as f_min, max as f_max, sequence, explode

dates = df.select(to_date(col("pickup_ts")).alias("d"))
min_date = dates.agg(f_min("d")).collect()[0][0]
max_date = dates.agg(f_max("d")).collect()[0][0]

dim_date = (
    spark.createDataFrame([(min_date, max_date)], ["start", "end"])
    .select(explode(sequence(col("start"), col("end"))).alias("date"))
    .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn("month_name", date_format(col("date"), "MMMM"))
    .withColumn("week", weekofyear(col("date")))
    .withColumn("day_of_week", dayofweek(col("date")))  # 1=Sunday .. 7=Saturday
    .withColumn("is_weekend", when(col("day_of_week").isin([1,7]), lit(1)).otherwise(lit(0)))
)

(dim_date.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(DIM_DATE))

print("✅ dim_date créée")

# COMMAND ----------

# MAGIC %md
# MAGIC 4) Dimension PAYMENT TYPE

# COMMAND ----------

payment_map = [
    (1, "Credit Card"),
    (2, "Cash"),
    (3, "No Charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided Trip")
]

dim_payment = spark.createDataFrame(
    payment_map,
    ["payment_type", "payment_label"]
)

(dim_payment.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_db.dim_payment_type")
)

print("✅ gold_db.dim_payment_type créée")

# COMMAND ----------

# MAGIC %md
# MAGIC 5) GOLD dim_zone

# COMMAND ----------

zone_path = "dbfs:/Workspace/Users/mdiedhio@gmail.com/NYC_Yellow_Taxi_Analytics/nyc_taxi_data/bronze_data/taxi_zone_lookup.csv"

dim_zone = (
    spark.read
         .option("header", True)
         .csv(zone_path)
         .withColumn("LocationID", col("LocationID").cast("int"))
)

(dim_zone.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(DIM_ZONE))

print("✅ dim_zone créée")

# COMMAND ----------

# MAGIC %md
# MAGIC 6) fact_trips

# COMMAND ----------

# DBTITLE 1,Cell 11
fact = df.select(
    date_format(to_date(col("pickup_ts")), "yyyyMMdd").cast("int").alias("pickup_date_key"),
    hour(col("pickup_ts")).alias("pickup_hour"),

    col("PULocationID").cast("int").alias("PULocationID"),
    col("DOLocationID").cast("int").alias("DOLocationID"),
    col("payment_type").cast("int").alias("payment_type"),

    col("passenger_count").cast("bigint").alias("passenger_count"),
    col("trip_distance").cast("double").alias("trip_distance"),
    col("fare_amount").cast("double").alias("fare_amount"),
    col("tip_amount").cast("double").alias("tip_amount"),
    col("total_amount").cast("double").alias("total_amount"),

    col("pickup_ts"),
    col("dropoff_ts"),
    col("ingestion_timestamp")
)

# Durée
fact = fact.withColumn(
    "trip_duration_min",
    f_round((unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 60.0, 2)
)

# ID technique
fact = fact.withColumn("trip_id", monotonically_increasing_id())

(fact.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(FACT_TABLE))

print("✅ fact_trips créée")

# COMMAND ----------

# MAGIC %md
# MAGIC 7) kpi_daily

# COMMAND ----------

kpi = (
    fact.groupBy("pickup_date_key")
        .agg(
            f_count(lit(1)).alias("trips"),
            f_sum("total_amount").alias("revenue"),
            f_avg("trip_distance").alias("avg_distance"),
            f_avg("trip_duration_min").alias("avg_duration_min"),
            (f_sum("tip_amount") / f_sum("total_amount")).alias("tip_rate")
        )
)

(kpi.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(KPI_DAILY))

print("✅ kpi_daily créée")

# COMMAND ----------

# MAGIC %md
# MAGIC CREATION de VIEWS

# COMMAND ----------

# DBTITLE 1,Cell 15
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_db.vw_fact_trips AS
# MAGIC SELECT
# MAGIC   trip_id, pickup_date_key, pickup_hour,
# MAGIC   PULocationID, DOLocationID, payment_type,
# MAGIC   passenger_count, trip_distance, fare_amount, tip_amount, total_amount,
# MAGIC   pickup_ts, dropoff_ts, trip_duration_min, ingestion_timestamp
# MAGIC FROM gold_db.fact_trips;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold_db.vw_dim_date AS
# MAGIC SELECT * FROM gold_db.dim_date;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold_db.vw_dim_zone AS
# MAGIC SELECT * FROM gold_db.dim_zone;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold_db.vw_dim_payment_type AS
# MAGIC SELECT * FROM gold_db.dim_payment_type;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold_db.vw_kpi_daily AS
# MAGIC SELECT * FROM gold_db.kpi_daily;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---SHOW TABLES IN gold_db;
# MAGIC