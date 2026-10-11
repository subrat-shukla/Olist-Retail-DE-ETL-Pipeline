# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

timestamp_format = "yyyy-MM-dd HH:mm:ss"

df_silver_orders = spark.read.table("retail_etl_project.retail_de.silver_orders")
df_base = df_silver_orders.filter(F.col("_batch_id") == "batch_1")

# COMMAND ----------

# converting base orders to all-string columns, like a real source file, so every batch_2 row can be derived from it.

df_base_str = df_base.select(
    "order_id",
    "customer_id",
    "order_status",
    F.date_format("order_purchase_timestamp", timestamp_format).alias("order_purchase_timestamp"),
    F.date_format("order_approved_at", timestamp_format).alias("order_approved_at"),
    F.date_format("order_delivered_carrier_date", timestamp_format).alias("order_delivered_carrier_date"),
    F.date_format("order_delivered_customer_date", timestamp_format).alias("order_delivered_customer_date"),
    F.date_format("order_estimated_delivery_date", timestamp_format).alias("order_estimated_delivery_date"),
)

# COMMAND ----------

#building two kinds of updates

df_upd_delivered = (
    df_base_str.filter(F.col("order_status") == "shipped")
    .orderBy("order_id").limit(3)
    .withColumn("order_status", F.lit("delivered"))
    .withColumn("order_delivered_customer_date", F.col("order_estimated_delivery_date"))
    .withColumn("kind", F.lit("update_shipped_to_delivered"))
)

df_upd_canceled = (
    df_base_str.filter(F.col("order_status") == "processing")
    .orderBy("order_id").limit(2)
    .withColumn("order_status", F.lit("canceled"))
    .withColumn("kind", F.lit("update_processing_to_canceled"))
)

# COMMAND ----------

#building new orders, all under (new_) ids with real customers.
df_new_delivered = (
    df_base_str.filter(F.col("order_status") == "delivered")
    .orderBy("order_id").limit(2)
    .withColumn("order_id", F.concat(F.lit("new_"), F.col("order_id")))
    .withColumn("kind", F.lit("new_delivered"))
)

df_new_created = (
    df_base_str.filter(F.col("order_status") == "canceled")
    .orderBy("order_id").limit(1)
    .withColumn("order_id", F.concat(F.lit("new_"), F.col("order_id")))
    .withColumn("order_status", F.lit("created"))
    .withColumn("order_approved_at", F.lit(None).cast("string"))
    .withColumn("order_delivered_carrier_date", F.lit(None).cast("string"))
    .withColumn("order_delivered_customer_date", F.lit(None).cast("string"))
    .withColumn("kind", F.lit("new_created_with_nulls"))
)

# COMMAND ----------

#exact duplicates and the bad rows.
df_dup = (
    df_new_delivered.limit(1)
    .withColumn("kind", F.lit("duplicate_in_batch"))
)

df_bad = (
    df_base_str.filter(F.col("order_status") == "delivered")
    .orderBy(F.col("order_id").desc()).limit(1)
    .withColumn("order_id", F.lit(None).cast("string"))
    .withColumn("kind", F.lit("bad_null_order_id"))
)

# COMMAND ----------

#stack everything, write batch_2 as CSV without the kind label, and show all 10 rows.
df_batch2 = (
    df_upd_delivered
    .unionByName(df_upd_canceled)
    .unionByName(df_new_delivered)
    .unionByName(df_new_created)
    .unionByName(df_dup)
    .unionByName(df_bad)
)

(df_batch2.drop("kind").coalesce(1).write
    .mode("overwrite")
    .option("header", True)
    .csv("/Volumes/retail_etl_project/retail_de/raw_files/batch_2/orders"))

df_batch2.select("kind", "order_id", "order_status", "order_delivered_customer_date").show(truncate=False)
print("batch_2 rows:", df_batch2.count())

# COMMAND ----------

#reading batch 2 folder
df_bronze_batch2 = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .load("/Volumes/retail_etl_project/retail_de/raw_files/batch_2/orders")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit("batch_2"))
)

print("batch_2 rows read:", df_bronze_batch2.count())

# COMMAND ----------

#delete any earlier batch_2 rows from Bronze, then append this batch.
from delta.tables import DeltaTable

DeltaTable.forName(spark, "retail_etl_project.retail_de.bronze_orders") \
          .delete("_batch_id = 'batch_2'")

(df_bronze_batch2.write.format("delta").mode("append")
    .saveAsTable("retail_etl_project.retail_de.bronze_orders"))

df_bronze_check = spark.read.table("retail_etl_project.retail_de.bronze_orders")
print("bronze_orders total rows:", df_bronze_check.count())
print("batch_2 rows in bronze: ", df_bronze_check.filter(F.col("_batch_id") == "batch_2").count())

# COMMAND ----------

#reading only the batch_2 rows from Bronze and clean and type them like Silver, keeping the raw purchase date.

df_bronze_orders_b2 = (
    spark.read.table("retail_etl_project.retail_de.bronze_orders")
    .filter(F.col("_batch_id") == "batch_2")
)

df_typed_orders_b2 = df_bronze_orders_b2.select(
    F.when(F.trim("order_id") == "", None).otherwise(F.trim("order_id")).alias("order_id"),
    F.when(F.trim("customer_id") == "", None).otherwise(F.trim("customer_id")).alias("customer_id"),
    F.lower(F.trim("order_status")).alias("order_status"),
    F.col("order_purchase_timestamp").alias("purchase_raw"),
    F.try_to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
    F.try_to_timestamp("order_approved_at").alias("order_approved_at"),
    F.try_to_timestamp("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
    F.try_to_timestamp("order_delivered_customer_date").alias("order_delivered_customer_date"),
    F.try_to_timestamp("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
    "_ingested_at", "_source", "_batch_id"
)

# COMMAND ----------

#tagging each row with a reject reason
df_tagged_orders_b2 = df_typed_orders_b2.withColumn(
    "reject_reason",
    F.when(F.col("order_id").isNull(), "null_order_id")
     .when(F.col("customer_id").isNull(), "null_customer_id")
     .when(F.col("purchase_raw").isNotNull() & F.col("order_purchase_timestamp").isNull(),
           "invalid_purchase_date")
     .otherwise(None)
)

# COMMAND ----------

#split into rejected and clean rows, then deduplicate the clean rows on order_id
df_quarantine_orders_b2 = df_tagged_orders_b2.filter(F.col("reject_reason").isNotNull())

df_clean_orders_b2 = (
    df_tagged_orders_b2
    .filter(F.col("reject_reason").isNull())
    .drop("reject_reason", "purchase_raw")
)

window_orders_b2 = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())

df_source_orders_b2 = (
    df_clean_orders_b2
    .withColumn("rn", F.row_number().over(window_orders_b2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("batch_2 rows:      ", df_tagged_orders_b2.count())
print("quarantined:       ", df_quarantine_orders_b2.count())
print("duplicates removed:", df_clean_orders_b2.count() - df_source_orders_b2.count())
print("rows to merge:     ", df_source_orders_b2.count())

# COMMAND ----------

#saving the rejected row to quarantine_orders, deleting any earlier batch_2 rejects first so a re-run doesn’t double them.
DeltaTable.forName(spark, "retail_etl_project.retail_de.quarantine_orders") \
          .delete("_batch_id = 'batch_2'")

(df_quarantine_orders_b2.write.format("delta").mode("append")
    .saveAsTable("retail_etl_project.retail_de.quarantine_orders"))

# COMMAND ----------

#runing MERGE, updating matched orders only when the source is newer, and inserting the rest.
silver_target = DeltaTable.forName(spark, "retail_etl_project.retail_de.silver_orders")

(silver_target.alias("t")
    .merge(df_source_orders_b2.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")
    .whenNotMatchedInsertAll()
    .execute())

metrics = silver_target.history(1).select("operationMetrics").first()[0]
print("updated: ", metrics["numTargetRowsUpdated"])
print("inserted:", metrics["numTargetRowsInserted"])
print("silver_orders rows:", spark.read.table("retail_etl_project.retail_de.silver_orders").count())

# COMMAND ----------

#looking at the 8 merged orders in Silver to confirm the updates and inserts.
df_merged_ids = df_source_orders_b2.select("order_id")

(spark.read.table("retail_etl_project.retail_de.silver_orders")
    .join(df_merged_ids, "order_id")
    .select("order_id", "order_status", "order_delivered_customer_date", "_batch_id")
    .orderBy("order_id")
    .show(truncate=False))

# COMMAND ----------

#record the Silver state before the second run
df_silver_before = spark.read.table("retail_etl_project.retail_de.silver_orders")

rows_before = df_silver_before.count()
fingerprint_before = (
    df_silver_before.join(df_merged_ids, "order_id")
    .agg(F.sum(F.hash("order_id", "order_status", "order_delivered_customer_date", "_ingested_at")))
    .first()[0]
)
print("rows before:       ", rows_before)
print("fingerprint before:", fingerprint_before)

# COMMAND ----------

#run the MERGE again with the same source DataFrame and print the metrics.
(silver_target.alias("t")
    .merge(df_source_orders_b2.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")
    .whenNotMatchedInsertAll()
    .execute())

metrics2 = silver_target.history(1).select("operationMetrics").first()[0]
print("updated: ", metrics2["numTargetRowsUpdated"])
print("inserted:", metrics2["numTargetRowsInserted"])

# COMMAND ----------

#record the state after, and compare with before.
df_silver_after = spark.read.table("retail_etl_project.retail_de.silver_orders")

rows_after = df_silver_after.count()
fingerprint_after = (
    df_silver_after.join(df_merged_ids, "order_id")
    .agg(F.sum(F.hash("order_id", "order_status", "order_delivered_customer_date", "_ingested_at")))
    .first()[0]
)
print("rows after:        ", rows_after)
print("fingerprint after: ", fingerprint_after)
print("same rows:         ", rows_before == rows_after)
print("same data:         ", fingerprint_before == fingerprint_after)

# COMMAND ----------

#-----results of incremental load----
#compare statuses for an updated order in Silver and in the Gold fact table.
df_fact = spark.read.table("retail_etl_project.retail_de.fact_order_items")
df_silver_now = spark.read.table("retail_etl_project.retail_de.silver_orders")

df_check_ids = df_merged_ids.filter(~F.col("order_id").startswith("new_"))

(df_silver_now.join(df_check_ids, "order_id").select("order_id", F.col("order_status").alias("silver_status"))
    .join(df_fact.select("order_id", F.col("order_status").alias("fact_status")).distinct(), "order_id", "left")
    .show(truncate=False))