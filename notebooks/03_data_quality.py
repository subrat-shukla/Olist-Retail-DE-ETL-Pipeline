# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ***Data Quality and Quarantine***

# COMMAND ----------

#-------------------building bad order rows explicitly---------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_bronze_orders = spark.read.table("retail_etl_project.retail_de.bronze_orders")

df_original = df_bronze_orders.filter(F.col("_batch_id") == "batch_1").first()
existing_order_id    = df_original["order_id"]
existing_customer_id = df_original["customer_id"]

bad_orders_data = [
    (None, "cust_bad_1", "delivered", "2018-01-01 10:00:00", None, None, None, "2018-02-01 00:00:00"),
    ("order_bad_2", "", "delivered", "2018-01-01 10:00:00", None, None, None, "2018-02-01 00:00:00"),
    (existing_order_id, existing_customer_id, "shipped",   "2018-01-01 10:00:00", None, None, None, "2018-02-01 00:00:00"),
    ("order_bad_4", "cust_bad_4", "delivered", "not a date", None, None, None, "2018-02-01 00:00:00"),
]

df_bad_orders = (
    spark.createDataFrame(bad_orders_data, schema=df_bronze_orders.select(
        "order_id", "customer_id", "order_status", "order_purchase_timestamp",
        "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date").schema)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("injected_test_rows"))
    .withColumn("_batch_id", F.lit("bad_test"))
)

df_bad_orders.show(truncate=False)

# COMMAND ----------

df_bronze_order_items = spark.read.table("retail_etl_project.retail_de.bronze_order_items")

bad_items_data = [
    ("order_bad_5", "1", "prod_x", "seller_x", "2018-01-01 10:00:00", "-10",  "5.0"),
    ("order_bad_6", "1", None, "seller_x", "2018-01-01 10:00:00", "20.0", "5.0"),
    ("order_bad_7", "1", "prod_x", "seller_x", "2018-01-01 10:00:00", "abc",  "5.0"),
]

df_bad_items = (
    spark.createDataFrame(bad_items_data, schema=df_bronze_order_items.select(
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value").schema)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("injected_test_rows"))
    .withColumn("_batch_id", F.lit("bad_test"))
)

df_bad_items.show(truncate=False)

# COMMAND ----------

# building idemptonce by delete-add pattern

from delta.tables import DeltaTable

for tbl in ["bronze_orders", "bronze_order_items"]:
    DeltaTable.forName(spark, f"retail_etl_project.retail_de.{tbl}") \
              .delete("_batch_id = 'bad_test'")

df_bad_orders.write.format("delta").mode("append") \
    .saveAsTable("retail_etl_project.retail_de.bronze_orders")
df_bad_items.write.format("delta").mode("append") \
    .saveAsTable("retail_etl_project.retail_de.bronze_order_items")

print("bronze_orders rows: ", spark.read.table("retail_etl_project.retail_de.bronze_orders").count())
print("bronze_order_items rows: ", spark.read.table("retail_etl_project.retail_de.bronze_order_items").count())

# COMMAND ----------

#--I kept one record to make a conflict for one order

(spark.read.table("retail_etl_project.retail_de.bronze_orders")
    .filter(F.col("order_id") == existing_order_id)
    .select("order_id", "order_status", "_ingested_at", "_batch_id")
    .show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC Adding rows with a reason to go to quarantine tables

# COMMAND ----------

df_bronze_orders = spark.read.table("retail_etl_project.retail_de.bronze_orders")

df_typed_orders = df_bronze_orders.select(
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

df_tagged_orders = df_typed_orders.withColumn(
    "reject_reason",
    F.when(F.col("order_id").isNull(), "null_order_id")
     .when(F.col("customer_id").isNull(), "null_customer_id")
     .when(F.col("purchase_raw").isNotNull() & F.col("order_purchase_timestamp").isNull(),
           "invalid_purchase_date")
     .otherwise(None)
)

# COMMAND ----------

#--------------Splitting into rejected and clean orders and deduplicating clean rows------------

df_quarantine_orders = df_tagged_orders.filter(F.col("reject_reason").isNotNull())

df_clean_orders = (
    df_tagged_orders
    .filter(F.col("reject_reason").isNull())
    .drop("reject_reason", "purchase_raw")
)

window_orders = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())

df_silver_orders = (
    df_clean_orders
    .withColumn("rn", F.row_number().over(window_orders))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

(df_quarantine_orders.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.quarantine_orders"))

(df_silver_orders.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.silver_orders"))

print("bronze rows:     ", df_bronze_orders.count())
print("quarantined rows:", df_quarantine_orders.count())
print("duplicates removed:", df_clean_orders.count() - df_silver_orders.count())
print("silver rows:     ", df_silver_orders.count())

# COMMAND ----------

spark.read.table("retail_etl_project.retail_de.quarantine_orders") \
     .select("order_id", "customer_id", "purchase_raw", "reject_reason") \
     .show(truncate=False)

dup_id = existing_order_id

(spark.read.table("retail_etl_project.retail_de.silver_orders")
    .filter(F.col("order_id") == dup_id)
    .select("order_id", "customer_id", "order_status", "_batch_id")
    .show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC Adding rows with a reason to go to quarantine tables (silver order items)

# COMMAND ----------

#-----------------Standardizing and casting columns------------

df_bronze_order_items = spark.read.table("retail_etl_project.retail_de.bronze_order_items")

df_typed_order_items = df_bronze_order_items.select(
    F.when(F.trim("order_id") == "", None).otherwise(F.trim("order_id")).alias("order_id"),
    F.col("order_item_id").try_cast("int").alias("order_item_id"),
    F.when(F.trim("product_id") == "", None).otherwise(F.trim("product_id")).alias("product_id"),
    F.when(F.trim("seller_id") == "", None).otherwise(F.trim("seller_id")).alias("seller_id"),
    F.try_to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
    F.col("price").alias("price_raw"),
    F.col("price").try_cast("double").alias("price"),
    F.col("freight_value").try_cast("double").alias("freight_value"),
    "_ingested_at", "_source", "_batch_id"
)

# COMMAND ----------

#------------------Adding reject reason-------------------

df_tagged_order_items = df_typed_order_items.withColumn(
    "reject_reason",
    F.when(F.col("order_id").isNull(), "null_order_id")
     .when(F.col("product_id").isNull(), "null_product_id")
     .when(F.col("seller_id").isNull(), "null_seller_id")
     .when(F.col("price_raw").isNotNull() & F.col("price").isNull(), "invalid_price")
     .when(F.col("price").isNull(), "null_price")
     .when(F.col("price") < 0, "negative_price")
     .when(F.col("freight_value") < 0, "negative_freight")
     .otherwise(None)
)

# COMMAND ----------

#--------------Splitting into rejected and clean orders and deduplicating clean rows------------

df_quarantine_order_items = df_tagged_order_items.filter(F.col("reject_reason").isNotNull())

df_clean_order_items = (
    df_tagged_order_items
    .filter(F.col("reject_reason").isNull())
    .drop("reject_reason", "price_raw")
)

window_order_items = (
    Window.partitionBy("order_id", "order_item_id")
          .orderBy(F.col("_ingested_at").desc())
)

df_silver_order_items = (
    df_clean_order_items
    .withColumn("rn", F.row_number().over(window_order_items))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

(df_quarantine_order_items.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.quarantine_order_items"))

(df_silver_order_items.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.silver_order_items"))

print("bronze rows:       ", df_bronze_order_items.count())
print("quarantined rows:  ", df_quarantine_order_items.count())
print("duplicates removed:", df_clean_order_items.count() - df_silver_order_items.count())
print("silver rows:       ", df_silver_order_items.count())

# COMMAND ----------

(spark.read.table("retail_etl_project.retail_de.quarantine_order_items")
    .select("order_id", "product_id", "price_raw", "reject_reason")
    .show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Quality Summary**

# COMMAND ----------

# count rejected rows per reason in each quarantine table and stack the two results into one DataFrame.

df_q_orders = spark.read.table("retail_etl_project.retail_de.quarantine_orders")
df_q_items  = spark.read.table("retail_etl_project.retail_de.quarantine_order_items")

df_summary_orders = (
    df_q_orders
    .groupBy("reject_reason")
    .agg(F.count("*").alias("rejected_rows"))
    .withColumn("table_name", F.lit("orders"))
)

df_summary_items = (
    df_q_items
    .groupBy("reject_reason")
    .agg(F.count("*").alias("rejected_rows"))
    .withColumn("table_name", F.lit("order_items"))
)

df_dq_summary = (
    df_summary_orders.unionByName(df_summary_items)
    .select("table_name", "reject_reason", "rejected_rows")
    .orderBy("table_name", "reject_reason")
)

# COMMAND ----------

#---saving the summary as a Delta table

(df_dq_summary.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.dq_summary"))

spark.read.table("retail_etl_project.retail_de.dq_summary").show(truncate=False)

# COMMAND ----------

#------reconciling the counts for each table, checking that Bronze rows equal Silver plus quarantined plus duplicates removed

df_silver_orders = spark.read.table("retail_etl_project.retail_de.silver_orders")
df_silver_order_items = spark.read.table("retail_etl_project.retail_de.silver_order_items")
df_bronze_orders = spark.read.table("retail_etl_project.retail_de.bronze_orders")
df_bronze_items = spark.read.table("retail_etl_project.retail_de.bronze_order_items")

print("orders---->>  bronze", df_bronze_orders.count(),
      "= silver", df_silver_orders.count(),
      "+ quarantine", df_q_orders.count(),
      "+ duplicates", df_bronze_orders.count() - df_silver_orders.count() - df_q_orders.count())

print("order_items--->>  bronze", df_bronze_items.count(),
      "= silver", df_silver_order_items.count(),
      "+ quarantine", df_q_items.count(),
      "+ duplicates", df_bronze_items.count() - df_silver_order_items.count() - df_q_items.count())

# COMMAND ----------

