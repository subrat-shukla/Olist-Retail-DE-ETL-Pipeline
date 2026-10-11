# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_etl_project.retail_de;
# MAGIC CREATE VOLUME IF NOT EXISTS retail_etl_project.retail_de.raw_files;

# COMMAND ----------

##Configuration

from pyspark.sql import functions as F

CATALOG  = "retail_etl_project"
SCHEMA   = "retail_de"
BATCH_ID = "batch_1"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files/{BATCH_ID}"

sources = {
    "bronze_customers":  "olist_customers_dataset.csv",
    "bronze_orders":     "olist_orders_dataset.csv",
    "bronze_order_items":"olist_order_items_dataset.csv",
    "bronze_payments":   "olist_order_payments_dataset.csv",
    "bronze_products":   "olist_products_dataset.csv",
    "bronze_sellers":    "olist_sellers_dataset.csv",
}

# COMMAND ----------

#------------------Ingesting each file data------------------

for table_name, file_name in sources.items():
    df = (
        spark.read
             .option("header", True)
             .option("inferSchema", False)      # keeping everything as string
             .csv(f"{RAW_PATH}/{file_name}")
             .withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_source", F.col("_metadata.file_path"))
             .withColumn("_batch_id", F.lit(BATCH_ID))
    )

    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}"))

    print(f"{table_name}: {df.count()} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -------------------------Validation-----------------------
# MAGIC
# MAGIC SELECT 'orders' AS tbl, COUNT(*) AS n FROM retail_etl_project.retail_de.bronze_orders
# MAGIC UNION ALL SELECT 'order_items', COUNT(*) FROM retail_etl_project.retail_de.bronze_order_items
# MAGIC UNION ALL SELECT 'payments',    COUNT(*) FROM retail_etl_project.retail_de.bronze_payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, order_status, order_purchase_timestamp, _ingested_at, _source, _batch_id
# MAGIC FROM retail_etl_project.retail_de.bronze_orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE retail_etl_project.retail_de.bronze_order_items;