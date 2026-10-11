# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import types as T
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver orders table**

# COMMAND ----------

df_bronze_orders = spark.read.format("delta").table("retail_etl_project.retail_de.bronze_orders")
print("bronze rows : ",df_bronze_orders.count())

# COMMAND ----------

df_bronze_orders.show()

# COMMAND ----------

#------------Standardizing and casting the columns----------
df_typed_orders = df_bronze_orders.select(
    when(trim("order_id") == "", None).otherwise(trim("order_id")).alias("order_id"),
    when(trim("customer_id") == "", None).otherwise(trim("customer_id")).alias("customer_id"),
    lower(trim("order_status")).alias("order_status"),
    try_to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
    try_to_timestamp("order_approved_at").alias("order_approved_at"),
    try_to_timestamp("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
    try_to_timestamp("order_delivered_customer_date").alias("order_delivered_customer_date"),
    try_to_timestamp("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
    "_ingested_at", "_source", "_batch_id"
)

# COMMAND ----------

# DBTITLE 1,Validate and deduplicate
#-----------------Validate and deduplicate---------------
df_valid_orders = df_typed_orders.filter(col("order_id").isNotNull() & col("customer_id").isNotNull())
#print("valid rows : ",df_valid_orders.count())

window_orders = Window.partitionBy("order_id").orderBy(col("_ingested_at").desc())

df_silver_orders = df_valid_orders.withColumn("rn", row_number().over(window_orders)).filter("rn = 1").drop("rn")

# COMMAND ----------

df_silver_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_etl_project.retail_de.silver_orders")

print("rows failing validation : ", df_typed_orders.count() - df_silver_orders.count())
print("duplicates removed : ", df_valid_orders.count() - df_silver_orders.count())
print("silver table rows : ", spark.read.table("retail_etl_project.retail_de.silver_orders").count())

# COMMAND ----------

#--------------------------Validation-------------------------

df_silver_orders_check = spark.read.table("retail_etl_project.retail_de.silver_orders")

# Check 1: column types (the 5 date columns should be timestamp)
df_silver_orders_check.printSchema()

# Check 2: counts and null dates
df_silver_orders_check.agg(
    count("*").alias("total"),
    countDistinct("order_id").alias("distinct_orders"),
    sum(when(col("order_delivered_customer_date").isNull(), 1).otherwise(0)).alias("no_delivery_date"),
    sum(when(col("order_purchase_timestamp").isNull(), 1).otherwise(0)).alias("no_purchase_date"),
).show()

# Check 3: orders by status
(df_silver_orders_check
    .groupBy("order_status")
    .count()
    .orderBy(col("count").desc())
    .show())

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver order_items table**

# COMMAND ----------

df_bronze_order_items = spark.read.format("delta").table("retail_etl_project.retail_de.bronze_order_items")
print("bronze order items rows : ", df_bronze_order_items.count())

df_bronze_order_items.show()

# COMMAND ----------

#--------------------------Standardizing and casting columns------------------------
df_typed_order_items = df_bronze_order_items.select(
    when(trim("order_id") == "", None).otherwise(trim("order_id")).alias("order_id"),
    when(trim("order_item_id") == "", None).otherwise(trim("order_item_id")).alias("order_item_id"),
    when(trim("product_id") == "", None).otherwise(trim("product_id")).alias("product_id"),
    when(trim("seller_id") == "", None).otherwise(trim("seller_id")).alias("seller_id"),
    try_to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
    col("price").try_cast("double").alias("price"),
    col("freight_value").try_cast("double").alias("freight_value"),
    "_ingested_at", "_source", "_batch_id"
)

# COMMAND ----------

#------------------------Validate and deduplicate------------
df_valid_order_items = df_typed_order_items.filter(
    col("order_id").isNotNull()
    & col("order_item_id").isNotNull() 
    & col("product_id").isNotNull() 
    & col("seller_id").isNotNull()
    & col("price").isNotNull()
    & ((col("price")>=0) & (col("freight_value")>=0))
)
#print("valid rows : ",df_valid_order_items.count())

window_order_items = Window.partitionBy("order_id", "order_item_id").orderBy(col("_ingested_at").desc())

#-----deduplication------
df_silver_order_items = df_valid_order_items.withColumn("rn", row_number().over(window_order_items)).filter("rn = 1").drop("rn")

# COMMAND ----------

df_silver_order_items.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("retail_etl_project.retail_de.silver_order_items")

print("rows failing validation : ", df_typed_order_items.count() - df_silver_order_items.count())
print("duplicates removed : ", df_valid_order_items.count() - df_silver_order_items.count())
print("silver table rows : ", spark.read.table("retail_etl_project.retail_de.silver_order_items").count())

# COMMAND ----------

#-----------------------Validation--------------------
df_silver_order_items_check = spark.read.table("retail_etl_project.retail_de.silver_order_items")

# Check 1: column types (the 5 date columns should be timestamp)
df_silver_order_items_check.printSchema()

# Check 2: counts and null dates
df_silver_order_items_check.agg(
    count("*").alias("total"),
    countDistinct("order_id", "order_item_id").alias("distinct_orders"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    min("freight_value").alias("min_freight_value"),
    max("freight_value").alias("max_freight_value")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver payments table**

# COMMAND ----------

df_bronze_payments = spark.read.format("delta").table("retail_etl_project.retail_de.bronze_payments")
print("bronze payments rows : ", df_bronze_payments.count())

# COMMAND ----------

#---------------------Standardizing and casting columns------------------------
df_typed_payments = df_bronze_payments.select(
    when(trim("order_id") == "", None).otherwise(trim("order_id")).alias("order_id"),
    col("payment_sequential").try_cast("int").alias("payment_sequential"),
    lower(trim("payment_type")).alias("payment_type"),
    col("payment_installments").try_cast("int").alias("payment_installments"),
    col("payment_value").try_cast("double").alias("payment_value"),
    "_ingested_at", "_source", "_batch_id"
)

# COMMAND ----------

#--------------------Validate and Deduplicate----------------
df_valid_payments = df_typed_payments.filter(
    col("order_id").isNotNull()
    & col("payment_sequential").isNotNull()
    & col("payment_type").isNotNull()
    & ((col("payment_value").isNotNull()) & (col("payment_value")>=0))
)

window_payments = Window.partitionBy("order_id", "payment_sequential").orderBy(col("_ingested_at").desc())

#-----deduplication------
df_silver_payments = df_valid_payments.withColumn("rn", row_number().over(window_payments)).filter("rn = 1").drop("rn")

# COMMAND ----------

(df_silver_payments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.silver_payments"))

print("rows failing validation:", df_typed_payments.count() - df_valid_payments.count())
print("duplicates removed:     ", df_valid_payments.count() - df_silver_payments.count())
print("silver table rows:      ", spark.read.table("retail_etl_project.retail_de.silver_payments").count())

# COMMAND ----------

#-------------------------Validation------------------------
df_silver_payments_check = spark.read.table("retail_etl_project.retail_de.silver_payments")

df_silver_payments_check.printSchema()

df_silver_payments_check.agg(
    count("*").alias("total"),
    countDistinct("order_id", "payment_sequential").alias("distinct_keys"),
    countDistinct("order_id").alias("distinct_orders"),
    min("payment_value").alias("min_value"),
    max("payment_value").alias("max_value"),
).show()

(df_silver_payments_check
    .groupBy("payment_type")
    .count()
    .orderBy(col("count").desc())
    .show())

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver customers table**

# COMMAND ----------

df_bronze_customers = spark.read.format("delta").table("retail_etl_project.retail_de.bronze_customers")

print("bronze customers rows : ", df_bronze_customers.count())

# COMMAND ----------

#-----------------------Standardizing and casting columns-----------------
df_typed_customers = df_bronze_customers.select(
    when(trim("customer_id") == "", None).otherwise(trim("customer_id")).alias("customer_id"),
    when(trim("customer_unique_id") == "", None).otherwise(trim("customer_unique_id")).alias("customer_unique_id"),
    trim("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
    lower(trim("customer_city")).alias("customer_city"),
    upper(trim("customer_state")).alias("customer_state"),
    "_ingested_at", "_source", "_batch_id"
)

# COMMAND ----------

#--------------------Validate and Deduplicate----------------
df_valid_customers = df_typed_customers.filter(
    col("customer_id").isNotNull() & col("customer_unique_id").isNotNull()
)

window_customers = Window.partitionBy("customer_id").orderBy(col("_ingested_at").desc())

#deduplication
df_silver_customers = (
    df_valid_customers.withColumn("rn", row_number().over(window_customers)).filter(col("rn")==1).drop("rn"))


# COMMAND ----------

(df_silver_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.silver_customers"))

print("rows failing validation : ", df_typed_customers.count() - df_valid_customers.count())
print("duplicates removed : ", df_valid_customers.count() - df_silver_customers.count())
print("silver table rows : ", spark.read.table("retail_etl_project.retail_de.silver_customers").count())

# COMMAND ----------

#-------------------------Validation-----------------------
df_silver_customers_check = spark.read.table("retail_etl_project.retail_de.silver_customers")

df_silver_customers_check.agg(
    count("*").alias("total"),
    countDistinct("customer_id").alias("distinct_customer_id"),
    countDistinct("customer_unique_id").alias("distinct_unique_id"),
    countDistinct("customer_state").alias("distinct_states"),
).show()

df_silver_customers_check.select("customer_zip_code_prefix", "customer_city", "customer_state").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver products table**

# COMMAND ----------

df_bronze_products = (
    spark.read.format("delta")
        .table("retail_etl_project.retail_de.bronze_products")
)

print("bronze rows:", df_bronze_products.count())

# COMMAND ----------

#---------------------Standardizing and casting columns-------------
df_typed_products = df_bronze_products.select(
    when(trim("product_id") == "", None).otherwise(trim("product_id")).alias("product_id"),
    when(trim("product_category_name") == "", None).otherwise(lower(trim("product_category_name"))).alias("product_category_name"),
    col("product_name_lenght").try_cast("int").alias("product_name_length"),
    col("product_description_lenght").try_cast("int").alias("product_description_length"),
    col("product_photos_qty").try_cast("int").alias("product_photos_qty"),
    col("product_weight_g").try_cast("int").alias("product_weight_g"),
    col("product_length_cm").try_cast("int").alias("product_length_cm"),
    col("product_height_cm").try_cast("int").alias("product_height_cm"),
    col("product_width_cm").try_cast("int").alias("product_width_cm"),
    "_ingested_at", "_source", "_batch_id"
)

# COMMAND ----------

#--------------------Validate and Deduplicate----------------
df_valid_products = df_typed_products.filter(
    col("product_id").isNotNull()
    & (col("product_weight_g").isNull() | (col("product_weight_g") >= 0))
    & (col("product_length_cm").isNull() | (col("product_length_cm") >= 0))
    & (col("product_height_cm").isNull() | (col("product_height_cm") >= 0))
    & (col("product_width_cm").isNull() | (col("product_width_cm") >= 0))
)

window_products = Window.partitionBy("product_id").orderBy(col("_ingested_at").desc())

df_silver_products = (
    df_valid_products
    .withColumn("rn", row_number().over(window_products))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

(df_silver_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.silver_products"))

print("rows failing validation:", df_typed_products.count() - df_valid_products.count())
print("duplicates removed:     ", df_valid_products.count() - df_silver_products.count())
print("silver table rows:      ", spark.read.table("retail_etl_project.retail_de.silver_products").count())

# COMMAND ----------

#------------------------Validate-----------------------
df_silver_products_check = spark.read.table("retail_etl_project.retail_de.silver_products")

df_silver_products_check.printSchema()

df_silver_products_check.agg(
    count("*").alias("total"),
    countDistinct("product_id").alias("distinct_products"),
    sum(when(col("product_category_name").isNull(), 1).otherwise(0)).alias("null_category"),
    sum(when(col("product_weight_g").isNull(), 1).otherwise(0)).alias("null_weight"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver Sellers Table**

# COMMAND ----------

df_bronze_sellers = spark.read.format("delta").table("retail_etl_project.retail_de.bronze_sellers")

print("bronze rows:", df_bronze_sellers.count())

# COMMAND ----------

#---------------------Standardizing and casting columns------------
df_typed_sellers = df_bronze_sellers.select(
    when(trim("seller_id") == "", None).otherwise(trim("seller_id")).alias("seller_id"),
    trim("seller_zip_code_prefix").alias("seller_zip_code_prefix"),
    lower(trim("seller_city")).alias("seller_city"),
    upper(trim("seller_state")).alias("seller_state"),
    "_ingested_at", "_source", "_batch_id"
)

# COMMAND ----------

#------------------Validation and deduplication-------------
df_valid_sellers = df_typed_sellers.filter(col("seller_id").isNotNull())

window_sellers = Window.partitionBy("seller_id").orderBy(col("_ingested_at").desc())

df_silver_sellers = (
    df_valid_sellers.withColumn("rn", row_number().over(window_sellers)).filter(col("rn") == 1).drop("rn")
)

# COMMAND ----------

(df_silver_sellers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.silver_sellers"))

print("rows failing validation:", df_typed_sellers.count() - df_valid_sellers.count())
print("duplicates removed:     ", df_valid_sellers.count() - df_silver_sellers.count())
print("silver table rows:      ", spark.read.table("retail_etl_project.retail_de.silver_sellers").count())

# COMMAND ----------

#------------------------Validate-----------------------
df_silver_sellers_check = spark.read.table("retail_etl_project.retail_de.silver_sellers")

df_silver_sellers_check.agg(
    count("*").alias("total"),
    countDistinct("seller_id").alias("distinct_sellers"),
    countDistinct("seller_state").alias("distinct_states"),
).show()

(df_silver_sellers_check.groupBy("seller_city").count().orderBy(col("count").desc()).show(5, truncate=False))