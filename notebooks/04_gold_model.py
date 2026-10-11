# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_items    = spark.read.table("retail_etl_project.retail_de.silver_order_items")
df_payments = spark.read.table("retail_etl_project.retail_de.silver_payments")

# COMMAND ----------

#For wxample : an order that has more than one item and more than one payment row.

df_item_counts = df_items.groupBy("order_id").agg(F.count("*").alias("item_rows"))
df_pay_counts  = df_payments.groupBy("order_id").agg(F.count("*").alias("payment_rows"))

df_example = (
    df_item_counts.join(df_pay_counts, "order_id")
    .filter((F.col("item_rows") > 1) & (F.col("payment_rows") > 1))
    .orderBy(F.col("item_rows").desc())
)

df_example.show(5, truncate=False)
example_order_id = df_example.first()["order_id"]

# COMMAND ----------

#show the raw item rows and payment rows for first order

df_items.filter(F.col("order_id") == example_order_id) \
    .select("order_id", "order_item_id", "price", "freight_value").show()

df_payments.filter(F.col("order_id") == example_order_id) \
    .select("order_id", "payment_sequential", "payment_type", "payment_value").show()

# COMMAND ----------

#--join items directly to payments (the wrong way) and see how many rows and what totals come out.
df_wrong = (
    df_items.filter(F.col("order_id") == example_order_id)
    .join(df_payments.filter(F.col("order_id") == example_order_id), "order_id")
)

print("rows after wrong join:", df_wrong.count())

df_wrong.agg(
    F.sum("price").alias("price_total"),
    F.sum("payment_value").alias("payment_total")
).show()

# COMMAND ----------

# aggregating payments to one row per order first, then join (the correct way).
df_pay_per_order = (
    df_payments
    .groupBy("order_id")
    .agg(F.sum("payment_value").alias("order_payment_total"),
         F.count("*").alias("payment_rows"))
)

df_right = (
    df_items.filter(F.col("order_id") == example_order_id)
    .join(df_pay_per_order, "order_id")
)

print("rows after correct join:", df_right.count())
df_right.select("order_id", "order_item_id", "price", "order_payment_total").show()

# COMMAND ----------

# comparing the whole-dataset payment total from the source, from the wrong join, and from the correct join.
true_total = df_payments.agg(F.sum("payment_value")).first()[0]

wrong_total = (
    df_items.join(df_payments, "order_id")
    .agg(F.sum("payment_value")).first()[0]
)

df_per_order_once = (
    df_items.select("order_id").distinct()
    .join(df_pay_per_order, "order_id")
)
right_total = df_per_order_once.agg(F.sum("order_payment_total")).first()[0]

print("true payments total:  ", true_total)
print("wrong join total:     ", wrong_total)
print("correct join total:   ", right_total)

# COMMAND ----------

#sum the payments belonging to orders that have no item rows, and compare with the gap.
df_orders_without_items = (
    df_pay_per_order
    .join(df_items.select("order_id").distinct(), "order_id", "left_anti")
)

orphan_total = df_orders_without_items.agg(F.sum("order_payment_total")).first()[0]
print("payments on orders with no items:", orphan_total)
print("true - correct join:             ", true_total - right_total)

# COMMAND ----------

# MAGIC %md
# MAGIC **Designing Dimension tables**

# COMMAND ----------

df_silver_sellers = spark.read.table("retail_etl_project.retail_de.silver_sellers")

#1. Dimension Seller Table
df_dim_seller = df_silver_sellers.select(
    F.xxhash64("seller_id").alias("seller_key"),
    "seller_id",
    "seller_city",
    "seller_state"
)

#I’m using xxhash64 of the business ID instead of row_number(). A hash gives the same key every run, so rebuilding a dimension never changes keys that the fact table already uses. row_number() can hand out different numbers on a rebuild and silently break joins.

# COMMAND ----------

df_silver_products = spark.read.table("retail_etl_project.retail_de.silver_products")

#2. Dimension Product Table
df_dim_product = df_silver_products.select(
    F.xxhash64("product_id").alias("product_key"),
    "product_id",
    F.coalesce("product_category_name", F.lit("unknown")).alias("product_category"),
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
)

# COMMAND ----------

#writing both dimension tables to delta

(df_dim_seller.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.dim_seller"))

(df_dim_product.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.dim_product"))

# COMMAND ----------

#checking row counts, key uniqueness, and the number of unknown categories.

df_dim_seller_check  = spark.read.table("retail_etl_project.retail_de.dim_seller")
df_dim_product_check = spark.read.table("retail_etl_project.retail_de.dim_product")

df_dim_seller_check.agg(
    F.count("*").alias("rows"),
    F.countDistinct("seller_key").alias("distinct_keys")
).show()

df_dim_product_check.agg(
    F.count("*").alias("rows"),
    F.countDistinct("product_key").alias("distinct_keys"),
    F.sum(F.when(F.col("product_category") == "unknown", 1).otherwise(0)).alias("unknown_category")
).show()

# COMMAND ----------

df_customers = spark.read.table("retail_etl_project.retail_de.silver_customers")
df_orders    = spark.read.table("retail_etl_project.retail_de.silver_orders")

df_pay_per_order = (
    spark.read.table("retail_etl_project.retail_de.silver_payments")
    .groupBy("order_id")
    .agg(F.sum("payment_value").alias("order_payment_total"))
)

# COMMAND ----------

#joining orders to customers (to get customer_unique_id) and to the per-order payment totals.
df_cust_orders = (
    df_orders.select("order_id", "customer_id", "order_purchase_timestamp")
    .join(df_customers.select("customer_id", "customer_unique_id", "customer_city", "customer_state"),
          "customer_id")
    .join(df_pay_per_order, "order_id", "left")
)

# COMMAND ----------

#calculating the metrics per person.
df_cust_metrics = (
    df_cust_orders
    .groupBy("customer_unique_id")
    .agg(
        F.min(F.to_date("order_purchase_timestamp")).alias("first_order_date"),
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.sum(F.coalesce("order_payment_total", F.lit(0.0))), 2).alias("total_spend")
    )
    .withColumn("repeat_customer_flag", F.when(F.col("total_orders") > 1, True).otherwise(False))
)

# COMMAND ----------

#picking each person’s city and state from their latest order, using a window

window_customers = (
    Window.partitionBy("customer_unique_id")
          .orderBy(F.col("order_purchase_timestamp").desc(), F.col("order_id").desc())
)

df_cust_latest = (
    df_cust_orders
    .withColumn("rn", F.row_number().over(window_customers))
    .filter(F.col("rn") == 1)
    .select("customer_unique_id", "customer_city", "customer_state")
)

# COMMAND ----------

#3. Dimension Customer table
df_dim_customer = (
    df_cust_metrics.join(df_cust_latest, "customer_unique_id")
    .select(
        F.xxhash64("customer_unique_id").alias("customer_key"),
        "customer_unique_id", "customer_city", "customer_state",
        "first_order_date", "total_orders", "total_spend", "repeat_customer_flag"
    )
)

(df_dim_customer.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.dim_customer"))

# COMMAND ----------

#validating row count, key uniqueness, repeat customers and total spend.

df_dim_customer_check = spark.read.table("retail_etl_project.retail_de.dim_customer")

df_dim_customer_check.agg(
    F.count("*").alias("rows"),
    F.countDistinct("customer_key").alias("distinct_keys"),
    F.sum(F.when(F.col("repeat_customer_flag"), 1).otherwise(0)).alias("repeat_customers"),
    F.round(F.sum("total_spend"), 2).alias("sum_total_spend")
).show()

# COMMAND ----------

#4. Dimension Date
date_range = df_orders.agg(
    F.min(F.to_date("order_purchase_timestamp")).alias("min_d"),
    F.max(F.to_date("order_purchase_timestamp")).alias("max_d")
).first()

print(date_range["min_d"], date_range["max_d"])

# COMMAND ----------

#4. Dimension Date table

df_dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{date_range['min_d']}'), to_date('{date_range['max_d']}'), interval 1 day)) AS full_date")
    .select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend")
    )
)

(df_dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.dim_date"))

df_dim_date.show(3)
print("days:", df_dim_date.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Desigining Fact Table**

# COMMAND ----------

# MAGIC %md
# MAGIC Building the central fact table, with one row per order item, holding foreign keys and measures.
# MAGIC Note: Everything in the analytics notebook queries this table and joins out to the dimensions.

# COMMAND ----------

#---------------------Fact order items table--------------------
df_items = spark.read.table("retail_etl_project.retail_de.silver_order_items")
df_orders = spark.read.table("retail_etl_project.retail_de.silver_orders")
df_customers = spark.read.table("retail_etl_project.retail_de.silver_customers")

# COMMAND ----------

#join items to orders and customers, keeping only the columns the fact needs.
df_fact_base = (
    df_items.select("order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value")
    .join(df_orders.select("order_id", "customer_id", "order_status", "order_purchase_timestamp",
                           "order_delivered_customer_date", "order_estimated_delivery_date"),
          "order_id")
    .join(df_customers.select("customer_id", "customer_unique_id"), "customer_id")
)

# COMMAND ----------

#building the final columns: surrogate keys, date key, total item value and the late flag.

df_fact_order_items = df_fact_base.select(
    "order_id",
    "order_item_id",
    F.xxhash64("customer_unique_id").alias("customer_key"),
    F.xxhash64("product_id").alias("product_key"),
    F.xxhash64("seller_id").alias("seller_key"),
    F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("order_date_key"),
    "price",
    "freight_value",
    F.round(F.col("price") + F.col("freight_value"), 2).alias("total_item_value"),
    "order_status",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    F.when(F.col("order_delivered_customer_date").isNull(), None)
     .otherwise(F.to_date("order_delivered_customer_date") > F.to_date("order_estimated_delivery_date"))
     .alias("is_late")
)

# COMMAND ----------

(df_fact_order_items.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail_etl_project.retail_de.fact_order_items"))

# COMMAND ----------

#checking the grain (row count and unique keys) and that price totals match Silver.
df_fact_check = spark.read.table("retail_etl_project.retail_de.fact_order_items")

df_fact_check.agg(
    F.count("*").alias("rows"),
    F.countDistinct("order_id", "order_item_id").alias("distinct_keys"),
    F.round(F.sum("price"), 2).alias("fact_price"),
    F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_rows")
).show()

print("silver items rows: ", df_items.count())
print("silver price total:", round(df_items.agg(F.sum("price")).first()[0], 2))

# COMMAND ----------

#check for orphan keys by testing that every fact key exists in its dimension.

df_dim_customer = spark.read.table("retail_etl_project.retail_de.dim_customer")
df_dim_product  = spark.read.table("retail_etl_project.retail_de.dim_product")
df_dim_seller = spark.read.table("retail_etl_project.retail_de.dim_seller")
df_dim_date = spark.read.table("retail_etl_project.retail_de.dim_date")

print("orphan customers:", df_fact_check.join(df_dim_customer, "customer_key", "left_anti").count())
print("orphan products: ", df_fact_check.join(df_dim_product,  "product_key",  "left_anti").count())
print("orphan sellers:  ", df_fact_check.join(df_dim_seller,   "seller_key",   "left_anti").count())
print("orphan dates:    ", df_fact_check.join(df_dim_date, df_fact_check.order_date_key == df_dim_date.date_key, "left_anti").count())