-- Databricks notebook source
USE CATALOG retail_etl_project;
USE SCHEMA retail_de;

-- COMMAND ----------

--Query 1: monthly revenue, with month-over-month growth using the window function LAG.

WITH monthly AS (
  SELECT d.year, d.month,
         ROUND(SUM(f.price), 2)       AS revenue,
         COUNT(DISTINCT f.order_id)   AS orders
  FROM fact_order_items f
  JOIN dim_date d ON f.order_date_key = d.date_key
  WHERE f.order_status NOT IN ('canceled', 'unavailable')
  GROUP BY d.year, d.month
)
SELECT year, month, revenue, orders,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY year, month))
                 / LAG(revenue) OVER (ORDER BY year, month), 1) AS mom_growth_pct
FROM monthly
ORDER BY year, month;

-- COMMAND ----------

--Query 2: top 10 products by revenue.

SELECT p.product_id, p.product_category,
       ROUND(SUM(f.price), 2) AS revenue,
       COUNT(*)               AS items_sold
FROM fact_order_items f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY p.product_id, p.product_category
ORDER BY revenue DESC
LIMIT 10;

-- COMMAND ----------

--Query 3: top 10 sellers, ranked with RANK(), plus each seller’s share of total revenue using SUM() OVER ().

WITH seller_rev AS (
  SELECT s.seller_id, s.seller_state,
         SUM(f.price)             AS revenue,
         COUNT(DISTINCT f.order_id) AS orders
  FROM fact_order_items f
  JOIN dim_seller s ON f.seller_key = s.seller_key
  WHERE f.order_status NOT IN ('canceled', 'unavailable')
  GROUP BY s.seller_id, s.seller_state
)
SELECT RANK() OVER (ORDER BY revenue DESC)        AS revenue_rank,
       seller_id, seller_state,
       ROUND(revenue, 2)                          AS revenue,
       orders,
       ROUND(100 * revenue / SUM(revenue) OVER (), 2) AS pct_of_total
FROM seller_rev
ORDER BY revenue_rank
LIMIT 10;

-- COMMAND ----------

--Query 4: customer repeat rate, from dim_customer.

SELECT COUNT(*)                                             AS customers,
       SUM(CASE WHEN repeat_customer_flag THEN 1 ELSE 0 END) AS repeat_customers,
       ROUND(100 * SUM(CASE WHEN repeat_customer_flag THEN 1 ELSE 0 END)
                 / COUNT(*), 2)                             AS repeat_rate_pct
FROM dim_customer;

-- COMMAND ----------

--Query 5: average order value, with items summed to one row per order first.

WITH order_totals AS (
  SELECT order_id, SUM(price) AS order_value
  FROM fact_order_items
  WHERE order_status NOT IN ('canceled', 'unavailable')
  GROUP BY order_id
)
SELECT COUNT(*)                              AS orders,
       ROUND(AVG(order_value), 2)            AS avg_order_value,
       ROUND(percentile_approx(order_value, 0.5), 2) AS median_order_value
FROM order_totals;

-- COMMAND ----------

SELECT ROUND(AVG(price), 2) AS avg_item_value
FROM fact_order_items
WHERE order_status NOT IN ('canceled', 'unavailable');

-- COMMAND ----------

--Query 6: revenue by product category, with each category’s share of total and a running cumulative share (two window functions).

WITH cat_rev AS (
  SELECT p.product_category, SUM(f.price) AS revenue
  FROM fact_order_items f
  JOIN dim_product p ON f.product_key = p.product_key
  WHERE f.order_status NOT IN ('canceled', 'unavailable')
  GROUP BY p.product_category
)
SELECT product_category,
       ROUND(revenue, 2) AS revenue,
       ROUND(100 * revenue / SUM(revenue) OVER (), 2) AS pct_of_total,
       ROUND(100 * SUM(revenue) OVER (ORDER BY revenue DESC) / SUM(revenue) OVER (), 2) AS cumulative_pct
FROM cat_rev
ORDER BY revenue DESC
LIMIT 10;

-- COMMAND ----------

--Query 7: late delivery rate by customer state, using only delivered items and states with enough volume.

SELECT c.customer_state,
       COUNT(*)                                          AS delivered_items,
       SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END)        AS late_items,
       ROUND(100 * SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END) / COUNT(*), 2) AS late_rate_pct
FROM fact_order_items f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE f.order_status = 'delivered' AND f.is_late IS NOT NULL
GROUP BY c.customer_state
HAVING COUNT(*) >= 500
ORDER BY late_rate_pct DESC;

-- COMMAND ----------

WITH monthly AS (
  SELECT d.year, d.month, ROUND(SUM(f.price), 2) AS revenue
  FROM fact_order_items f
  JOIN dim_date d ON f.order_date_key = d.date_key
  WHERE f.order_status NOT IN ('canceled', 'unavailable')
  GROUP BY d.year, d.month
)
SELECT * FROM monthly ORDER BY revenue DESC LIMIT 3;

-- COMMAND ----------

SELECT ROUND(SUM(price), 2) AS total_revenue
FROM fact_order_items
WHERE order_status NOT IN ('canceled', 'unavailable');

-- COMMAND ----------

--Query 8: order status breakdown: This counts orders by status at order grain, using silver_orders because the fact table has one row per item, and shows each status’s share of all orders.
SELECT order_status,
       COUNT(*) AS orders,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_orders
FROM silver_orders
GROUP BY order_status
ORDER BY orders DESC;

-- COMMAND ----------

--Query 9: payment type mix: This totals payment value by payment type from silver_payments, which is at payment grain. No join is involved, so there is no multiplication.
SELECT payment_type,
       COUNT(DISTINCT order_id)  AS orders,
       COUNT(*)                  AS payment_rows,
       ROUND(SUM(payment_value), 2) AS total_value,
       ROUND(100 * SUM(payment_value) / SUM(SUM(payment_value)) OVER (), 2) AS pct_of_value
FROM silver_payments
GROUP BY payment_type
ORDER BY total_value DESC;

-- COMMAND ----------

--Query 10: average delivery days by month : This takes delivered orders only, from silver_orders at order grain, and averages the days from purchase to delivery for each purchase month.
SELECT YEAR(order_purchase_timestamp)  AS year,
       MONTH(order_purchase_timestamp) AS month,
       COUNT(*)                        AS delivered_orders,
       ROUND(AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)), 1) AS avg_delivery_days
FROM silver_orders
WHERE order_status = 'delivered'
  AND order_delivered_customer_date IS NOT NULL
GROUP BY YEAR(order_purchase_timestamp), MONTH(order_purchase_timestamp)
ORDER BY year, month;

-- COMMAND ----------

--total-revenue counter
SELECT ROUND(SUM(price), 2) AS total_revenue
FROM retail_etl_project.retail_de.fact_order_items
WHERE order_status NOT IN ('canceled', 'unavailable');