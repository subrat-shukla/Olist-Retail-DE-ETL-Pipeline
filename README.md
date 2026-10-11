# Retail Data Engineering Pipeline

A Medallion-architecture (Bronze, Silver, Gold) pipeline built on Databricks with PySpark and Delta Lake, using the Olist Brazilian e-commerce dataset.

This is a learning project. It covers cleaning, deduplication, data-quality checks, incremental loading with Delta MERGE, star-schema modeling and SQL analytics. It is not a production platform.

## 1. Project overview

The pipeline loads six Olist CSV files into Delta tables, cleans them, models them into a star schema and answers a small set of business questions with SQL. A second batch of orders is then applied incrementally with `MERGE`, and re-running the same batch is shown to change nothing.

## 2. Business problem

An online retailer wants reliable answers to questions such as monthly revenue, best products and sellers, repeat-customer rate and late deliveries. The raw data is split across several files with different grains, so a naive join gives wrong totals. This project builds a clean, tested model for those questions.

## 3. Architecture

```
Olist CSVs (Unity Catalog Volume)
   -> Bronze Delta (raw strings + metadata)
   -> Silver Delta (typed, deduplicated, validated) + quarantine tables
   -> Gold Delta (star schema)
   -> Databricks SQL analytics
```

Everything lives in catalog `retail_etl_project`, schema `retail_de`. See `docs/architecture.md` for details.

## 4. Dataset

Olist Brazilian E-Commerce Public Dataset (Kaggle). Six files are used:

| File | Rows |
|---|---|
| customers | 99,441 |
| orders | 99,441 |
| order_items | 112,650 |
| payments | 103,886 |
| products | 32,951 |
| sellers | 3,095 |

The full dataset is not committed to this repo. Download it from Kaggle. Only small samples are in `data/sample_data_only/`.

The data is already fairly clean. To show that the validation works, some bad rows were injected (see section 7). These are synthetic and are not Olist errors.

## 5. Technology stack

- Databricks Free Edition, serverless compute
- Python, PySpark (DataFrame API)
- Spark SQL (analytics notebook)
- Delta Lake
- Unity Catalog Volume for raw files
- Databricks AI/BI dashboard for the final analytics

## 6. Bronze, Silver and Gold

**Bronze.** Raw copy of each file with every column kept as a string, plus `_ingested_at`, `_source` and `_batch_id`. Strings are kept so that a bad value is not silently turned into null.

**Silver.** Trim and standardize text, safely cast types (`try_cast`, `try_to_timestamp`), validate keys and amounts, and deduplicate with `row_number()` over the business key, keeping the latest `_ingested_at`. Nulls with real meaning (for example a missing delivery date) are kept.

**Gold.** A star schema built from Silver (section 8).

## 7. Data-quality approach

- Validation rules: null keys, unparseable dates and prices, negative price, negative freight, negative payment value.
- Orders and order items use a quarantine pattern. A single `when` chain gives each bad row one `reject_reason`, rejected rows go to `quarantine_orders` and `quarantine_order_items`, and only clean rows reach Silver. Rejected rows are split off before deduplication.
- `dq_summary` counts rejected rows per table and reason.
- **Reconciliation:** Bronze = Silver + quarantined + duplicates removed. Orders: 99,445 = 99,441 + 3 + 1. Order items: 112,653 = 112,650 + 3 + 0.
- Injected test rows are tagged `_batch_id = bad_test`. The other four Silver tables use plain filters rather than quarantine.

Reconciliation also caught a real problem during development. An injected duplicate order had a `customer_id` that did not exist, replaced the real order in Silver, and was silently dropped by an inner join, leaving the customer spend total 38.71 short of the payments total. The test data was corrected.

## 8. Star schema

| Table | Grain |
|---|---|
| fact_order_items | one row per order item (112,650 rows) |
| dim_customer | one row per real person, `customer_unique_id` (96,096 rows) |
| dim_product | one row per product (32,951 rows) |
| dim_seller | one row per seller (3,095 rows) |
| dim_date | one row per day, 2016-09-04 to 2018-10-17 (774 rows) |

Surrogate keys are `xxhash64` of the business ID, so rebuilding a dimension does not change keys. `dim_customer` is at person grain because `customer_id` is generated per order. It includes `first_order_date`, `total_orders`, `total_spend` and `repeat_customer_flag`.

**Grain and join multiplication.** Items and payments are both children of an order. Joining them directly repeats rows: one test order with 7 items and 3 payments produced 21 rows. Across the dataset the wrong join gave 20.3M against a true payments total of 16.0M. Payments are aggregated to one row per order before joining, which gives 15.85M. The remaining 162,591.95 is payments on orders that have no item rows. Payments are not stored in the fact table for this reason.

See `docs/data_dictionary.md` for every column.

## 9. Incremental processing

A second batch of 10 order rows is written to the Volume (3 shipped-to-delivered updates, 2 processing-to-canceled updates, 2 new delivered orders, 1 new `created` order with null dates, 1 exact duplicate, 1 row with a null `order_id`).

- Bronze ingestion is delete-then-append on `_batch_id`, so it can be re-run safely.
- Batch 2 is cleaned with the same rules: 1 row quarantined, 1 duplicate removed, 8 rows left.
- `MERGE` into `silver_orders` on `order_id`: matched rows are updated only when the source `_ingested_at` is newer, and unmatched rows are inserted. Result: 5 updated, 3 inserted, 99,444 rows.
- **Idempotency:** running the same MERGE again gave 0 updates and 0 inserts, with an unchanged row count and an unchanged hash fingerprint of the affected rows.

Only Bronze ingestion and the Silver orders merge are incremental. Gold is rebuilt in full.

## 10. SQL analytics

Ten queries in `06_sql_analytics`:

1. Monthly revenue with month-over-month growth (`LAG`)
2. Top 10 products by revenue
3. Top 10 sellers with `RANK` and share of total
4. Customer repeat rate
5. Average order value (items summed to order grain first)
6. Revenue by category with share and cumulative share
7. Late-delivery rate by customer state
8. Order status breakdown
9. Payment type mix
10. Average delivery days by month

Revenue means `SUM(price)`, excluding freight and excluding orders with status `canceled` or `unavailable`. Queries 8 to 10 read from Silver, because Gold has no order-grain table.

Selected results from this run:
- Total revenue 13,493,660.94. Peak month November 2017 (1,003,862.14).
- Repeat rate 3.12% (2,999 of 96,096 customers).
- 98,197 orders, average order value 137.41, median 86.90. Average item value is 120.37.
- Late rate is highest in several northeastern states (MA 17.91%, CE 13.59%, PI 13.58%).
- 97.02% of orders are delivered. Credit card is 78.34% of payment value.

### Dashboard

The dashboard "Olist Retail ETL Project - Analytics" is built in Databricks on the queries above. It has four counters (total revenue, orders, average order value, repeat rate) and charts for monthly revenue, orders by status, top categories, payment value by type, top sellers, late-delivery rate by state and average delivery days.

Two display choices are noted on the charts: months with fewer than 100 orders are hidden from the monthly charts (partial or near-empty months at the start and end of the data), and the payment chart uses value rather than order counts, because an order paid with two payment types is counted under both.

Screenshots are in `docs/images/` and a PDF export is in `docs/dashboard.pdf`.

## 11. Key design decisions

- Bronze stays as strings so bad values stay visible.
- Silver keeps nulls and Gold applies defaults (`unknown` category).
- Deduplication uses `row_number()` for a deterministic result, not `dropDuplicates`.
- `dim_customer` uses `customer_unique_id` because `customer_id` changes per order.
- Payments are aggregated before joining and are not in the fact table.
- Lateness compares dates, not timestamps, because the estimate is always midnight.
- Gold is a full rebuild because the data is small. It would become incremental if volume required it.

## 12. How to run

1. Create a Databricks Free Edition workspace.
2. Download the six Olist files from Kaggle.
3. Run:
   ```sql
   CREATE CATALOG IF NOT EXISTS retail_etl_project;
   CREATE SCHEMA  IF NOT EXISTS retail_etl_project.retail_de;
   CREATE VOLUME  IF NOT EXISTS retail_etl_project.retail_de.raw_files;
   ```
4. Create a folder `batch_1` in the Volume and upload the six files.
5. Run the notebooks in order:
   - `01_bronze_ingestion`
   - `02_silver_transformations`
   - `03_data_quality`
   - `04_gold_model`
   - `05_incremental_processing`
   - `06_sql_analytics`
6. Optional: `07_create_sample_data` regenerates the small CSV files in `data/sample_data_only/` (200 orders and their related rows). It is not part of the pipeline.

The catalog name may need changing if your workspace restricts catalog creation.

## 13. Limitations

- Orchestration is manual: the notebooks are run in order. Databricks Jobs are available in Free Edition but were not used in this project.
- Queries 8 to 10 and their dashboard charts read Silver, because there is no order-grain Gold table.
- The dashboard reads from the tables as of the last run and is not refreshed automatically.
- Money columns use `double`, which gives tiny floating-point differences. A production system would use `decimal`.
- Quarantine covers two of the six tables.
- Deduplication relies on ingestion time, not a source version column.
- Batch 2 covers orders only. The new orders have no items or payments and so do not appear in the fact table.
- Gold is rebuilt in full rather than incrementally.
- Seller city values are messy in the source and are only trimmed and lowercased.
- The test rows and batch 2 are synthetic, and the data volume is small (about 100K orders). No large-scale performance claims are made.
