# Architecture

## Flow

```
Olist CSVs (Unity Catalog Volume: raw_files/batch_1, batch_2)
        |  spark.read.csv, every column as string
        v
BRONZE   bronze_*      raw copy + _ingested_at, _source, _batch_id
        |  trim, type-cast, standardize, validate, deduplicate
        +--------> quarantine_orders, quarantine_order_items (rejected rows + reason)
        v
SILVER   silver_*      typed, deduplicated, valid records
        |  join, aggregate, model
        v
GOLD     fact_order_items, dim_customer, dim_product, dim_seller, dim_date
        |
        v
Databricks SQL analytics (10 queries) and an AI/BI dashboard
```

Everything lives in catalog `retail_etl_project`, schema `retail_de`. Compute is Databricks Free Edition serverless. Notebooks are run manually in order, 01 to 06.

## Layers

| Layer | Purpose | Key rules |
|---|---|---|
| Bronze | Faithful copy of what the source sent | All columns are strings, so bad values are not silently turned into nulls. Metadata columns added. |
| Silver | Trustworthy, typed data | Safe casts (`try_cast`, `try_to_timestamp`), trimmed and standardized text, validation, deterministic dedup with `row_number()` keeping the latest `_ingested_at`. |
| Gold | Business-ready star schema | One fact table and four dimensions with deterministic surrogate keys (`xxhash64`). |

## Grain of each table

| Table | One row per |
|---|---|
| silver_orders | order (`order_id`) |
| silver_order_items | order item (`order_id`, `order_item_id`) |
| silver_payments | payment (`order_id`, `payment_sequential`) |
| silver_customers | `customer_id` (generated per order in Olist) |
| fact_order_items | order item |
| dim_customer | real person (`customer_unique_id`) |
| dim_product, dim_seller | product, seller |
| dim_date | calendar day |

## Key design decisions

1. **Bronze stays as strings.** Casting at ingestion could hide bad values. Typing and validation happen in Silver.
2. **`dim_customer` is at `customer_unique_id` grain.** `customer_id` is new for every order, so grouping by it would show every customer with one order and 0 repeat customers. Grouping by person gives 96,096 customers, 2,999 of them repeat (3.12%).
3. **Payments are not in the fact table.** They belong to the order. Joining items to payments directly multiplies rows: for one test order with 7 items and 3 payments the join produced 21 rows. Over the whole dataset the wrong join gave 20.3M against a true payments total of 16.0M. Aggregating payments to order grain first gives 15.85M, and the 162,591.95 difference is exactly the payments on orders that have no item rows.
4. **Deterministic surrogate keys.** `xxhash64` of the business ID gives the same key on every rebuild, so fact references stay valid.
5. **Silver keeps nulls, Gold applies defaults.** Null product categories stay null in Silver and become `unknown` in `dim_product` (610 products).
6. **Dedup keeps the latest `_ingested_at`.** This assumes later arrival means newer data. If the source had an update timestamp, that would be used instead.
7. **Quarantine instead of silent drops.** One `when` chain gives each rejected row one reason. Rejected rows are split off before deduplication so an invalid row can never be the surviving record.

## Data quality

Quarantine and reason codes are implemented for orders and order items only. The Olist data is already clean, so rows were injected to prove the rules work (a null `order_id`, an empty `customer_id`, an unparseable date, a negative price, a null `product_id`, an unparseable price, and a duplicate order). These are test rows tagged `_batch_id = bad_test`, not real Olist errors.

Reconciliation after the split: Bronze orders 99,445 = Silver 99,441 + quarantined 3 + duplicates removed 1. Order items: 112,653 = 112,650 + 3 + 0.

The reconciliation also caught a real design flaw during development: an injected duplicate order carried a `customer_id` that did not exist, replaced the real order in Silver, and was then silently dropped by an inner join in `dim_customer`. The spend total was 38.71 short of the payments total. The test data was corrected to reuse the original customer.

## Incremental processing and idempotency

- **Batch 2** contains 10 rows: 3 shipped-to-delivered updates, 2 processing-to-canceled updates, 2 new delivered orders, 1 new `created` order with null dates, 1 exact duplicate and 1 row with a null `order_id`.
- Bronze ingestion is **delete-then-append on `_batch_id`**, so re-running it does not double the batch.
- Batch 2 is cleaned and split with the same rules. 1 row is quarantined and 1 duplicate removed, leaving 8 rows. The duplicate is removed before the MERGE because two source rows matching one target row would make Delta raise an error.
- **MERGE into `silver_orders`** matches on `order_id`. Matched rows are updated only when `s._ingested_at > t._ingested_at`; unmatched rows are inserted. Result: 5 updated, 3 inserted, Silver 99,444 rows.
- **Idempotency test:** running the same MERGE again produced 0 updates and 0 inserts, with unchanged row count and an unchanged hash fingerprint of the affected rows.

## Gold freshness

Only Bronze ingestion and the Silver orders merge are incremental. Gold is rebuilt in full. After the MERGE, the fact table showed old statuses until it was rebuilt, then matched Silver.

## Analytics

Ten SQL queries: monthly revenue with month-over-month growth (`LAG`), top products, top sellers (`RANK`, share of total), repeat rate, average order value (order grain first), revenue by category (share and cumulative share), late-delivery rate by state, order status breakdown, payment type mix, and average delivery days by month. The first seven read Gold. The last three read Silver, because Gold has no order-grain table.

A Databricks AI/BI dashboard sits on these queries. Each chart is fed by one dataset (one query). Months with fewer than 100 orders are hidden on the monthly charts. The payment chart uses value rather than order counts, because an order paid with two types is counted under both.

- Total revenue (excluding canceled and unavailable orders): 13,493,660.94.
- Average order value 137.41, median 86.90, 98,197 orders. Average item value is 120.37.
- Peak month: November 2017 (1,003,862.14).

## Limitations

- Orchestration is manual: the notebooks are run in order. Databricks Jobs are available in Free Edition but were not used.
- The dashboard is not refreshed automatically.
- Money columns use `double`, which gives tiny floating-point differences. A production system would use `decimal`.
- Quarantine covers two of six tables; the others use plain filters.
- Dedup relies on ingestion time rather than a source version column.
- Batch 2 covers orders only. The new orders have no items or payments, so they do not appear in the fact table.
- Gold is rebuilt in full rather than incrementally.
- Seller city values are messy in the source and are only trimmed and lowercased.
- The test rows and batch 2 are synthetic.
