# Data Dictionary

Catalog: `retail_etl_project` | Schema: `retail_de`

All Bronze, Silver and Gold tables are Delta tables. Raw files live in the Volume `raw_files` (`batch_1/`, `batch_2/`).

## Conventions

- **Metadata columns** on Bronze and Silver: `_ingested_at` (load timestamp), `_source` (source file path), `_batch_id` (which load the row came from).
- **Surrogate keys** in Gold are `xxhash64` of the business ID, so the same ID always gives the same key.
- **Money columns** are `double`. A production system would use `decimal`; see Limitations in the README.
- **Revenue** in analytics means `SUM(price)`, excluding freight and excluding orders with status `canceled` or `unavailable`.

---

## Bronze (raw, all columns are strings)

| Table | Grain | Notes |
|---|---|---|
| bronze_customers | one row per source row | raw copy of the customers file |
| bronze_orders | one row per source row | also holds injected test rows (`_batch_id = bad_test`) and batch_2 (`_batch_id = batch_2`) |
| bronze_order_items | one row per source row | also holds injected test rows (`_batch_id = bad_test`) |
| bronze_payments | one row per source row | raw copy |
| bronze_products | one row per source row | keeps the source's misspelled `product_name_lenght` and `product_description_lenght` |
| bronze_sellers | one row per source row | raw copy |

---

## Silver (typed, standardized, deduplicated, validated)

Duplicates are removed with `row_number()` over the business key, keeping the latest `_ingested_at`.

### silver_orders
Grain: one row per order. Key: `order_id`.

| Column | Type | Description |
|---|---|---|
| order_id | string | Order identifier |
| customer_id | string | Customer identifier. Generated per order in Olist, so it is not a person |
| order_status | string | Lowercased status (delivered, shipped, canceled, ...) |
| order_purchase_timestamp | timestamp | When the order was placed |
| order_approved_at | timestamp | Payment approval time, null if not approved |
| order_delivered_carrier_date | timestamp | Handed to carrier, null if not yet |
| order_delivered_customer_date | timestamp | Delivered to customer, null if not delivered |
| order_estimated_delivery_date | timestamp | Promised delivery date |

Nulls in the delivery dates are real information and are kept.

### silver_order_items
Grain: one row per order item. Key: (`order_id`, `order_item_id`).

| Column | Type | Description |
|---|---|---|
| order_id | string | Order identifier |
| order_item_id | int | Item number within the order (1, 2, 3, ...) |
| product_id | string | Product identifier |
| seller_id | string | Seller identifier |
| shipping_limit_date | timestamp | Seller's shipping deadline |
| price | double | Item price, never negative |
| freight_value | double | Freight charged for the item, never negative |

### silver_payments
Grain: one row per payment. Key: (`order_id`, `payment_sequential`). One order can have several payments.

| Column | Type | Description |
|---|---|---|
| order_id | string | Order identifier |
| payment_sequential | int | Payment number within the order |
| payment_type | string | Lowercased type (credit_card, boleto, voucher, debit_card, not_defined) |
| payment_installments | int | Number of installments |
| payment_value | double | Payment amount, never negative (zero is allowed) |

### silver_customers
Grain: one row per `customer_id`. Key: `customer_id`.

| Column | Type | Description |
|---|---|---|
| customer_id | string | Per-order customer identifier |
| customer_unique_id | string | The real person; used to build `dim_customer` |
| customer_zip_code_prefix | string | Kept as string to preserve leading zeros |
| customer_city | string | Lowercased |
| customer_state | string | Uppercased |

### silver_products
Grain: one row per product. Key: `product_id`.

| Column | Type | Description |
|---|---|---|
| product_id | string | Product identifier |
| product_category_name | string | Lowercased category, null when the source has none |
| product_name_length | int | Corrected spelling of the source column |
| product_description_length | int | Corrected spelling of the source column |
| product_photos_qty | int | Number of photos |
| product_weight_g | int | Weight in grams |
| product_length_cm, product_height_cm, product_width_cm | int | Dimensions in cm |

### silver_sellers
Grain: one row per seller. Key: `seller_id`.

| Column | Type | Description |
|---|---|---|
| seller_id | string | Seller identifier |
| seller_zip_code_prefix | string | Kept as string |
| seller_city | string | Lowercased; some source values are messy (for example city plus state in one field) |
| seller_state | string | Uppercased |

---

## Data quality tables

### quarantine_orders
Rows rejected from `silver_orders`. Same columns as the typed order data, plus:

| Column | Description |
|---|---|
| purchase_raw | The raw purchase date string, kept to show what failed to parse |
| reject_reason | `null_order_id`, `null_customer_id` or `invalid_purchase_date` |

### quarantine_order_items
Rows rejected from `silver_order_items`. Plus:

| Column | Description |
|---|---|
| price_raw | The raw price string |
| reject_reason | `null_order_id`, `null_product_id`, `null_seller_id`, `invalid_price`, `null_price`, `negative_price` or `negative_freight` |

### dq_summary
Count of rejected rows per table and reason: `table_name`, `reject_reason`, `rejected_rows`.

Only orders and order items are quarantined. The other four Silver tables use plain filters.

---

## Gold (star schema)

### fact_order_items
Grain: **one row per order item**. Key: (`order_id`, `order_item_id`). Row count equals `silver_order_items`.

| Column | Type | Description |
|---|---|---|
| order_id | string | Order identifier |
| order_item_id | int | Item number within the order |
| customer_key | bigint | FK to `dim_customer` |
| product_key | bigint | FK to `dim_product` |
| seller_key | bigint | FK to `dim_seller` |
| order_date_key | int | FK to `dim_date`, purchase date as `yyyyMMdd` |
| price | double | Item price |
| freight_value | double | Item freight |
| total_item_value | double | price + freight |
| order_status | string | Status of the parent order |
| order_delivered_customer_date | timestamp | Delivery time, null if not delivered |
| order_estimated_delivery_date | timestamp | Promised delivery date |
| is_late | boolean | True if delivery date is after the estimated date (compared as dates); null if not delivered |

Payments are deliberately not in this table. They belong to the order, and repeating them on item rows would inflate totals.

### dim_customer
Grain: **one row per real person** (`customer_unique_id`).

| Column | Type | Description |
|---|---|---|
| customer_key | bigint | Surrogate key |
| customer_unique_id | string | The person |
| customer_city, customer_state | string | Taken from the person's most recent order |
| first_order_date | date | Earliest purchase date |
| total_orders | long | Distinct orders, all statuses including canceled |
| total_spend | double | Sum of payments, aggregated per order before joining |
| repeat_customer_flag | boolean | True when `total_orders > 1` |

### dim_product
Grain: one row per product.

| Column | Type | Description |
|---|---|---|
| product_key | bigint | Surrogate key |
| product_id | string | Business key |
| product_category | string | Category, with nulls replaced by `unknown` |
| product_name_length, product_description_length, product_photos_qty | int | Basic attributes |
| product_weight_g, product_length_cm, product_height_cm, product_width_cm | int | Weight and dimensions |

### dim_seller
Grain: one row per seller. Columns: `seller_key`, `seller_id`, `seller_city`, `seller_state`.

### dim_date
Grain: one row per calendar day from 2016-09-04 to 2018-10-17 (774 days).

| Column | Description |
|---|---|
| date_key | `yyyyMMdd` integer |
| full_date | Date |
| year, quarter, month, day | Calendar parts |
| month_name, day_name | Names |
| is_weekend | True for Saturday and Sunday |
