# Data Directory

This directory is a placeholder for the three source CSV files used by the ShopStream batch pipeline.

## Source Files

The raw CSV files are stored in the Databricks Unity Catalog volume `shopstream.core.raw` at runtime. They are **not included in this repository** because you should generate or upload your own data.

### orders_2026_h1.csv

* **Location:** `/Volumes/shopstream/core/raw/orders_2026_h1.csv`
* **Size:** ~1 MB (13,717 rows)
* **Columns:** `order_line_id`, `order_id`, `customer_id`, `product_id`, `quantity`, `unit_price`, `order_ts`, `status`, `coupon_code`
* **Description:** Order line data for the first half of 2026. Each row represents one line item in an order. Statuses include `completed`, `cancelled`, and `returned` (in both lowercase and uppercase). Contains intentional data quality issues: double-fired order lines (234 duplicates), impossible quantities (259 rows with `quantity <= 0`), and mixed-casing status values.
* **Used by:** `01_bronze_batch_ingestion` → `bronze_orders` table

### customers.csv

* **Location:** `/Volumes/shopstream/core/raw/customers.csv`
* **Size:** ~78 KB (1,000 rows)
* **Columns:** `customer_id`, `name`, `email`, `city`, `country`, `signup_date`, `signup_channel`
* **Description:** Customer dimension data with 1,000 customer profiles across multiple countries (US, CA, IN, AE, AU, DE, etc.) and signup channels (email, referral, organic, paid_search, social).
* **Used by:** `01_bronze_batch_ingestion` → `bronze_customers` table

### products.csv

* **Location:** `/Volumes/shopstream/core/raw/products.csv`
* **Size:** ~8.7 KB (197 rows)
* **Columns:** `product_id`, `product_name`, `category`, `unit_price`, `unit_cost`
* **Description:** Product dimension data with 197 products across 8 categories: home-kitchen, fashion, books, fitness, electronics, toys, grocery, and beauty. Includes both `unit_price` and `unit_cost` to enable margin analysis.
* **Used by:** `01_bronze_batch_ingestion` → `bronze_products` table

## How to Obtain the Data

### Option 1: Download from your existing Databricks workspace

If you have an existing workspace with the ShopStream project, download the files:

```bash
databricks fs cp dbfs:/Volumes/shopstream/core/raw/orders_2026_h1.csv ./data/raw/orders_2026_h1.csv
databricks fs cp dbfs:/Volumes/shopstream/core/raw/customers.csv ./data/raw/customers.csv
databricks fs cp dbfs:/Volumes/shopstream/core/raw/products.csv ./data/raw/products.csv
```

### Option 2: Generate your own data

Create CSV files with the same schema (columns listed above). The data should include:
* Order lines with mixed-casing statuses and some duplicates and bad quantities
* 1,000 customers with diverse countries and signup channels
* ~200 products across 8 categories with prices and costs

### Option 3: Upload to your Databricks workspace

After obtaining the files, upload them to the volume:

```bash
databricks fs cp ./data/raw/orders_2026_h1.csv dbfs:/Volumes/shopstream/core/raw/orders_2026_h1.csv
databricks fs cp ./data/raw/customers.csv dbfs:/Volumes/shopstream/core/raw/customers.csv
databricks fs cp ./data/raw/products.csv dbfs:/Volumes/shopstream/core/raw/products.csv
```

## Streaming Events

The streaming events are generated dynamically by the `04_stream_events` notebook and stored in `/Volumes/shopstream/core/events/orders_stream/` as JSON files. These are not source data files and do not need to be pre-loaded.
