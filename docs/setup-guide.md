# ShopStream Setup Guide

This guide walks through the exact steps to reproduce the ShopStream project in a Databricks workspace.

## Prerequisites

* A Databricks workspace with Unity Catalog enabled
* Serverless compute enabled (for the Lakeflow pipeline)
* Permission to create catalogs, schemas, and volumes

## Step 1: Create Unity Catalog Infrastructure

Run the following SQL in a Databricks SQL cell or SQL editor:

```sql
CREATE CATALOG IF NOT EXISTS shopstream;
CREATE SCHEMA IF NOT EXISTS shopstream.core;
CREATE VOLUME IF NOT EXISTS shopstream.core.raw;
CREATE VOLUME IF NOT EXISTS shopstream.core.events;
```

This creates:
* A managed catalog `shopstream`
* A schema `core` inside `shopstream`
* A storage volume `raw` for CSV seed data
* A storage volume `events` for streaming JSON events

## Step 2: Upload Seed Data

Upload the three CSV files to the `raw` volume:

* `orders_2026_h1.csv` → `/Volumes/shopstream/core/raw/orders_2026_h1.csv`
* `customers.csv` → `/Volumes/shopstream/core/raw/customers.csv`
* `products.csv` → `/Volumes/shopstream/core/raw/products.csv`

You can upload via:
* The Databricks workspace UI (Catalog Explorer → Volumes → Upload)
* The Databricks CLI: `databricks fs cp ./data/raw/orders_2026_h1.csv dbfs:/Volumes/shopstream/core/raw/`
* A notebook cell: `dbutils.fs.cp("file:/tmp/orders_2026_h1.csv", "/Volumes/shopstream/core/raw/orders_2026_h1.csv")`

See [data/README.md](../data/README.md) for details about each file.

## Step 3: Import Notebooks

Import the five notebooks from the `notebooks/` directory into your workspace:

1. `01_bronze_batch_ingestion.py`
2. `02_silver_layer.py`
3. `03_gold_layer.py`
4. `04_stream_events.py`
5. `05_streaming_bronze.py`

You can import via:
* The Databricks workspace UI (Create → Notebook → Import)
* The Databricks CLI: `databricks workspace import notebooks/01_bronze_batch_ingestion.py /Workspace/Users/<your-email>/01_bronze_batch_ingestion --format SOURCE --language python`

> **Note:** The notebook paths in the job JSON reference `/Workspace/Users/suhailahmed030803@gmail.com/01_bronze_batch_ingestion`. Update these paths to match your workspace path.

## Step 4: Run the Batch Pipeline

Run the notebooks in order:

1. **`01_bronze_batch_ingestion`** — Loads CSV files into Bronze tables using `COPY INTO`
2. **`02_silver_layer`** — Cleans, deduplicates, and transforms Bronze data into Silver tables
3. **`03_gold_layer`** — Creates Gold aggregation tables from Silver

Each notebook can be run by opening it and clicking **Run All**.

### Expected Results

| Step | Result |
| --- | --- |
| Bronze orders | 13,717 rows |
| Bronze customers | 1,000 rows |
| Bronze products | 197 rows |
| Silver orders (after dedup + clean) | 13,228 rows |
| Silver orders (after DELETE cancelled) | 11,061 rows |
| Gold daily revenue | One row per day with completed orders |
| Gold category performance | 8 categories with revenue and margin |
| Gold customer LTV | One row per customer with lifetime metrics |

## Step 5: Run the Streaming Pipeline (Optional)

### 5a. Generate Live Events

Run notebook `04_stream_events` to generate JSON event files:
* 60 files, 25 events each, 5-second intervals
* Files are written to `/Volumes/shopstream/core/events/orders_stream/`
* This notebook takes ~5 minutes to complete

### 5b. Ingest Events via Auto Loader

Run notebook `05_streaming_bronze` to ingest the events:
* Auto Loader picks up new JSON files from the events volume
* Writes to `shopstream.core.bronze_orders_stream`
* Uses `trigger(availableNow=True)` for batch-style processing

## Step 6: Configure the Lakeflow Job

### Option A: Via JSON Import

Use the job configuration in `jobs/shopstream_daily_batch.json`:

```bash
databricks jobs create --json-file jobs/shopstream_daily_batch.json
```

> **Important:** Update the `notebook_path` values in the JSON to match your workspace path before importing.

### Option B: Via UI

1. Go to **Jobs** in the Databricks sidebar
2. Click **Create Job**
3. Name it `shopstream_daily_batch`
4. Add three tasks in order:
   * Task 1: `bronze_ingestion` → notebook `01_bronze_batch_ingestion`
   * Task 2: `silver_layer` → notebook `02_silver_layer` (depends on `bronze_ingestion`)
   * Task 3: `gold_layer` → notebook `03_gold_layer` (depends on `silver_layer`)
5. Set the schedule to daily (or leave paused)
6. Set run-as to owner
7. Set performance target to PERFORMANCE_OPTIMIZED

## Step 7: Configure the Lakeflow Pipeline

### Option A: Via JSON Import

Use the pipeline configuration in `pipeline-config/shopstream_lakeflow.json`:

```bash
databricks pipelines create --json-file pipeline-config/shopstream_lakeflow.json
```

> **Important:** Update the `libraries[0].include` glob path to match your workspace path. The transformation files must be at a path like `/Workspace/Users/<your-email>/shopstream_lakeflow/transformations/`.

### Option B: Via UI

1. Upload `my_transformation.py` and `daily_revenue.py` to your workspace
2. Go to **Pipelines** in the Databricks sidebar
3. Click **Create Pipeline**
4. Name it `shopstream_lakeflow`
5. Set the pipeline type to Workspace
6. Set the catalog to `shopstream` and schema to `core`
7. Enable Photon and Serverless
8. Add the transformation files as libraries (glob pattern: `**/transformations/**/*.py`)
9. Click **Start**

## Step 8: Configure the Dashboard

The dashboard configuration is in `dashboard/ShopStream_Sales_dashboard.json`.

1. Go to **Dashboards** in the Databricks sidebar
2. Click **Create Dashboard**
3. Import the JSON file or recreate manually:
   * Add two datasets: `shopstream.core.gold_category_performance` and `shopstream.core.gold_daily_revenue`
   * Add a page with GRID_V1 layout
4. **Note:** The current dashboard has datasets registered but no visualizations placed. Add charts/widgets to complete it.

## Verification

After completing all steps, verify the setup:

```sql
-- Check all 12 tables exist
SHOW TABLES IN shopstream.core;

-- Verify Bronze row counts
SELECT 'bronze_orders' AS t, COUNT(*) AS rows FROM shopstream.core.bronze_orders
UNION ALL
SELECT 'bronze_customers', COUNT(*) FROM shopstream.core.bronze_customers
UNION ALL
SELECT 'bronze_products', COUNT(*) FROM shopstream.core.bronze_products;

-- Verify Silver row count
SELECT COUNT(*) AS silver_rows FROM shopstream.core.silver_orders;

-- Verify Gold tables
SELECT * FROM shopstream.core.gold_daily_revenue ORDER BY order_date LIMIT 7;
SELECT * FROM shopstream.core.gold_category_performance ORDER BY revenue DESC;
SELECT * FROM shopstream.core.gold_customer_ltv ORDER BY lifetime_revenue DESC LIMIT 10;
```

## Troubleshooting

| Issue | Solution |
| --- | --- |
| `COPY INTO` loads 0 rows | The CSV file may already be loaded (idempotent). Check `SELECT COUNT(*) FROM bronze_orders`. |
| Auto Loader finds no events | Run `04_stream_events` first to generate event files. |
| Pipeline fails to find transformations | Verify the glob path in the pipeline library matches the workspace path where `my_transformation.py` and `daily_revenue.py` are stored. |
| Job notebook paths not found | Update `notebook_path` in the job JSON to match your workspace path. |
