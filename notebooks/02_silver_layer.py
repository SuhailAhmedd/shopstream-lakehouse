# Databricks notebook source
#
# ShopStream - Silver Layer
# Data quality remediation: dedup double-fired order lines, drop impossible quantities,
# normalize status casing, cast types, derive line_revenue, then DELETE cancelled orders.
#
# Source tables: shopstream.core.bronze_orders, bronze_customers, bronze_products
# Target tables: shopstream.core.silver_orders, silver_customers, silver_products

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Problem 1: the source system logs statuses in two casings
# MAGIC SELECT status, COUNT(*) AS row_count
# MAGIC FROM shopstream.core.bronze_orders
# MAGIC GROUP BY status
# MAGIC ORDER BY row_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Problem 2: double-fired order lines and impossible quantities
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM (
# MAGIC     SELECT order_line_id FROM shopstream.core.bronze_orders
# MAGIC     GROUP BY order_line_id HAVING COUNT(*) > 1
# MAGIC   )) AS duplicated_line_ids,
# MAGIC   (SELECT COUNT(*) FROM shopstream.core.bronze_orders WHERE quantity <= 0) AS bad_quantity_rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_orders: dedup double-fires, drop impossible rows, normalize, type, derive
# MAGIC CREATE OR REPLACE TABLE shopstream.core.silver_orders AS
# MAGIC WITH deduped AS (
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY order_line_id ORDER BY order_ts) AS rn
# MAGIC   FROM shopstream.core.bronze_orders
# MAGIC )
# MAGIC SELECT
# MAGIC   order_line_id,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   product_id,
# MAGIC   CAST(quantity AS INT) AS quantity,
# MAGIC   CAST(unit_price AS DOUBLE) AS unit_price,
# MAGIC   ROUND(quantity * unit_price, 2) AS line_revenue,
# MAGIC   CAST(order_ts AS TIMESTAMP) AS order_ts,
# MAGIC   LOWER(status) AS status,
# MAGIC   NULLIF(coupon_code, '') AS coupon_code
# MAGIC FROM deduped
# MAGIC WHERE rn = 1
# MAGIC   AND quantity > 0;
# MAGIC
# MAGIC SELECT COUNT(*) AS silver_rows FROM shopstream.core.silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dimensions need a lighter touch: types + a helpful derived column
# MAGIC CREATE OR REPLACE TABLE shopstream.core.silver_customers AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   email,
# MAGIC   city,
# MAGIC   country,
# MAGIC   CAST(signup_date AS DATE) AS signup_date,
# MAGIC   signup_channel
# MAGIC FROM shopstream.core.bronze_customers;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE shopstream.core.silver_products AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   CAST(unit_price AS DOUBLE) AS unit_price,
# MAGIC   CAST(unit_cost AS DOUBLE) AS unit_cost,
# MAGIC   ROUND(unit_price - unit_cost, 2) AS unit_margin
# MAGIC FROM shopstream.core.bronze_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta keeps a transaction log for every table. Look:
# MAGIC DESCRIBE HISTORY shopstream.core.silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Business rule: cancelled orders don't belong in silver.
# MAGIC -- Delta DML makes this a one-liner, and time travel keeps the audit trail.
# MAGIC DELETE FROM shopstream.core.silver_orders WHERE status = 'cancelled';
# MAGIC
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM shopstream.core.silver_orders) AS rows_now,
# MAGIC   (SELECT COUNT(*) FROM shopstream.core.silver_orders VERSION AS OF 0) AS rows_before_delete
