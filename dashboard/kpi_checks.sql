-- ShopStream Sales: KPI checks
-- Run in the Databricks SQL editor after importing the dashboard. With no filters
-- applied, each value below should match the matching dashboard widget.

-- 1. Section 1 KPI cards
SELECT
  d.total_revenue,
  d.total_orders,
  d.units_sold,
  ROUND(d.total_revenue / d.total_orders, 2)   AS avg_order_value,
  c.gross_margin,
  ROUND(c.gross_margin / c.revenue * 100, 1)   AS gross_margin_pct
FROM (
  SELECT SUM(revenue) AS total_revenue, SUM(orders) AS total_orders, SUM(units_sold) AS units_sold
  FROM shopstream.core.gold_daily_revenue
) d
CROSS JOIN (
  SELECT SUM(gross_margin) AS gross_margin, SUM(revenue) AS revenue
  FROM shopstream.core.gold_category_performance
) c;

-- 2. Section 4 customer KPI cards
SELECT
  COUNT(DISTINCT customer_id)        AS customers,
  ROUND(AVG(lifetime_revenue), 2)    AS avg_lifetime_revenue,
  ROUND(AVG(lifetime_orders), 1)     AS avg_lifetime_orders
FROM shopstream.core.gold_customer_ltv;

-- 3. Consistency across the three Gold tables. All three revenue totals come from
-- completed silver_orders, so they should agree (small differences mean orders whose
-- product_id or customer_id has no match in silver_products / silver_customers).
-- total_orders_by_day equals distinct_orders unless an order's lines span two dates.
SELECT
  (SELECT SUM(revenue)          FROM shopstream.core.gold_daily_revenue)        AS revenue_daily,
  (SELECT SUM(revenue)          FROM shopstream.core.gold_category_performance) AS revenue_category,
  (SELECT SUM(lifetime_revenue) FROM shopstream.core.gold_customer_ltv)         AS revenue_customers,
  (SELECT SUM(orders)           FROM shopstream.core.gold_daily_revenue)        AS total_orders_by_day,
  (SELECT COUNT(DISTINCT order_id) FROM shopstream.core.silver_orders
    WHERE status = 'completed')                                                 AS distinct_orders;
