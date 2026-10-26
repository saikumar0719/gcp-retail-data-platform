-- Staging: clean raw events, one row per event.
-- Partition filter first: never scan the whole table.
CREATE OR REPLACE TABLE `{{project}}.retail_marts.stg_events` AS
WITH cleaned AS (
  SELECT
    event_id,
    event_type,
    event_ts,
    customer_id,
    product_id,
    amount,
    quantity,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts) AS rn
  FROM `{{project}}.retail_raw.events`
  WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
SELECT * EXCEPT (rn) FROM cleaned WHERE rn = 1;

-- Mart: daily product revenue with running totals and ranking.
-- CTEs keep each step readable; window functions do the analytics.
CREATE OR REPLACE TABLE `{{project}}.retail_marts.daily_product_revenue` AS
WITH daily AS (
  SELECT
    DATE(event_ts) AS sale_date,
    product_id,
    COUNT(*) AS order_count,
    SUM(quantity) AS units_sold,
    SUM(amount) AS revenue
  FROM `{{project}}.retail_marts.stg_events`
  WHERE event_type = 'order'
  GROUP BY sale_date, product_id
)
SELECT
  sale_date,
  product_id,
  order_count,
  units_sold,
  revenue,
  SUM(revenue) OVER (
    PARTITION BY product_id ORDER BY sale_date
  ) AS running_revenue,
  RANK() OVER (
    PARTITION BY sale_date ORDER BY revenue DESC
  ) AS revenue_rank
FROM daily;
