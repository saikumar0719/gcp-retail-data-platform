-- Top 10 products by revenue, last 7 days (partition-filtered).
SELECT product_id, SUM(revenue) AS revenue_7d
FROM `{{project}}.retail_marts.daily_product_revenue`
WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY product_id
ORDER BY revenue_7d DESC
LIMIT 10;

-- Reconciliation: raw vs mart order counts for yesterday.
SELECT
  (SELECT COUNT(*) FROM `{{project}}.retail_raw.events`
   WHERE event_type = 'order'
     AND DATE(event_ts) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS raw_orders,
  (SELECT SUM(order_count) FROM `{{project}}.retail_marts.daily_product_revenue`
   WHERE sale_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS mart_orders;
