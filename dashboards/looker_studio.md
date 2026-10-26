# Dashboard spec (build in Looker Studio on top of the marts)

Source: `retail_marts.daily_product_revenue` (BigQuery).

## Pages

1. **Overview** — revenue trend (line), orders per day (bars), top 10
   products by revenue (table).
2. **Product detail** — filter by product: running revenue, units sold,
   revenue rank over time.
3. **Data health** — raw vs mart order counts by day (the reconciliation
   query in `sql/sample_queries.sql`), so a data problem is visible on
   the dashboard itself, not just in logs.

## Notes

- Keep the dashboard on the mart tables, never on raw events: the marts
  are deduplicated, validated, and far cheaper to query.
- All charts filter on `sale_date` so BigQuery prunes partitions.
