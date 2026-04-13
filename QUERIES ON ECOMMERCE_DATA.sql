--QUERY 1 — How Much Revenue Did We Make? (Basic Sanity Check)
SELECT
order_id,
order_item_id,
price,
freight_value,
revenue
FROM fact_order_items

SELECT
order_id,
SUM(revenue) [Total revenue per order]
FROM fact_order_items
GROUP BY order_id

--QUERY 2 — Revenue by Month (Trend Analysis)
--QUERY 3 — Running Total Revenue (Window Function)
--QUERY 4 — Top 10 Product Categories by Revenue
--QUERY 5 — Late Delivery Rate by Seller State
--QUERY 6 — Customer Review Score by Category
--QUERY 7 — Payment Method Analysis
--QUERY 8 — Customer Geography (Top States)
--QUERY 9 — Seller Performance Scorecard
--QUERY 10 — Day of Week Analysis