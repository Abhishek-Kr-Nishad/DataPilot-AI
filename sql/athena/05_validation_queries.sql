-- Read-only Amazon Athena Validation Queries for DataPilot AI

-- 1. Layer-to-layer row reconciliation
SELECT 'bronze' AS layer, COUNT(*) AS row_count FROM datapilot_ecommerce_db.bronze_orders
UNION ALL
SELECT 'silver' AS layer, COUNT(*) AS row_count FROM datapilot_ecommerce_db.silver_orders
UNION ALL
SELECT 'quarantine' AS layer, COUNT(*) AS row_count FROM datapilot_ecommerce_db.silver_quarantine;

-- 2. Duplicate order_id audit in Bronze
SELECT order_id, COUNT(*) AS duplicate_count
FROM datapilot_ecommerce_db.bronze_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 25;

-- 3. Quarantine reason breakdown
SELECT _quarantine_reason, COUNT(*) AS affected_rows
FROM datapilot_ecommerce_db.silver_quarantine
GROUP BY _quarantine_reason
ORDER BY affected_rows DESC;

-- 4. Gold vs Silver net revenue check
SELECT
  ROUND(SUM(s.net_amount), 2) AS silver_net_revenue,
  (SELECT ROUND(SUM(g.net_revenue), 2) FROM datapilot_ecommerce_db.gold_daily_revenue g) AS gold_net_revenue
FROM datapilot_ecommerce_db.silver_orders s;
