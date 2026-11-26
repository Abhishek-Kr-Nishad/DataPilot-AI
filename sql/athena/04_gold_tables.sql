-- Gold Analytical Aggregation Tables
-- Replace <YOUR_BUCKET> with your AWS_S3_BUCKET value before executing in Athena.
CREATE EXTERNAL TABLE IF NOT EXISTS datapilot_ecommerce_db.gold_daily_revenue (
  order_date date,
  total_orders bigint,
  completed_orders bigint,
  total_units bigint,
  gross_revenue double,
  total_discounts double,
  net_revenue double,
  avg_order_value double
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/datapilot-ai/gold/daily_revenue/';

CREATE EXTERNAL TABLE IF NOT EXISTS datapilot_ecommerce_db.gold_category_performance (
  category string,
  total_orders bigint,
  total_units bigint,
  gross_revenue double,
  net_revenue double,
  avg_unit_price double,
  return_or_cancel_count bigint
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/datapilot-ai/gold/category_performance/';

CREATE EXTERNAL TABLE IF NOT EXISTS datapilot_ecommerce_db.gold_regional_sales (
  region string,
  country string,
  currency string,
  total_orders bigint,
  unique_customers bigint,
  net_revenue double,
  avg_order_value double
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/datapilot-ai/gold/regional_sales/';

CREATE EXTERNAL TABLE IF NOT EXISTS datapilot_ecommerce_db.gold_customer_ltv_summary (
  customer_id string,
  order_count bigint,
  total_units bigint,
  lifetime_net_spend double,
  first_order_at timestamp,
  last_order_at timestamp
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/datapilot-ai/gold/customer_ltv_summary/';
