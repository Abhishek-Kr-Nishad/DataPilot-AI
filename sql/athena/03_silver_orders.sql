-- Silver Layer & Quarantine External Tables
-- Replace <YOUR_BUCKET> with your AWS_S3_BUCKET value before executing in Athena.
CREATE EXTERNAL TABLE IF NOT EXISTS datapilot_ecommerce_db.silver_orders (
  order_id string,
  customer_id string,
  order_timestamp timestamp,
  order_date date,
  product_id string,
  product_name string,
  category string,
  quantity int,
  unit_price double,
  discount_pct double,
  gross_amount double,
  discount_amount double,
  net_amount double,
  currency string,
  payment_method string,
  order_status string,
  country string,
  region string,
  _batch_id string,
  _source_file string,
  _ingested_at timestamp,
  _cleaned_at timestamp
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/datapilot-ai/silver/';

CREATE EXTERNAL TABLE IF NOT EXISTS datapilot_ecommerce_db.silver_quarantine (
  order_id string,
  customer_id string,
  order_timestamp string,
  product_id string,
  product_name string,
  category string,
  quantity string,
  unit_price string,
  discount_pct string,
  currency string,
  order_status string,
  country string,
  region string,
  _batch_id string,
  _source_file string,
  _ingested_at timestamp,
  _quarantine_reason string
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/datapilot-ai/quarantine/';
