-- Bronze Layer External Table (raw CSV ingested as Parquet with lineage metadata)
-- Replace <YOUR_BUCKET> with your AWS_S3_BUCKET value before executing in Athena.
CREATE EXTERNAL TABLE IF NOT EXISTS datapilot_ecommerce_db.bronze_orders (
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
  payment_method string,
  order_status string,
  country string,
  region string,
  _batch_id string,
  _source_file string,
  _ingested_at timestamp,
  _execution_mode string
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/datapilot-ai/bronze/';
