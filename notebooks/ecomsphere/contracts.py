"""Typed source contracts and the exact resources owned by this lab."""

# Source names, business keys and types follow the supplied Bronze guide.
# IDs are strings; money uses fixed precision; all timestamps are UTC.
SOURCES = {
    'customer': ('customer_id', 'customer_id STRING, first_name STRING, last_name STRING, email STRING, phone_number STRING, status_code STRING, created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted INT'),
    'product_variant': ('product_variant_id', 'product_variant_id STRING, product_id STRING, sku STRING, variant_name STRING, created_at TIMESTAMP, updated_at TIMESTAMP, is_active INT, is_deleted INT'),
    'orders': ('order_id', 'order_id STRING, customer_id STRING, order_date TIMESTAMP, order_status_code STRING, order_channel_code STRING, currency_code STRING, billing_address_id STRING, shipping_address_id STRING, total_item_amount DECIMAL(18,2), total_discount_amount DECIMAL(18,2), total_tax_amount DECIMAL(18,2), total_shipping_amount DECIMAL(18,2), grand_total_amount DECIMAL(18,2), created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted INT'),
    'order_item': ('order_item_id', 'order_item_id STRING, order_id STRING, product_variant_id STRING, quantity INT, base_unit_price DECIMAL(18,2), discount_amount DECIMAL(18,2), tax_amount DECIMAL(18,2), shipping_amount DECIMAL(18,2), line_total_amount DECIMAL(18,2), created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted INT'),
    'payment': ('payment_id', 'payment_id STRING, order_id STRING, payment_method_code STRING, payment_status_code STRING, amount DECIMAL(18,2), currency_code STRING, transaction_reference STRING, payment_date TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted INT'),
    'shipment': ('shipment_id', 'shipment_id STRING, order_id STRING, warehouse_id STRING, carrier_name STRING, shipment_status_code STRING, tracking_number STRING, shipped_date TIMESTAMP, delivered_date TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP, is_deleted INT'),
}
BRONZE_META = '_ingest_ts TIMESTAMP, bronze_load_date DATE, source_system STRING, source_object STRING, pipeline_run_id STRING, source_record_ts TIMESTAMP, record_hash STRING, cdc_op STRING, cdc_extracted_at TIMESTAMP, cdc_run_id STRING'
# This event projection intentionally retains the raw JSON line. It is a lab subset of the full event contract.
EVENT_DDL = 'event_id STRING, order_id STRING, event_type STRING, event_timestamp TIMESTAMP, raw_record_json STRING, _source_file STRING, _ingest_ts TIMESTAMP, pipeline_run_id STRING, record_hash STRING'
LAYERS = ('meta', 'source', 'bronze', 'silver', 'gold', 'quarantine', 'governance')
CONTROL = {
    'pipeline_config': 'pipeline_name STRING, source_type STRING, source_object STRING, target_object STRING, load_type STRING, business_key STRING, sequence_column STRING, is_active BOOLEAN, schedule_group STRING, created_ts TIMESTAMP, updated_ts TIMESTAMP',
    'cdc_watermark': 'source_system STRING, source_object STRING, watermark_column STRING, last_watermark_ts_utc TIMESTAMP, overlap_minutes INT, last_successful_run_id STRING, updated_ts TIMESTAMP, status STRING, last_committed_batch INT, pending_batch INT',
    'load_control': 'pipeline_run_id STRING, dataset_name STRING, load_start_ts TIMESTAMP, load_end_ts TIMESTAMP, load_mode STRING, lower_watermark_ts TIMESTAMP, upper_watermark_ts TIMESTAMP, records_read BIGINT, records_written BIGINT, status STRING',
}
AUDIT_DDL = 'pipeline_run_id STRING, batch INT, start_time TIMESTAMP, end_time TIMESTAMP, status STRING, trusted_lines BIGINT, revenue DECIMAL(18,2), rejects BIGINT, error_message STRING'
REJECT_DDL = 'pipeline_run_id STRING, batch INT, source_object STRING, source_record_key STRING, failed_rules STRING, raw_record_json STRING, detected_ts TIMESTAMP'
# Reset is allowlisted; the ownership marker is deliberately preserved.
OUTPUTS = {'silver': ['silver_customer_profile','silver_order_line','silver_order_event','silver_data_quality_audit'], 'gold': ['dim_customer','fact_sales_order_item','ecommerce_sales_obt','mart_daily_sales'], 'quarantine': ['current_rejects','reject_history'], 'governance': ['pipeline_run_audit']}
