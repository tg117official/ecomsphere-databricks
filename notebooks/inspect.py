# Databricks notebook source
# Read-only lab inspection. Set widgets to the exact catalog and prefix used in bundle deployment.
import re
dbutils.widgets.text('catalog','workspace')
dbutils.widgets.text('prefix','ecomsphere17_dev')
catalog = dbutils.widgets.get('catalog')
prefix = dbutils.widgets.get('prefix')
if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',catalog) or not re.fullmatch(r'ecomsphere17_[a-z0-9_]+',prefix):
    raise ValueError('Invalid lab namespace')
spark.conf.set('spark.sql.session.timeZone','UTC')

def table(layer,name):
    return f'`{catalog}`.`{prefix}_{layer}`.`{name}`'

# COMMAND ----------
# A single compact snapshot of Bronze counts, trusted data and sales totals.
queries = []
for source in ('customer','orders','order_item','payment','shipment','product_variant','order_event_log'):
    queries.append(f"SELECT 'bronze_{source}' metric,CAST(count(*) AS STRING) value FROM {table('bronze','bronze_'+source)}")
for layer,name in [('silver','silver_customer_profile'),('silver','silver_order_line'),('silver','silver_order_event'),('gold','dim_customer'),('gold','fact_sales_order_item'),('quarantine','current_rejects'),('quarantine','reject_history')]:
    queries.append(f"SELECT '{name}' metric,CAST(count(*) AS STRING) value FROM {table(layer,name)}")
queries.append(f"SELECT 'revenue_INR' metric,CAST(sum(net_line_amount) AS STRING) value FROM {table('gold','fact_sales_order_item')}")
display(spark.sql(' UNION ALL '.join(queries)))

# COMMAND ----------
# Compare raw and trusted records, historical customer versions and grain-safe context.
display(spark.sql(f"SELECT customer_id,email,source_record_ts,cdc_run_id FROM {table('bronze','bronze_customer')} ORDER BY source_record_ts,customer_id,cdc_run_id"))
display(spark.sql(f"SELECT customer_id,email FROM {table('silver','silver_customer_profile')} ORDER BY customer_id"))
display(spark.sql(f"SELECT order_item_id,order_id,net_line_amount,payment_id,shipment_id FROM {table('gold','fact_sales_order_item')} ORDER BY order_item_id"))
display(spark.sql(f"SELECT customer_id,email,effective_from_ts,effective_to_ts,is_current FROM {table('gold','dim_customer')} ORDER BY customer_id,effective_from_ts"))
display(spark.sql(f"SELECT sales_date,total_orders,total_order_items,total_revenue FROM {table('gold','mart_daily_sales')} ORDER BY sales_date"))

# COMMAND ----------
# Inspect current DQ exceptions separately from historical evidence and committed progress.
display(spark.sql(f"SELECT batch,source_object,source_record_key,failed_rules FROM {table('quarantine','current_rejects')} ORDER BY source_object,source_record_key"))
display(spark.sql(f"SELECT batch,source_object,source_record_key,failed_rules FROM {table('quarantine','reject_history')} ORDER BY batch,source_object,source_record_key"))
display(spark.sql(f"SELECT batch,pipeline_run_id,status,trusted_lines,revenue,rejects,error_message FROM {table('governance','pipeline_run_audit')} ORDER BY start_time"))
display(spark.sql(f"SELECT source_object,last_watermark_ts_utc,last_committed_batch,pending_batch,last_successful_run_id FROM {table('meta','cdc_watermark')} ORDER BY source_object"))
display(spark.sql(f"SELECT pipeline_run_id,dataset_name,lower_watermark_ts,upper_watermark_ts,records_read,records_written,status FROM {table('meta','load_control')} ORDER BY load_start_ts,dataset_name"))
