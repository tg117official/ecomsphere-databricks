"""Deduplicate observed versions, classify quality failures and build trusted projections."""
from .contracts import SOURCES


def latest_views(c):
    # Choose the latest observed version BEFORE filtering deletes or quality failures.
    # Otherwise a newer invalid/deleted version could resurrect an older valid record.
    for name, (key, _) in SOURCES.items():
        c.sql(f"""CREATE OR REPLACE TEMP VIEW latest_{name} AS
          SELECT * EXCEPT(rn) FROM (SELECT *,row_number() OVER(PARTITION BY {key} ORDER BY source_record_ts DESC,_ingest_ts DESC,record_hash DESC) rn
          FROM {c.table('bronze','bronze_'+name)} WHERE source_record_ts<={c.upper}) WHERE rn=1 AND is_deleted=0""")


def classify(c):
    # Normalize customer attributes in Silver, never in Bronze. Mandatory customer fields are checked first.
    c.sql("""CREATE OR REPLACE TEMP VIEW customer_check AS SELECT *,
      CASE WHEN customer_id IS NULL OR trim(coalesce(first_name,''))='' OR trim(coalesce(email,''))='' THEN 'MANDATORY_CUSTOMER' END failure
      FROM latest_customer""")
    c.sql("""CREATE OR REPLACE TEMP VIEW valid_customer AS SELECT customer_id,trim(first_name) first_name,trim(last_name) last_name,
      concat_ws(' ',trim(first_name),trim(last_name)) full_name,lower(trim(email)) email,phone_number,status_code customer_status,
      source_record_ts source_updated_at,record_hash FROM customer_check WHERE failure IS NULL""")
    # Validate header totals using decimal arithmetic and a one-cent inclusive tolerance (DQ rule 36).
    c.sql("""CREATE OR REPLACE TEMP VIEW order_check AS SELECT o.*,
      CASE WHEN o.order_id IS NULL OR o.order_date IS NULL THEN 'MANDATORY_ORDER'
      WHEN c.customer_id IS NULL THEN 'ORDER_CUSTOMER_REFERENCE'
      WHEN o.grand_total_amount IS NULL OR o.total_item_amount IS NULL OR o.total_discount_amount IS NULL OR o.total_tax_amount IS NULL OR o.total_shipping_amount IS NULL
        OR least(o.grand_total_amount,o.total_item_amount,o.total_discount_amount,o.total_tax_amount,o.total_shipping_amount)<0 THEN 'INVALID_ORDER_AMOUNT'
      WHEN abs(o.grand_total_amount-(o.total_item_amount-o.total_discount_amount+o.total_tax_amount+o.total_shipping_amount))>CAST(0.01 AS DECIMAL(18,2)) THEN 'DQ36_ORDER_TOTAL'
      END failure FROM latest_orders o LEFT JOIN valid_customer c ON o.customer_id=c.customer_id""")
    # An invalid parent order makes its line untrustworthy. Check product variant and line arithmetic too.
    c.sql("""CREATE OR REPLACE TEMP VIEW line_check AS SELECT i.*,
      CASE WHEN i.order_item_id IS NULL THEN 'MANDATORY_ORDER_ITEM'
      WHEN o.order_id IS NULL OR o.failure IS NOT NULL THEN 'ORDER_ITEM_ORDER_REFERENCE'
      WHEN v.product_variant_id IS NULL THEN 'ORDER_ITEM_VARIANT_REFERENCE'
      WHEN i.quantity IS NULL OR i.quantity<=0 OR i.base_unit_price IS NULL OR i.discount_amount IS NULL OR i.tax_amount IS NULL OR i.shipping_amount IS NULL OR i.line_total_amount IS NULL
        OR least(i.base_unit_price,i.discount_amount,i.tax_amount,i.shipping_amount,i.line_total_amount)<0 THEN 'INVALID_LINE_AMOUNT_OR_QUANTITY'
      WHEN abs(i.line_total_amount-(i.quantity*i.base_unit_price-i.discount_amount+i.tax_amount+i.shipping_amount))>CAST(0.01 AS DECIMAL(18,2)) THEN 'LINE_TOTAL'
      END failure FROM latest_order_item i LEFT JOIN order_check o ON i.order_id=o.order_id
      LEFT JOIN latest_product_variant v ON i.product_variant_id=v.product_variant_id AND v.is_active=1""")
    # File records are deduplicated by event_id; invalid JSON/missing keys and invalid orders are rejected.
    c.sql(f"""CREATE OR REPLACE TEMP VIEW event_check AS SELECT e.*,CASE WHEN e.event_id IS NULL OR e.event_timestamp IS NULL OR e.event_type IS NULL THEN 'INVALID_EVENT'
      WHEN o.order_id IS NULL OR o.failure IS NOT NULL THEN 'EVENT_ORDER_REFERENCE' END failure
      FROM (SELECT * EXCEPT(rn) FROM (SELECT *,row_number() OVER(PARTITION BY coalesce(event_id,record_hash) ORDER BY event_timestamp DESC NULLS LAST,_ingest_ts DESC,record_hash DESC) rn
      FROM {c.table('bronze','bronze_order_event_log')} WHERE event_timestamp<={c.upper} OR event_timestamp IS NULL) WHERE rn=1) e
      LEFT JOIN order_check o ON e.order_id=o.order_id""")


def write_rejects_and_audit(c):
    # Maintain a current reject snapshot and an immutable per-job reject history.
    entities = [('customer','customer_check','customer_id'),('orders','order_check','order_id'),('order_item','line_check','order_item_id'),('order_event_log','event_check','event_id')]
    parts = []
    for name,view,key in entities:
        parts.append(f"SELECT {c.run} pipeline_run_id,{c.batch} batch,'{name}' source_object,coalesce({key},record_hash) source_record_key,failure failed_rules,to_json(struct(*)) raw_record_json,current_timestamp() detected_ts FROM {view} WHERE failure IS NOT NULL")
    c.replace('quarantine','current_rejects',' UNION ALL '.join(parts))
    current, history = c.table('quarantine','current_rejects'), c.table('quarantine','reject_history')
    c.sql(f"""MERGE INTO {history} t USING {current} s ON t.pipeline_run_id=s.pipeline_run_id AND t.source_object=s.source_object AND t.source_record_key=s.source_record_key
      WHEN NOT MATCHED THEN INSERT *""")
    # Store per-entity classification counts for this run. These are not all 50 individual DQ rules.
    audit = c.table('silver','silver_data_quality_audit')
    c.create('silver','silver_data_quality_audit','dq_run_id STRING, table_name STRING, rule_name STRING, rule_category STRING, total_records_checked BIGINT, failed_records_count BIGINT, passed_records_count BIGINT, failure_percentage DOUBLE, run_timestamp TIMESTAMP, severity STRING, status STRING, pipeline_run_id STRING')
    for name,view,_ in entities:
        c.sql(f"""MERGE INTO {audit} t USING (SELECT concat({c.run},':','{name}') dq_run_id,'{name}' table_name,'LAB_CLASSIFICATION' rule_name,'Integrated DQ' rule_category,
         count(*) total_records_checked,count(failure) failed_records_count,count(*)-count(failure) passed_records_count,
         coalesce(100.0*try_divide(count(failure),count(*)),0.0) failure_percentage,current_timestamp() run_timestamp,'CRITICAL' severity,
         CASE WHEN count(failure)>0 THEN 'REJECTS' ELSE 'PASS' END status,{c.run} pipeline_run_id FROM {view}) s
         ON t.dq_run_id=s.dq_run_id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def write_trusted(c):
    # Select a single payment and shipment per order before the line join. These are descriptive contexts,
    # not payment totals or shipment-item allocations. Tie-breakers make selection repeatable.
    c.sql("""CREATE OR REPLACE TEMP VIEW payment_context AS SELECT * EXCEPT(rn) FROM
      (SELECT *,row_number() OVER(PARTITION BY order_id ORDER BY payment_date DESC NULLS LAST,payment_id DESC) rn FROM latest_payment) WHERE rn=1""")
    c.sql("""CREATE OR REPLACE TEMP VIEW shipment_context AS SELECT * EXCEPT(rn) FROM
      (SELECT *,row_number() OVER(PARTITION BY order_id ORDER BY shipped_date DESC NULLS LAST,shipment_id DESC) rn FROM latest_shipment) WHERE rn=1""")
    # Rebuild the small Silver snapshots from trusted current state; a rejected order can recover on a later run.
    c.replace('silver','silver_customer_profile',f'SELECT *,current_timestamp() silver_processed_ts,{c.run} pipeline_run_id FROM valid_customer')
    c.replace('silver','silver_order_line',f"""SELECT i.order_item_id,i.order_id,o.customer_id,i.product_variant_id,v.product_id,o.order_date,o.currency_code,o.order_status_code,o.order_channel_code,
      i.quantity,i.base_unit_price,i.discount_amount,i.tax_amount,i.shipping_amount,i.line_total_amount,
      p.payment_id,p.payment_method_code,p.payment_status_code,s.shipment_id,s.warehouse_id,s.carrier_name,s.shipment_status_code,s.shipped_date,s.delivered_date,
      greatest(i.source_record_ts,o.source_record_ts) source_updated_at,current_timestamp() silver_processed_ts,{c.run} pipeline_run_id
      FROM line_check i JOIN order_check o ON i.order_id=o.order_id JOIN latest_product_variant v ON i.product_variant_id=v.product_variant_id
      LEFT JOIN payment_context p ON i.order_id=p.order_id LEFT JOIN shipment_context s ON i.order_id=s.order_id WHERE i.failure IS NULL""")
    c.replace('silver','silver_order_event',f"SELECT event_id,order_id,event_type,event_timestamp,current_timestamp() silver_processed_ts,{c.run} pipeline_run_id FROM event_check WHERE failure IS NULL")


def process_silver(c):
    # Keep task-level responsibilities visible for the code-tracing assignment.
    latest_views(c)
    classify(c)
    write_rejects_and_audit(c)
    write_trusted(c)
