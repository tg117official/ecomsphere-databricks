"""Canonical sales projection, observed customer history and serving models."""


def build_customer_dimension(c):
    # Deduplicate overlapping observations, then form SCD2 intervals from changes to tracked fields.
    # Source-query CDC can only preserve versions that were actually observed by extraction.
    b = c.table('bronze','bronze_customer')
    c.replace('gold','dim_customer',f"""WITH versions AS (
      SELECT customer_id,concat_ws(' ',trim(first_name),trim(last_name)) full_name,lower(trim(email)) email,status_code customer_status,
       source_record_ts effective_from_ts,is_deleted,sha2(to_json(named_struct('name',concat_ws(' ',trim(first_name),trim(last_name)),'email',lower(trim(email)),'status',status_code,'deleted',is_deleted)),256) version_hash,
       row_number() OVER(PARTITION BY customer_id,source_record_ts ORDER BY _ingest_ts DESC,record_hash DESC) rn
      FROM {b} WHERE source_record_ts<={c.upper} AND customer_id IS NOT NULL AND trim(coalesce(first_name,''))<>'' AND trim(coalesce(email,''))<>''
      ), ordered AS (SELECT *,lag(version_hash) OVER(PARTITION BY customer_id ORDER BY effective_from_ts) previous_hash FROM versions WHERE rn=1),
      changes AS (SELECT *,lead(effective_from_ts) OVER(PARTITION BY customer_id ORDER BY effective_from_ts) effective_to_ts FROM ordered WHERE previous_hash IS NULL OR version_hash<>previous_hash)
      SELECT sha2(concat_ws('|',customer_id,cast(effective_from_ts AS STRING)),256) customer_key,customer_id,full_name,email,customer_status,effective_from_ts,effective_to_ts,
      effective_to_ts IS NULL is_current,current_timestamp() gold_processed_ts,{c.run} pipeline_run_id FROM changes WHERE is_deleted=0""")


def build_sales_fact(c):
    # Resolve customer keys as of the order timestamp; interval boundaries are [from,to).
    # The canonical monetary definition is the trusted line total including tax and shipping.
    s,d = c.table('silver','silver_order_line'),c.table('gold','dim_customer')
    c.replace('gold','fact_sales_order_item',f"""SELECT sha2(s.order_item_id,256) sales_fact_key,s.order_item_id,s.order_id,coalesce(d.customer_key,'-1') customer_key,
      s.customer_id,s.product_id,s.product_variant_id,s.order_date,s.currency_code,s.quantity,
      CAST(s.quantity*s.base_unit_price AS DECIMAL(18,2)) gross_amount,s.discount_amount,s.tax_amount,s.shipping_amount,s.line_total_amount net_line_amount,
      s.payment_id,s.payment_status_code,s.shipment_id,s.shipment_status_code,current_timestamp() gold_processed_ts,{c.run} pipeline_run_id
      FROM {s} s LEFT JOIN {d} d ON s.customer_id=d.customer_id AND s.order_date>=d.effective_from_ts AND (s.order_date<d.effective_to_ts OR d.effective_to_ts IS NULL)""")


def build_serving(c):
    # Reuse the canonical fact instead of reimplementing revenue calculations in serving models.
    f,d = c.table('gold','fact_sales_order_item'),c.table('gold','dim_customer')
    c.replace('gold','ecommerce_sales_obt',f'SELECT f.*,d.full_name customer_full_name,d.email customer_email,d.customer_status FROM {f} f LEFT JOIN {d} d ON f.customer_key=d.customer_key')
    c.replace('gold','mart_daily_sales',f"""SELECT cast(order_date AS DATE) sales_date,count(DISTINCT order_id) total_orders,count(*) total_order_items,
      sum(net_line_amount) total_revenue,sum(discount_amount) total_discount,sum(shipping_amount) total_shipping,sum(tax_amount) total_tax,
      CAST(try_divide(sum(net_line_amount),count(DISTINCT order_id)) AS DECIMAL(18,2)) avg_order_value,current_timestamp() last_refreshed_ts,{c.run} pipeline_run_id FROM {f} GROUP BY cast(order_date AS DATE)""")


def model_gold(c):
    build_customer_dimension(c)
    build_sales_fact(c)
    build_serving(c)
