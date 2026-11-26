"""Reconciliation, commit boundary, failure logging and explicit lab reset."""
from decimal import Decimal
from .fixtures import EXPECTED
from .contracts import SOURCES, CONTROL, OUTPUTS
from .setup import owns_lab


def metrics(c):
    # Compare trusted line counts and sums across all sales-serving outputs.
    line = c.table('silver','silver_order_line')
    fact, obt, mart = (c.table('gold',name) for name in ('fact_sales_order_item','ecommerce_sales_obt','mart_daily_sales'))
    def sales(table, amount):
        return tuple(c.sql(f'SELECT count(*),count(DISTINCT order_item_id),coalesce(sum({amount}),0) FROM {table}').first())
    sl, f, wide = sales(line,'line_total_amount'),sales(fact,'net_line_amount'),sales(obt,'net_line_amount')
    m = tuple(c.sql(f'SELECT coalesce(sum(total_order_items),0),coalesce(sum(total_revenue),0) FROM {mart}').first())
    rejects = c.scalar(f'SELECT count(*) FROM {c.table("quarantine","current_rejects")}')
    events = c.scalar(f'SELECT count(*) FROM {c.table("silver","silver_order_event")}')
    versions = c.scalar(f'SELECT count(*) FROM {c.table("gold","dim_customer")}')
    if not (sl==f==wide and f[0]==f[1] and m==(f[0],f[2])):
        raise RuntimeError(f'Sales grain/amount reconciliation failed: {sl}, {f}, {wide}, {m}')
    current_duplicates = c.scalar(f"SELECT count(*) FROM (SELECT customer_id FROM {c.table('gold','dim_customer')} WHERE is_current GROUP BY customer_id HAVING count(*)<>1)")
    if current_duplicates or c.scalar(f"SELECT count(*) FROM {fact} WHERE customer_key='-1'"):
        raise RuntimeError('Customer dimension resolution failed')
    actual = (f[0],f[2],rejects,events,versions)
    if actual != EXPECTED[c.batch]:
        raise RuntimeError(f'Fixture check failed: expected {EXPECTED[c.batch]}, observed {actual}')
    return f[0],Decimal(f[2]),rejects


def reconcile(c):
    # Quarantined business rows are expected in batch 2; structural or reconciliation failures stop the job.
    n,revenue,rejects = metrics(c)
    print(f'PASS: trusted lines={n}; revenue={revenue}; current rejects={rejects}')


def finalize(c):
    # Recheck data before committing progress. All six relational watermarks advance in one Delta UPDATE.
    n,revenue,rejects = metrics(c)
    w,a,lc = c.table('meta','cdc_watermark'),c.table('governance','pipeline_run_audit'),c.table('meta','load_control')
    pending = c.scalar(f'SELECT count(*) FROM {w} WHERE pending_batch={c.batch}')
    already_committed = c.scalar(f'SELECT count(*) FROM {w} WHERE last_successful_run_id={c.run} AND pending_batch IS NULL AND last_committed_batch={c.batch}')
    if pending == len(SOURCES):
        c.sql(f"UPDATE {w} SET last_watermark_ts_utc={c.upper},last_committed_batch={c.batch},pending_batch=NULL,last_successful_run_id={c.run},updated_ts=current_timestamp(),status='SUCCESS'")
    elif already_committed != len(SOURCES):
        raise RuntimeError('Pending batch changed; do not finalize')
    # If finalization was retried after its commit, finish audit logging without another state transition.
    # Audit updates are separate Delta transactions. The watermark commit is authoritative if logging fails afterwards.
    c.sql(f"UPDATE {lc} SET status='SUCCESS' WHERE pipeline_run_id={c.run}")
    c.sql(f"UPDATE {a} SET status='SUCCESS',end_time=current_timestamp(),trusted_lines={n},revenue={revenue},rejects={rejects},error_message=NULL WHERE pipeline_run_id={c.run}")
    print(f'Committed batch {c.batch}, revenue={revenue}')


def record_failure(c):
    # Failure handler is triggered only when an upstream task fails. No watermark is advanced here.
    if not owns_lab(c):
        print('No lab ownership marker; inspect setup error in Jobs UI')
        return
    a,w,lc = c.table('governance','pipeline_run_audit'),c.table('meta','cdc_watermark'),c.table('meta','load_control')
    if not c.exists('governance','pipeline_run_audit'):
        print('Audit table not initialized; inspect setup task output')
        return
    committed = c.exists('meta','cdc_watermark') and c.scalar(f'SELECT count(*) FROM {w} WHERE last_successful_run_id={c.run} AND pending_batch IS NULL')==len(SOURCES)
    status = 'COMMITTED_WITH_ERROR' if committed else 'FAILED'
    # Keep the audit status honest if the watermark committed but subsequent logging failed.
    c.sql(f"""MERGE INTO {a} t USING (SELECT {c.run} pipeline_run_id) s ON t.pipeline_run_id=s.pipeline_run_id
      WHEN MATCHED THEN UPDATE SET status='{status}',end_time=current_timestamp(),error_message='Inspect failed task output in Jobs UI'
      WHEN NOT MATCHED THEN INSERT VALUES(s.pipeline_run_id,{c.batch},current_timestamp(),current_timestamp(),'{status}',NULL,NULL,NULL,'Inspect failed task output in Jobs UI')""")
    if c.exists('meta','load_control'):
        c.sql(f"UPDATE {lc} SET status='{status}' WHERE pipeline_run_id={c.run}")


def reset(c, confirmation):
    # Do not run reset concurrently with the pipeline. The explicit confirmation and ownership marker
    # protect unrelated resources. Only allowlisted tables and the two lab-managed volumes are dropped.
    if confirmation!=f'RESET_{c.prefix}' or not owns_lab(c):
        raise ValueError('Reset refused: verify lab ownership and confirmation')
    for layer,names in OUTPUTS.items():
        for name in names:
            c.sql(f'DROP TABLE IF EXISTS {c.table(layer,name)}')
    c.sql(f'DROP TABLE IF EXISTS {c.table("bronze","bronze_order_event_log")}')
    for name in SOURCES:
        c.sql(f'DROP TABLE IF EXISTS {c.table("source",name)}')
        c.sql(f'DROP TABLE IF EXISTS {c.table("bronze","bronze_"+name)}')
    for name in CONTROL:
        c.sql(f'DROP TABLE IF EXISTS {c.table("meta",name)}')
    for name in ('landing','checkpoints'):
        c.sql(f'DROP VOLUME IF EXISTS {c.schema("source")}.`{name}`')
    print('Reset complete. Ownership marker and schemas preserved; start again with batch 1.')
