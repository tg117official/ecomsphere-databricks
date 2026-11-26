"""Create the lab namespace and seed configuration without resetting existing state."""
from .contracts import SOURCES, BRONZE_META, EVENT_DDL, LAYERS, CONTROL, AUDIT_DDL, REJECT_DDL

MARKER = 'TG117_ECOMSPHERE_SESSION17_V3'


def owns_lab(c):
    # Never assume an existing schema belongs to this application.
    marker = c.table('meta','lab_owner')
    return c.exists('meta','lab_owner') and c.scalar(f"SELECT count(*) FROM {marker} WHERE owner_marker='{MARKER}'") == 1


def setup(c):
    # Refuse to adopt a pre-existing namespace unless the marker proves ownership.
    names = {r[0].lower() for r in c.sql(f'SHOW SCHEMAS IN `{c.catalog}`').collect()}
    if not owns_lab(c):
        if any(f'{c.prefix}_{layer}' in names for layer in LAYERS):
            raise RuntimeError('Namespace already exists without lab ownership marker; choose a fresh prefix')
        c.sql(f'CREATE SCHEMA IF NOT EXISTS {c.schema("meta")}')
        c.create('meta','lab_owner','owner_marker STRING')
        c.sql(f"INSERT INTO {c.table('meta','lab_owner')} VALUES ('{MARKER}')")
    # Setup is safe to rerun: CREATE IF NOT EXISTS preserves all existing rows.
    for layer in LAYERS:
        c.sql(f'CREATE SCHEMA IF NOT EXISTS {c.schema(layer)}')
    for name, (_, ddl) in SOURCES.items():
        c.create('source',name,ddl)
        c.create('bronze','bronze_'+name,ddl+', '+BRONZE_META)
    c.create('bronze','bronze_order_event_log',EVENT_DDL)
    for name, ddl in CONTROL.items():
        c.create('meta',name,ddl)
    c.create('governance','pipeline_run_audit',AUDIT_DDL)
    c.create('quarantine','reject_history',REJECT_DDL)
    for name in ('landing','checkpoints'):
        c.sql(f'CREATE VOLUME IF NOT EXISTS {c.schema("source")}.`{name}`')
    c.dbutils.fs.mkdirs(c.volume('landing')+'/order_event_log')

    # Insert missing metadata rows only; never reset a watermark or configuration on a retry.
    p, w = c.table('meta','pipeline_config'), c.table('meta','cdc_watermark')
    for name, (key, _) in SOURCES.items():
        c.sql(f"""MERGE INTO {p} t USING (SELECT '{name}' source_object) s ON t.source_object=s.source_object
          WHEN NOT MATCHED THEN INSERT (pipeline_name,source_type,source_object,target_object,load_type,business_key,sequence_column,is_active,schedule_group,created_ts,updated_ts) VALUES ('ecomsphere17','DELTA',s.source_object,concat('bronze_',s.source_object),'INCREMENTAL','{key}','updated_at',true,'sales',current_timestamp(),current_timestamp())""")
        c.sql(f"""MERGE INTO {w} t USING (SELECT '{name}' source_object) s ON t.source_object=s.source_object
          WHEN NOT MATCHED THEN INSERT (source_system,source_object,watermark_column,last_watermark_ts_utc,overlap_minutes,last_successful_run_id,updated_ts,status,last_committed_batch,pending_batch) VALUES ('simulated_oltp',s.source_object,'updated_at',NULL,30,NULL,current_timestamp(),'READY',0,NULL)""")
    # File ingestion has its own checkpoint, not a relational watermark.
    c.sql(f"""MERGE INTO {p} t USING (SELECT 'order_event_log' source_object) s ON t.source_object=s.source_object
       WHEN NOT MATCHED THEN INSERT (pipeline_name,source_type,source_object,target_object,load_type,business_key,sequence_column,is_active,schedule_group,created_ts,updated_ts) VALUES ('ecomsphere17','JSON',s.source_object,'bronze_order_event_log','STREAM','event_id','event_timestamp',true,'sales',current_timestamp(),current_timestamp())""")
    # Persist a pending-batch marker before any source or target changes. A failed batch must be repaired first.
    states = c.sql(f'SELECT DISTINCT last_committed_batch,pending_batch FROM {w}').collect()
    if len(states)!=1 or c.scalar(f'SELECT count(*) FROM {w}')!=len(SOURCES):
        raise RuntimeError('Inconsistent control state; inspect before continuing')
    last, pending = states[0]
    if (pending is not None and pending != c.batch) or (pending is None and c.batch not in (last,last+1)):
        raise ValueError(f'Batch order violation: committed={last}, pending={pending}, requested={c.batch}')
    active = {r[0] for r in c.sql(f'SELECT source_object FROM {p} WHERE is_active').collect()}
    if active != set(SOURCES)|{'order_event_log'}:
        raise ValueError('All seven configured datasets are required for this integrated lab')
    c.sql(f'UPDATE {w} SET pending_batch={c.batch}')
    audit = c.table('governance','pipeline_run_audit')
    c.sql(f"""MERGE INTO {audit} t USING (SELECT {c.run} pipeline_run_id) s ON t.pipeline_run_id=s.pipeline_run_id
      WHEN MATCHED THEN UPDATE SET status='RUNNING',end_time=NULL,error_message=NULL
      WHEN NOT MATCHED THEN INSERT (pipeline_run_id,batch,start_time,end_time,status,trusted_lines,revenue,rejects,error_message) VALUES (s.pipeline_run_id,{c.batch},current_timestamp(),NULL,'RUNNING',NULL,NULL,NULL,NULL)""")
    print(f'Setup ready: batch={c.batch}, last committed={last}, pending={c.batch}')
