"""Metadata-driven query CDC and bounded file streaming."""
from .contracts import SOURCES


def ingest_relational(c, row):
    # Validate metadata against known contracts before using it in identifiers.
    name, target, key, sequence = row.source_object,row.target_object,row.business_key,row.sequence_column
    if name not in SOURCES or target!='bronze_'+name or key!=SOURCES[name][0] or sequence!='updated_at':
        raise ValueError(f'Unsupported configuration: {name}')
    w = c.table('meta','cdc_watermark')
    src, dst = c.table('source',name), c.table('bronze',target)
    # Read the last successful timestamp and subtract the configured overlap in minutes.
    # Resolve the scalar boundary once so the same exact timestamp is used in extraction and audit.
    lower_value = c.scalar(f"SELECT date_format(coalesce(timestampadd(MINUTE,-overlap_minutes,last_watermark_ts_utc),TIMESTAMP '1900-01-01 00:00:00'),'yyyy-MM-dd HH:mm:ss.SSSSSS') FROM {w} WHERE source_object='{name}'")
    lower = f"TIMESTAMP '{lower_value}'"
    cols = c.spark.table(src).columns
    hashed = ','.join(f's.`{col}`' for col in cols)
    # Retain source values and attach technical metadata. cdc_run_id identifies a logical batch.
    c.sql(f"""CREATE OR REPLACE TEMP VIEW extracted AS SELECT s.*,current_timestamp() _ingest_ts,current_date() bronze_load_date,
      'simulated_oltp' source_system,'{name}' source_object,{c.run} pipeline_run_id,s.updated_at source_record_ts,
      sha2(to_json(struct({hashed}),map('ignoreNullFields','false')),256) record_hash,
      CASE WHEN s.is_deleted=1 THEN 'D' ELSE 'UPSERT' END cdc_op,current_timestamp() cdc_extracted_at,'batch_{c.batch:03d}' cdc_run_id
      FROM {src} s WHERE s.{sequence}>={lower} AND s.{sequence}<={c.upper}""")
    read = c.scalar('SELECT count(*) FROM extracted')
    # Insert-only MERGE: overlap is retained across batches but repeating the same batch cannot duplicate a version.
    condition = f't.`{key}`=s.`{key}` AND t.cdc_run_id=s.cdc_run_id AND t.record_hash=s.record_hash'
    before = c.scalar(f'SELECT count(*) FROM {dst}')
    c.sql(f'MERGE INTO {dst} t USING extracted s ON {condition} WHEN NOT MATCHED THEN INSERT *')
    written = c.scalar(f'SELECT count(*) FROM {dst}')-before
    # Stage counts for operational inspection. Watermarks are not advanced here.
    lc = c.table('meta','load_control')
    c.sql(f"""MERGE INTO {lc} t USING (SELECT {c.run} pipeline_run_id,'{name}' dataset_name) s
      ON t.pipeline_run_id=s.pipeline_run_id AND t.dataset_name=s.dataset_name
      WHEN MATCHED THEN UPDATE SET load_end_ts=current_timestamp(),records_read={read},records_written=coalesce(t.records_written,0)+{written},status='STAGED'
      WHEN NOT MATCHED THEN INSERT VALUES(s.pipeline_run_id,s.dataset_name,current_timestamp(),current_timestamp(),'INCREMENTAL',{lower},{c.upper},{read},{written},'STAGED')""")
    print(f'{name}: read={read}, appended={written}')


def ingest_events(c):
    # Auto Loader reads immutable JSON lines as text, preserving the exact raw payload.
    from pyspark.sql import functions as F
    root = c.volume('checkpoints')+'/order_event_log'
    raw = (c.spark.readStream.format('cloudFiles').option('cloudFiles.format','text')
           .option('cloudFiles.useManagedFileEvents','false')
           .option('cloudFiles.schemaLocation',root+'/schema').schema('value STRING')
           .load(c.volume('landing')+'/order_event_log'))
    parsed = raw.select(F.from_json('value','event_id STRING, order_id STRING, event_type STRING, event_timestamp STRING').alias('event'),
                        F.col('value').alias('raw_record_json'),F.col('_metadata.file_path').alias('_source_file'))
    # Parse event fields and append to Delta; the durable checkpoint tracks files across job runs.
    records = parsed.select('event.event_id','event.order_id','event.event_type',F.expr('try_cast(event.event_timestamp as timestamp)').alias('event_timestamp'),
                            'raw_record_json','_source_file',F.current_timestamp().alias('_ingest_ts'),F.lit(c.run_id).alias('pipeline_run_id'),F.sha2('raw_record_json',256).alias('record_hash'))
    query = (records.writeStream.format('delta').outputMode('append').option('checkpointLocation',root+'/stream')
             .trigger(availableNow=True).toTable(c.table('bronze','bronze_order_event_log')))
    query.awaitTermination()


def ingest(c):
    # The control table selects datasets and dispatches them to the appropriate adapter.
    p = c.table('meta','pipeline_config')
    rows = c.sql(f'SELECT * FROM {p} WHERE is_active ORDER BY source_object').collect()
    for row in rows:
        if row.source_type=='DELTA':
            ingest_relational(c,row)
        elif row.source_type=='JSON' and row.source_object=='order_event_log':
            ingest_events(c)
        else:
            raise ValueError(f'Unsupported source type: {row.source_type}')
