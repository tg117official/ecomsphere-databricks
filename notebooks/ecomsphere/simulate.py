"""Simulate an OLTP database and ADLS-style immutable JSON arrivals."""
import json
from .context import literal
from .contracts import SOURCES
from .fixtures import SNAPSHOTS, EVENTS


def generate_sources(c):
    # Upsert the deterministic source snapshot by natural key. A repeated batch is not a new event.
    for name, (key, _) in SOURCES.items():
        table = c.table('source',name)
        rows = SNAPSHOTS[c.batch][name]
        cols = list(rows[0])
        values = ','.join('('+','.join(literal(row[col]) for col in cols)+')' for row in rows)
        types = {f.name:f.dataType.simpleString() for f in c.spark.table(table).schema.fields}
        projection = ','.join(f'CAST(`{col}` AS {types[col]}) AS `{col}`' for col in cols)
        c.sql(f"""MERGE INTO {table} t USING (SELECT {projection} FROM VALUES {values} AS v({','.join(cols)})) s
          ON t.{key}=s.{key} WHEN MATCHED AND s.updated_at>=t.updated_at THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *""")
    # Generate a single immutable file per batch. Preserve the file identity used by the checkpoint.
    directory = c.volume('landing')+'/order_event_log'
    filename = f'batch_{c.batch:03d}.json'
    path = directory+'/'+filename
    payload = json.dumps(EVENTS[c.batch],sort_keys=True)+'\n'
    if filename in {f.name for f in c.dbutils.fs.ls(directory)}:
        if c.dbutils.fs.head(path,65536)!=payload:
            raise RuntimeError('Existing batch file differs; investigate rather than overwrite it')
    else:
        c.dbutils.fs.put(path,payload,False)
    print(f'Source batch {c.batch} ready: {path}')
