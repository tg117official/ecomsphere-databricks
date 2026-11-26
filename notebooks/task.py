# Databricks notebook source
# Read parameters injected by Lakeflow Jobs. The bundle supplies the deployed module directory.
import sys
for name, default in [('stage',''),('catalog',''),('prefix',''),('batch','1'),('run_id',''),('module_path',''),('confirmation','')]:
    dbutils.widgets.text(name,default)
module_path = dbutils.widgets.get('module_path')
# Use an absolute workspace path; do not rely on the notebook's serverless working directory.
if not module_path.startswith('/Workspace/'):
    module_path = '/Workspace' + module_path
sys.path.insert(0,module_path)

# Import normal Python modules from the synced workspace files (driver-side code only).
from ecomsphere.context import Context
from ecomsphere.setup import setup
from ecomsphere.simulate import generate_sources
from ecomsphere.ingestion import ingest
from ecomsphere.silver import process_silver
from ecomsphere.gold import model_gold
from ecomsphere.operations import reconcile, finalize, record_failure, reset

# Each task starts an independent notebook session. Persistent Delta tables and volumes carry state.
c = Context(spark,dbutils,dbutils.widgets.get('catalog'),dbutils.widgets.get('prefix'),dbutils.widgets.get('run_id'),int(dbutils.widgets.get('batch')))
stage = dbutils.widgets.get('stage')
# Dispatch the requested stage; let exceptions propagate so Jobs can mark failed tasks correctly.
actions = {'setup':setup,'simulate':generate_sources,'ingest':ingest,'silver':process_silver,'gold':model_gold,'reconcile':reconcile,'finalize':finalize,'failure':record_failure}
if stage == 'reset':
    reset(c,dbutils.widgets.get('confirmation'))
elif stage in actions:
    actions[stage](c)
else:
    raise ValueError(f'Unknown task stage: {stage}')
