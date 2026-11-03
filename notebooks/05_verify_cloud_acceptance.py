# Databricks notebook source
# A fresh task verifies durable evidence after the producer task has terminated.
import json
from telemetry.cloud_reports import validate_report

defaults = dict(volume_root='',job_run_id='',include_extended_checks='false',
                include_failure_injection='false',upstream_result_state='')
parameters = {}
for name, default in defaults.items():
    try:
        parameters[name] = dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name,default)
        parameters[name] = dbutils.widgets.get(name)

run_id=parameters['job_run_id']
if not run_id.isdigit():
    raise ValueError('Expected the actual numeric Lakeflow run ID')
root=parameters['volume_root'].rstrip('/')
if not root.startswith('/Volumes/') or '..' in root.split('/'):
    raise ValueError('Expected a managed-volume report root')
path=root+'/'+run_id+'/result.json'
rows=spark.read.option('wholetext','true').text(path).collect()
if len(rows)!=1:
    raise ValueError('Expected exactly one complete acceptance report')
report=json.loads(rows[0].value)
result=validate_report(report,run_id,
    extended=parameters['include_extended_checks']=='true',
    failures=parameters['include_failure_injection']=='true',
    upstream_state=parameters['upstream_result_state'])
print('CLOUD_ACCEPTANCE_VERIFIED '+json.dumps(result,sort_keys=True))
dbutils.notebook.exit(json.dumps(result,sort_keys=True))
