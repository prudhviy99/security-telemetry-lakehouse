"""Exercise the real write-stage functions against a serverless SQL boundary.

Extracting these functions lets this narrow regression run without installing
Spark. Spark integration and real workspace execution remain separate checks.
"""
from __future__ import annotations

import __future__
import ast
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from telemetry.config import PipelineConfig
from telemetry.contracts import validate_customer_snapshot

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT/'src/telemetry/transforms.py').read_text()


def write_stage_context(backend):
    names = {'_location', 'run_dimension', '_gold_select', '_overwrite_sql', 'run_gold'}
    nodes = [node for node in ast.parse(SOURCE).body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = dict(_prepare_tables=Mock(), _frame=Mock(return_value='customer-frame'),
                     _save_frame=Mock(), CUSTOMER_SCHEMA=object(),
                     validate_customer_snapshot=validate_customer_snapshot,
                     refresh_silver_events=Mock(), UNKNOWN_SEGMENT='UNKNOWN')
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'telemetry/transforms.py',
                 'exec',flags=__future__.annotations.compiler_flag),namespace)
    cloud = backend == 'databricks'
    config = PipelineConfig(backend=backend,catalog='workspace' if cloud else 'local',
        namespace='telemetry_test',events_path='/Volumes/workspace/demo/io/events' if cloud else 'data/events.jsonl',
        customers_path='/Volumes/workspace/demo/io/customers.jsonl' if cloud else 'data/customers.jsonl',
        bronze_checkpoint='/Volumes/workspace/demo/io/bronze' if cloud else 'checkpoints/bronze',
        silver_checkpoint='/Volumes/workspace/demo/io/silver' if cloud else 'checkpoints/silver',
        job_run_id='regression')
    spark = Mock()
    spark.read.text.return_value.collect.return_value = [SimpleNamespace(value=json.dumps({
        'customer_id':'known','signup_date':'2026-01-01','plan_tier':'premium',
        'country_code':'US','acquisition_channel':'organic','updated_at':'2026-01-01T00:00:00Z'}))]
    def execute_sql(statement):
        if cloud and statement.strip().upper().startswith('REFRESH TABLE'):
            raise RuntimeError('[NOT_SUPPORTED_WITH_SERVERLESS] REFRESH TABLE')
        return Mock()
    spark.sql.side_effect = execute_sql
    return namespace, spark, config


class ServerlessWrites(unittest.TestCase):
    def test_dimension_publishes_without_unsupported_serverless_refresh(self):
        functions,spark,config=write_stage_context('databricks')
        functions['run_dimension'](spark,config)
        write=functions['_save_frame'].call_args
        self.assertEqual(write.args[2:4],('dim_customer','overwrite'))
        self.assertEqual(write.kwargs,{'overwriteSchema':'true'})

    def test_all_three_gold_marts_publish_on_serverless_boundary(self):
        functions,spark,config=write_stage_context('databricks')
        functions['run_gold'](spark,config)
        self.assertEqual([call.args[2] for call in functions['_save_frame'].call_args_list],
                         ['gold_device_day','gold_product_version_day','gold_signup_cohort_day'])

    def test_local_path_tables_still_refresh_after_overwrite(self):
        functions,spark,config=write_stage_context('local')
        functions['run_dimension'](spark,config)
        functions['run_gold'](spark,config)
        refreshes=[call.args[0] for call in spark.sql.call_args_list
                   if call.args[0].startswith('REFRESH TABLE')]
        self.assertEqual(refreshes,[f'REFRESH TABLE {config.qualified(table)}' for table in
            ('dim_customer','gold_device_day','gold_product_version_day','gold_signup_cohort_day')])


if __name__=='__main__':
    unittest.main()
