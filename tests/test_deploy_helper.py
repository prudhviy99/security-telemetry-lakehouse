"""Offline tests for the CLI handoff's data and failure safeguards."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock

spec=importlib.util.spec_from_file_location('deploy_databricks',Path(__file__).resolve().parents[1]/'scripts/deploy_databricks.py')
deploy=importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class DeploymentSafeguards(unittest.TestCase):
    def test_catalog_selection_uses_actual_workspace_response(self):
        self.assertEqual(deploy.choose_catalog({'catalogs':[{'name':'system'},{'name':'portfolio'}]}),'portfolio')
        self.assertEqual(deploy.choose_catalog([{'name':'workspace'},{'name':'other'}]),'workspace')
        with self.assertRaises(ValueError):
            deploy.choose_catalog([{'name':'one'},{'name':'two'}])

    def test_nonexistent_explicit_catalog_is_rejected(self):
        with self.assertRaises(ValueError):
            deploy.choose_catalog([{'name':'portfolio'}],'invented')

    def test_baseline_repeats_with_unchanged_parameters(self):
        steps=deploy.deployment_steps('portfolio','demo')
        self.assertEqual(steps[2],steps[3])
        self.assertEqual(steps[-1][-1],'telemetry_acceptance')
        self.assertFalse(any('failure_injection' in argument for step in steps for argument in step))
        extended=deploy.deployment_steps('portfolio','demo',True)
        self.assertIn('include_extended_checks=true,include_failure_injection=true',extended[-1])

    def test_exact_existing_input_can_be_reused_without_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            source=Path(directory)/'events.jsonl';source.write_bytes(b'reference\n')
            cli=Mock()
            def call(arguments,**kwargs):
                if arguments[:3]==['fs','cp','dbfs:/Volumes/c/s/v/events.jsonl']:
                    Path(arguments[3]).write_bytes(source.read_bytes())
                return subprocess.CompletedProcess(arguments,1 if kwargs.get('check') is False else 0)
            cli.call.side_effect=call
            deploy.upload_immutable(cli,source,'dbfs:/Volumes/c/s/v/events.jsonl')
            self.assertTrue(all('--overwrite' not in args[0][0] for args in cli.call.call_args_list))

    def test_changed_existing_input_blocks_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            source=Path(directory)/'events.jsonl';source.write_bytes(b'reference\n')
            cli=Mock()
            def call(arguments,**kwargs):
                if arguments[:3]==['fs','cp','dbfs:/Volumes/c/s/v/events.jsonl']:
                    Path(arguments[3]).write_bytes(b'other\n')
                return subprocess.CompletedProcess(arguments,0)
            cli.call.side_effect=call
            with self.assertRaisesRegex(RuntimeError,'differs'):
                deploy.upload_immutable(cli,source,'dbfs:/Volumes/c/s/v/events.jsonl')

    def test_remote_read_failure_is_not_treated_as_existing_success(self):
        with tempfile.TemporaryDirectory() as directory:
            source=Path(directory)/'events.jsonl';source.write_bytes(b'reference\n')
            cli=Mock();cli.call.side_effect=[None,None,RuntimeError('permission denied')]
            with self.assertRaisesRegex(RuntimeError,'permission denied'):
                deploy.upload_immutable(cli,source,'dbfs:/Volumes/c/s/v/events.jsonl')


if __name__=='__main__':
    unittest.main()
