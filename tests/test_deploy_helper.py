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


class ResumeAndEvidence(unittest.TestCase):
    def test_run_url_parser_requires_real_unique_job_and_run_ids(self):
        result=deploy.parse_run_url('Run URL: https://test.cloud.databricks.com/jobs/456/runs/123?o=1\n')
        self.assertEqual((result['job_id'],result['run_id']),('456','123'))
        for value in ('RUNNING','Run URL: https://example.com/no-id',
                      'Run URL: https://test/jobs/1/runs/2\nRun URL: https://test/jobs/1/runs/3'):
            with self.assertRaises(ValueError):
                deploy.parse_run_url(value)

    def test_log_parser_keeps_repeated_pipeline_runs_distinct(self):
        import json
        command=['bundle','run','telemetry_pipeline']
        text=''.join('COMMAND '+json.dumps(command)+'\nRun URL: https://test/jobs/456/runs/'+run+'\n' for run in ('1','2'))
        self.assertEqual([item['run_id'] for item in deploy.logged_runs(text)],['1','2'])

    def test_resume_rejects_changed_pipeline_source(self):
        from unittest.mock import patch
        hashes={'events':'same'}
        previous=dict(catalog='workspace',namespace='demo',profile='TELEMETRY',input_sha256=hashes,
                      source_dirty=False,source_commit='a'*40)
        with patch.object(deploy.subprocess,'check_output',return_value='changed source'):
            with self.assertRaisesRegex(ValueError,'Pipeline source changed'):
                deploy.validate_resume_source(previous,'workspace','demo','TELEMETRY',hashes)
        previous['source_dirty']=True
        with self.assertRaises(ValueError):
            deploy.validate_resume_source(previous,'workspace','demo','TELEMETRY',hashes)

    def test_verified_parent_identity_rejects_wrong_job(self):
        execution=dict(run_id='123',job_id='456',run_url='https://test/jobs/456/runs/123')
        with self.assertRaises(ValueError):
            deploy.check_parent_identity(dict(run_id=123,job_id=999),execution)
        deploy.check_parent_identity(dict(run_id=123,job_id=456),execution)

    def test_acceptance_download_requires_report_and_job_verification(self):
        from tests.test_cloud_reports import report,job
        import json
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)
            cli=Mock()
            run={**job(), 'run_page_url':'https://test/jobs/456/runs/123'}
            def call(arguments,**kwargs):
                if arguments[:2]==['jobs','get-run']:
                    return run
                if arguments[:2]==['fs','cp']:
                    Path(arguments[3]).write_text(json.dumps(report(False,False)))
                    return subprocess.CompletedProcess(arguments,0)
                raise AssertionError(arguments)
            cli.call.side_effect=call
            execution=dict(run_id='123',job_id='456',run_url='https://test/jobs/456/runs/123')
            result=deploy.collect_acceptance(cli,execution,'workspace','demo',output,
                extended=False,failures=False,expected_version='0.1.2')
            self.assertEqual(result['status'],'succeeded')
            self.assertEqual(result['reference_counts']['ready_events'],100)
            run['tasks'][-1]['state']['result_state']='FAILED'
            with self.assertRaises(ValueError):
                deploy.collect_acceptance(cli,execution,'workspace','demo',output,
                    extended=False,failures=False,expected_version='0.1.2')


class ResumeExecution(unittest.TestCase):
    def test_resume_skips_baseline_runs_but_redeploys_and_runs_extended(self):
        import json,shutil
        from unittest.mock import patch
        actual_root=deploy.ROOT
        plan=deploy.deployment_steps('workspace','security_telemetry_demo',True)
        reused=[dict(command=command,run_id=str(i),status='verified_prior_success')
                for i,command in enumerate(plan[2:5])]
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            shutil.copytree(actual_root/'data',root/'data')
            shutil.copy(actual_root/'pyproject.toml',root/'pyproject.toml')
            (root/'.tools/deploy-venv/bin').mkdir(parents=True)
            (root/'.tools/deploy-venv/bin/python').touch()
            (root/'validation').mkdir()
            (root/'validation/previous.json').write_text('{}')
            cli=Mock();cli.env={}
            def call(arguments,**kwargs):
                if arguments==['current-user','me']:return {'id':'user'}
                if arguments==['auth','profiles']:return {'profiles':[dict(name='TELEMETRY',host='https://test')]}
                if arguments==['catalogs','list']:return [{'name':'workspace'}]
                if arguments[:2]==['schemas','list']:return [{'name':'security_telemetry_demo'}]
                if arguments[:2]==['volumes','list']:return [dict(name='pipeline_files',volume_type='MANAGED')]
                raise AssertionError(arguments)
            cli.call.side_effect=call
            cli.stream.return_value=dict(run_id='123',run_url='https://test/jobs/456/runs/123')
            def git_output(arguments,**kwargs):
                return 'a'*40 if arguments[1]=='rev-parse' else ''
            with patch.object(deploy,'ROOT',root), patch.object(deploy,'CLI',return_value=cli), \
                 patch.object(deploy.shutil,'which',return_value='/bin/tool'), \
                 patch.object(deploy.subprocess,'run'), \
                 patch.object(deploy.subprocess,'check_output',side_effect=git_output), \
                 patch.object(deploy,'upload_immutable',return_value='sha'), \
                 patch.object(deploy,'collect_resume',return_value=reused), \
                 patch.object(deploy,'collect_acceptance',return_value=dict(status='succeeded',check_count=21)):
                with patch('builtins.print'):
                    deploy.main(['--extended','--resume-from','validation/previous.json'])
            self.assertEqual([call.args[0] for call in cli.stream.call_args_list],plan[:2]+plan[5:])
            summaries=list((root/'validation').glob('databricks-deploy-*.json'))
            result=json.loads(summaries[0].read_text())
            self.assertEqual(result['status'],'cloud_checks_verified')
            self.assertEqual(len(result['reused_runs']),3)


if __name__=='__main__':
    unittest.main()
