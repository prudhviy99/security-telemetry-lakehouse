#!/usr/bin/env python3
"""Run the supported CLI deployment from an authenticated, networked terminal.

Uses the existing TELEMETRY profile without exporting tokens or changing token
storage. Only synthetic reference fixtures and dedicated demo resources are used.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from urllib.parse import urlsplit
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def identifier(value):
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f'Use a simple catalog/schema identifier, got {value!r}')
    return value


def records(payload, key):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get(key), list):
        return payload[key]
    raise ValueError(f'Unexpected CLI JSON shape for {key}')


def choose_catalog(payload, requested=None):
    names = {row['name'] for row in records(payload, 'catalogs')}
    if requested:
        identifier(requested)
        if requested not in names:
            raise ValueError(f'Catalog {requested!r} was not returned by this workspace')
        return requested
    usable = names - {'system', 'samples', 'hive_metastore', '__databricks_internal'}
    if 'workspace' in usable:
        return 'workspace'
    if len(usable) == 1:
        return identifier(next(iter(usable)))
    raise ValueError('Choose an actual catalog with --catalog NAME. Available: '+', '.join(sorted(usable)))


class CLI:
    def __init__(self, executable, profile, log_path, env=None):
        self.executable, self.profile = executable, profile
        self.log_path, self.env = log_path, env or dict(os.environ)

    def command(self, arguments):
        return [self.executable, *arguments, '--profile', self.profile]

    def call(self, arguments, *, json_result=False, check=True):
        command = self.command(arguments + (['--output', 'json'] if json_result else []))
        # Authentication material is never requested as output or passed in arguments.
        print('Running: databricks '+' '.join(arguments), flush=True)
        result = subprocess.run(command, cwd=ROOT, env=self.env, text=True, capture_output=True)
        if check and result.returncode:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or 'CLI failed')
        if json_result:
            if result.returncode:
                raise RuntimeError(result.stderr.strip() or 'CLI failed')
            return json.loads(result.stdout)
        return result

    def stream(self, arguments, *, check=True):
        command = self.command(arguments)
        print('Running: databricks '+' '.join(arguments), flush=True)
        with self.log_path.open('a', encoding='utf-8') as log:
            log.write('\nCOMMAND '+json.dumps(arguments)+'\n')
            log.flush()
            process = subprocess.Popen(command, cwd=ROOT, env=self.env, text=True,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = []
            try:
                for line in process.stdout:
                    output.append(line)
                    print(line, end='', flush=True)
                    log.write(line)
                    log.flush()
                code = process.wait()
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait()
            if code and check:
                raise RuntimeError(f'CLI command failed ({code}); see {self.log_path}')
        result = dict(returncode=code, command=arguments, output=''.join(output))
        if arguments[:2] == ['bundle','run']:
            result.update(parse_run_url(result['output']))
        return result


def upload_immutable(cli, local, remote):
    cli.call(['fs', 'mkdirs', remote.rsplit('/', 1)[0]])
    # Existing files are never overwritten. A failed upload may mean an exact
    # copy is already present, but permission/network errors still fail on read.
    cli.call(['fs', 'cp', str(local), remote], check=False)
    with tempfile.TemporaryDirectory(prefix='telemetry-check-') as directory:
        downloaded = Path(directory)/local.name
        cli.call(['fs', 'cp', remote, str(downloaded)])
        if downloaded.read_bytes() != local.read_bytes():
            raise RuntimeError(f'Existing immutable input differs: {remote}. Use a fresh isolated schema; do not overwrite it.')
    return hashlib.sha256(local.read_bytes()).hexdigest()


def deployment_steps(catalog, namespace, extended=False):
    common = ['-t', 'dev', '--var', f'catalog={catalog},namespace={namespace}']
    steps = [
        ['bundle', 'validate', *common],
        ['bundle', 'deploy', *common],
        ['bundle', 'run', *common, 'telemetry_pipeline'],
        ['bundle', 'run', *common, 'telemetry_pipeline'],
        ['bundle', 'run', *common, 'telemetry_acceptance'],
    ]
    if extended:
        steps.append(['bundle', 'run', *common, 'telemetry_acceptance', '--params',
                      'include_extended_checks=true,include_failure_injection=true'])
    return steps


def parse_run_url(text):
    matches=re.findall(r'^Run URL: (https://[^\s]+)',text,re.MULTILINE)
    if len(matches)!=1:
        raise ValueError('Expected one actual CLI run URL; refusing to guess a run ID')
    url=matches[0]
    parsed=urlsplit(url)
    address=parsed.path+'/'+parsed.fragment
    job=re.search(r'/(?:jobs|job)/(\d+)',address)
    run=re.search(r'/(?:runs|run)/(\d+)',address)
    if not job or not run:
        raise ValueError('Unrecognized actual CLI run URL: '+url)
    return dict(run_id=run.group(1),job_id=job.group(1),run_url=url)


def logged_runs(text):
    result=[]
    command=None
    for line in text.splitlines():
        if line.startswith('COMMAND '):
            command=json.loads(line[len('COMMAND '):])
        elif line.startswith('Run URL: ') and command:
            result.append(dict(command=command,**parse_run_url(line)))
    return result


def check_parent_identity(run, execution):
    if str(run.get('run_id'))!=execution['run_id'] or str(run.get('job_id'))!=execution['job_id']:
        raise ValueError('Jobs API response does not match the recorded CLI run/job IDs')
    if run.get('run_page_url') and urlsplit(run['run_page_url']).netloc!=urlsplit(execution['run_url']).netloc:
        raise ValueError('Jobs API returned a different workspace')


def collect_acceptance(cli, execution, catalog, namespace, output, *, extended, failures,
                       expected_version, require_verifier=True):
    from telemetry.cloud_reports import validate_report, validate_job_result
    run_id=execution['run_id']
    run=cli.call(['jobs','get-run',run_id],json_result=True)
    check_parent_identity(run,execution)
    (output/f'cloud-job-{run_id}.json').write_text(json.dumps(run,indent=2)+'\n')
    report_path=output/f'cloud-acceptance-{run_id}.json'
    remote=f'dbfs:/Volumes/{catalog}/{namespace}/pipeline_files/acceptance/{run_id}/result.json'
    cli.call(['fs','cp',remote,str(report_path),'--overwrite'])
    report=json.loads(report_path.read_text())
    if report.get('package_version')!=expected_version:
        raise ValueError('Acceptance report package version differs from the deployed source')
    facts=validate_report(report,run_id,extended=extended,failures=failures)
    producer=next((task for task in run.get('tasks',[]) if task.get('task_key')=='cloud_acceptance'),{})
    error_output=None
    if producer.get('state',{}).get('result_state')=='FAILED':
        error_output=cli.call(['jobs','get-run-output',str(producer['run_id'])],json_result=True)
        (output/f'cloud-task-error-{run_id}.json').write_text(json.dumps(error_output,indent=2)+'\n')
    status=validate_job_result(run,run_id,failures=failures,error_output=error_output,
                               require_verifier=require_verifier)
    return {**facts,'job_status':run['state']['result_state'],'status':status,
            'report_path':str(report_path),'run_url':execution['run_url']}


def comparable_pipeline_source(text):
    return re.sub(r'security_telemetry_lakehouse-[0-9.]+-py3-none-any\.whl',
                  'security_telemetry_lakehouse-VERSION-py3-none-any.whl',text)


def validate_resume_source(previous, catalog, namespace, profile, hashes):
    if (previous.get('catalog'),previous.get('namespace'),previous.get('profile')) != (catalog,namespace,profile):
        raise ValueError('Resume summary catalog/schema/profile differs from this deployment')
    if previous.get('input_sha256')!=hashes or previous.get('source_dirty') is not False:
        raise ValueError('Resume requires the same immutable inputs and a clean prior source commit')
    commit=previous.get('source_commit','')
    if not re.fullmatch(r'[0-9a-f]{40}',commit):
        raise ValueError('Resume summary lacks a precise source commit')
    files=[str(path.relative_to(ROOT)) for path in (ROOT/'src/telemetry').glob('*.py')
           if path.name not in {'cloud_acceptance.py','cloud_reports.py'}]
    files += ['notebooks/01_bronze.py','notebooks/02_silver.py','notebooks/03_gold_validate.py',
              'resources/telemetry.job.yml','databricks.yml']
    for name in files:
        old=subprocess.check_output(['git','show',commit+':'+name],cwd=ROOT,text=True)
        if comparable_pipeline_source(old)!=comparable_pipeline_source((ROOT/name).read_text()):
            raise ValueError('Pipeline source changed since the previous run: '+name)
    old_project=subprocess.check_output(['git','show',commit+':pyproject.toml'],cwd=ROOT,text=True)
    return re.search(r'^version = "([^"]+)"',old_project,re.MULTILINE).group(1)


def collect_resume(cli, previous, catalog, namespace, profile, hashes, output, workspace_host):
    old_version=validate_resume_source(previous,catalog,namespace,profile,hashes)
    path=Path(previous['log_path']).resolve()
    if not path.is_relative_to((ROOT/'validation').resolve()):
        raise ValueError('Resume log must be inside this checkout validation directory')
    entries=logged_runs(path.read_text(errors='replace'))
    eligible=[step['command'] for step in previous.get('steps',[])
              if step.get('status')=='succeeded' and step['command'][:2]==['bundle','run']]
    reused=[]
    for command in eligible:
        match=next((entry for entry in entries if entry['command']==command),None)
        if match is None:
            raise ValueError('Successful prior step has no recorded CLI run URL')
        entries.remove(match)
        if urlsplit(match['run_url']).netloc!=urlsplit(workspace_host).netloc:
            raise ValueError('Resume run URL points at a different workspace')
        if 'telemetry_pipeline' in command:
            run=cli.call(['jobs','get-run',match['run_id']],json_result=True)
            check_parent_identity(run,match)
            expected={'bronze','silver','gold','validate_publish'}
            tasks=run.get('tasks',[])
            if (run.get('state',{}).get('result_state')!='SUCCESS'
                    or {task['task_key'] for task in tasks}!=expected
                    or any(task.get('state',{}).get('result_state')!='SUCCESS' for task in tasks)):
                raise ValueError('A previously successful pipeline run cannot be verified')
            record=dict(command=command,run_id=match['run_id'],run_url=match['run_url'],status='verified_prior_success')
            (output/f'cloud-job-{match["run_id"]}.json').write_text(json.dumps(run,indent=2)+'\n')
        elif 'telemetry_acceptance' in command and '--params' not in command:
            record=dict(command=command,**collect_acceptance(cli,match,catalog,namespace,output,
                extended=False,failures=False,expected_version=old_version,require_verifier=False))
            record['status']='verified_prior_success'
        else:
            continue
        reused.append(record)
    if sum('telemetry_pipeline' in item['command'] for item in reused)!=2 or not any('telemetry_acceptance' in item['command'] for item in reused):
        raise ValueError('Resume requires two verified pipeline runs and a complete basic acceptance report')
    return reused


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', default='TELEMETRY')
    parser.add_argument('--catalog', help='An existing catalog; otherwise select workspace or the sole non-system catalog')
    parser.add_argument('--namespace', default='security_telemetry_demo')
    parser.add_argument('--resume-from',type=Path,help='Verify and reuse completed baseline stages from a prior validation summary')
    parser.add_argument('--extended', action='store_true', help='Also run the larger cloud/recovery suite; subject to Free Edition quota')
    args = parser.parse_args(argv)
    identifier(args.namespace)
    executable = shutil.which('databricks')
    if not executable:
        parser.error('Install the official CLI: brew install databricks/tap/databricks')
    builder = shutil.which('python3.12')
    if builder is None and sys.version_info >= (3,11):
        builder = sys.executable
    if builder is None:
        parser.error('A Python 3.11+ wheel builder is required. On macOS: brew install python@3.12')

    sys.path.insert(0, str(ROOT/'src'))
    from telemetry.contracts import reconcile_lines, validate_customer_snapshot
    result = reconcile_lines((ROOT/'data/events.jsonl').read_text().splitlines())
    customers, errors = validate_customer_snapshot((ROOT/'data/customers.jsonl').read_text().splitlines())
    expected = (105,102,3,101,100)
    if tuple(result[k] for k in ('raw_rows','valid_rows','invalid_rows','ledger_versions','ready_events')) != expected or errors or len(customers) != 12:
        raise RuntimeError('Reference fixtures changed. Verify them before deployment.')

    output = ROOT/'validation'
    output.mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    log_path = output/f'databricks-deploy-{timestamp}.log'
    cli = CLI(executable, args.profile, log_path)
    # Fail before resource creation if authentication or network access is absent.
    cli.call(['current-user','me'], json_result=True)
    profiles=records(cli.call(['auth','profiles'],json_result=True),'profiles')
    profile=next((item for item in profiles if item['name']==args.profile),None)
    if profile is None or not profile.get('host'):
        raise ValueError('The selected profile has no workspace host')
    workspace_host=profile['host']
    catalog = choose_catalog(cli.call(['catalogs','list'], json_result=True), args.catalog)
    print(f'Using catalog={catalog}, namespace={args.namespace}', flush=True)

    # A small build-only venv avoids installing local Spark on the deployment host.
    venv = ROOT/'.tools/deploy-venv'
    if not (venv/'bin/python').exists():
        subprocess.run([builder,'-m','venv',str(venv)],check=True,cwd=ROOT)
    cli.env['PATH'] = str(venv/'bin')+os.pathsep+os.environ.get('PATH','')
    # Build before creating workspace resources; bundle deploy builds again from the same source.
    subprocess.run([str(venv/'bin/python'),'-m','pip','wheel','--no-deps','--wheel-dir','dist','.'],
                   check=True,cwd=ROOT,env=cli.env)

    schemas = records(cli.call(['schemas','list',catalog], json_result=True),'schemas')
    if args.namespace not in {row['name'] for row in schemas}:
        cli.call(['schemas','create',args.namespace,catalog])
    volumes = records(cli.call(['volumes','list',catalog,args.namespace], json_result=True),'volumes')
    existing = next((row for row in volumes if row['name']=='pipeline_files'),None)
    if existing and existing.get('volume_type') != 'MANAGED':
        raise RuntimeError('pipeline_files already exists as an external volume; choose a dedicated namespace')
    if not existing:
        cli.call(['volumes','create',catalog,args.namespace,'pipeline_files','MANAGED'])
    root = f'dbfs:/Volumes/{catalog}/{args.namespace}/pipeline_files/reference'
    hashes = {
        'events':upload_immutable(cli,ROOT/'data/events.jsonl',root+'/events/events.jsonl'),
        'customers':upload_immutable(cli,ROOT/'data/customers.jsonl',root+'/customers.jsonl'),
    }
    expected_version=re.search(r'^version = "([^"]+)"',(ROOT/'pyproject.toml').read_text(),re.MULTILINE).group(1)
    summary = dict(catalog=catalog,namespace=args.namespace,profile=args.profile,workspace_host=workspace_host,
                   source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                   source_dirty=bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True).strip()),
                   input_sha256=hashes,extended=args.extended,log_path=str(log_path),status='running',
                   steps=[],reused_runs=[],cloud_reports=[])
    summary_path = output/f'databricks-deploy-{timestamp}.json'
    def save_summary():
        summary_path.write_text(json.dumps(summary,indent=2)+'\n')
    try:
        if args.resume_from:
            previous_path=args.resume_from if args.resume_from.is_absolute() else ROOT/args.resume_from
            previous=json.loads(previous_path.read_text())
            summary['reused_runs']=collect_resume(cli,previous,catalog,args.namespace,args.profile,hashes,output,workspace_host)
            summary['resumed_from']=str(previous_path)
            save_summary()
        reused=Counter(tuple(item['command']) for item in summary['reused_runs'])
        for arguments in deployment_steps(catalog,args.namespace,args.extended):
            key=tuple(arguments)
            if reused[key]:
                reused[key]-=1
                print('Reusing verified prior run: '+' '.join(arguments),flush=True)
                continue
            acceptance='telemetry_acceptance' in arguments
            execution=cli.stream(arguments,check=not acceptance)
            record=dict(command=arguments,status='succeeded')
            if arguments[:2]==['bundle','run']:
                record.update(run_id=execution['run_id'],run_url=execution['run_url'])
            if acceptance:
                extended='--params' in arguments
                verified=collect_acceptance(cli,execution,catalog,args.namespace,output,
                    extended=extended,failures=extended,expected_version=expected_version)
                record['status']=verified['status']
                summary['cloud_reports'].append(verified)
            summary['steps'].append(record)
            save_summary()
        summary['status']='cloud_checks_verified'
    except Exception as error:
        summary.update(status='failed',error=str(error))
        raise
    finally:
        save_summary()
    print(f'Cloud checks verified. Summary: {summary_path}\nRun URLs and output: {log_path}\n'
          'Measured acceptance reports are saved under validation/cloud-acceptance-RUN_ID.json.')


if __name__=='__main__':
    try:
        main()
    except (RuntimeError,ValueError,subprocess.CalledProcessError) as error:
        print('Deployment stopped: '+str(error),file=sys.stderr)
        raise SystemExit(1)
