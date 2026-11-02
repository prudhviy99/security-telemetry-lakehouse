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

    def stream(self, arguments):
        command = self.command(arguments)
        print('Running: databricks '+' '.join(arguments), flush=True)
        with self.log_path.open('a', encoding='utf-8') as log:
            log.write('\nCOMMAND '+json.dumps(arguments)+'\n')
            log.flush()
            process = subprocess.Popen(command, cwd=ROOT, env=self.env, text=True,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            try:
                for line in process.stdout:
                    print(line, end='', flush=True)
                    log.write(line)
                    log.flush()
                code = process.wait()
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait()
            if code:
                raise RuntimeError(f'CLI command failed ({code}); see {self.log_path}')


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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', default='TELEMETRY')
    parser.add_argument('--catalog', help='An existing catalog; otherwise select workspace or the sole non-system catalog')
    parser.add_argument('--namespace', default='security_telemetry_demo')
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
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    log_path = output/f'databricks-deploy-{timestamp}.log'
    cli = CLI(executable, args.profile, log_path)
    # Fail before resource creation if authentication or network access is absent.
    cli.call(['current-user','me'], json_result=True)
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
    summary = dict(catalog=catalog,namespace=args.namespace,profile=args.profile,
                   source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                   source_dirty=bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True).strip()),
                   input_sha256=hashes,extended=args.extended,log_path=str(log_path),status='running',steps=[])
    summary_path = output/f'databricks-deploy-{timestamp}.json'
    try:
        for arguments in deployment_steps(catalog,args.namespace,args.extended):
            cli.stream(arguments)
            summary['steps'].append(dict(command=arguments,status='succeeded'))
        summary['status']='cli_commands_succeeded'
    except Exception as error:
        summary.update(status='failed',error=str(error))
        raise
    finally:
        summary_path.write_text(json.dumps(summary,indent=2)+'\n')
    print(f'CLI execution finished. Summary: {summary_path}\nRun URLs and output: {log_path}\n'
          'Retrieve the measured acceptance JSON from the job output before publishing cloud counts.')


if __name__=='__main__':
    try:
        main()
    except (RuntimeError,ValueError,subprocess.CalledProcessError) as error:
        print('Deployment stopped: '+str(error),file=sys.stderr)
        raise SystemExit(1)
