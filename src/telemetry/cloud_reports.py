"""Strict, dependency-free validation of measured cloud acceptance evidence."""
from __future__ import annotations
import re

FAULT_SINKS = ('classification','quarantine_invalid','ledger','current','quarantine_conflict','audit')
BASE_CHECKS = {
    'reference_fixture_counts_and_gold','auto_loader_baseline_and_quarantine',
    'unknown_customer_is_retained','same_checkpoint_replay','receipt_time_excluded_from_identity',
    'late_revision_moves_historical_day','separate_batch_conflict_is_excluded',
    'higher_version_resolves_conflict','duplicate_dimension_blocks_publication','failed_gold_does_not_publish',
}
EXTENDED_CHECKS = {'lower_after_higher','conflict_all_arrival_orders','storage_type_boundaries',
                   'additive_and_wrong_type_schema','isolated_rebuild'}
REFERENCE_COUNTS = dict(raw_rows=105,valid_rows=102,invalid_rows=3,duplicate_delivery_rows=1,
                        ledger_versions=101,superseded_versions=1,conflict_events=0,
                        ready_events=100,quarantine_rows=3)
GOLD_TABLES = {'gold_device_day','gold_product_version_day','gold_signup_cohort_day'}


def validate_report(report, run_id, *, extended, failures, upstream_state=None):
    """A failed task is acceptable only with complete measured recovery evidence.

    This does not change the upstream task's actual status. The caller must also
    inspect its Jobs API error trace before accepting an expected task failure.
    """
    if report.get('status') != 'passed' or str(report.get('job_run_id')) != str(run_id):
        raise ValueError('Missing, failed, unfinished, or wrong-run acceptance report')
    if report.get('include_extended_checks') is not extended or report.get('include_failure_injection') is not failures:
        raise ValueError('Acceptance report flags do not match the requested checks')
    if upstream_state is not None:
        state = upstream_state.lower()
        if state not in {'success','failed'} or (state == 'failed' and not failures):
            raise ValueError('Unexpected upstream acceptance task state: '+state)
    checks = report.get('checks',[])
    by_name = {item.get('name'):item for item in checks}
    if len(by_name) != len(checks):
        raise ValueError('Duplicate acceptance check names')
    required = BASE_CHECKS | (EXTENDED_CHECKS if extended else set())
    if failures:
        required = required | {'recovery_after_'+sink for sink in FAULT_SINKS}
    if set(by_name) != required or any(item.get('status')!='passed' for item in checks):
        raise ValueError('Acceptance checks are missing, unexpected, or unsuccessful')
    reference = by_name['reference_fixture_counts_and_gold'].get('observed',{})
    if reference.get('counts') != REFERENCE_COUNTS:
        raise ValueError('Measured reference counts do not reconcile')
    sums=reference.get('gold_sums',{})
    if set(sums) != GOLD_TABLES or any(value != 100 for value in sums.values()):
        raise ValueError('Measured gold totals do not reconcile')
    for sink in FAULT_SINKS if failures else ():
        if by_name['recovery_after_'+sink].get('observed',{}).get('matches_uninterrupted') is not True:
            raise ValueError('Recovery comparison did not pass for '+sink)
    return dict(job_run_id=str(run_id),status='verified',check_count=len(checks),
                package_version=report.get('package_version'),reference_counts=reference['counts'],
                gold_sums=sums,not_run=report.get('not_run',[]),
                upstream_task_state=upstream_state)


def validate_job_result(run, run_id, *, failures, error_output=None, require_verifier=True):
    if str(run.get('run_id')) != str(run_id):
        raise ValueError('Jobs API returned a different run')
    state=run.get('state',{})
    if state.get('life_cycle_state') != 'TERMINATED':
        raise ValueError('Acceptance job did not terminate normally')
    tasks={task['task_key']:task for task in run.get('tasks',[])}
    producer=tasks.get('cloud_acceptance',{})
    producer_state=producer.get('state',{}).get('result_state')
    if require_verifier and tasks.get('verify_report',{}).get('state',{}).get('result_state') != 'SUCCESS':
        raise ValueError('The independent report-verification task did not succeed')
    if producer_state == 'SUCCESS' and state.get('result_state') == 'SUCCESS':
        return 'succeeded'
    if (not failures or producer_state != 'FAILED'
            or state.get('result_state') not in {'SUCCESS_WITH_FAILURES','SUCCESS'}):
        raise ValueError('Unexpected acceptance job/task failure')
    output=error_output or {}
    message=output.get('error','')
    trace=message+'\n'+output.get('error_trace','')
    observed=set(re.findall(r'injected failure after sink ([a-z_]+)',trace))
    if (not ' '.join(message.split()).startswith('ERROR: Some streams terminated')
            or observed != set(FAULT_SINKS) or 'Acceptance namespace already exists' in trace):
        raise ValueError('Task failure is not the complete planned streaming-fault set')
    return 'succeeded_with_expected_stream_failures'
