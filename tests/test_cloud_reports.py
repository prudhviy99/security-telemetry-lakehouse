"""Accept measured recovery evidence while rejecting unrelated failed jobs."""
import copy
import unittest
from telemetry.cloud_reports import (BASE_CHECKS,EXTENDED_CHECKS,FAULT_SINKS,
    REFERENCE_COUNTS,GOLD_TABLES,validate_report,validate_job_result)


def report(extended=True,failures=True):
    names=BASE_CHECKS | (EXTENDED_CHECKS if extended else set())
    if failures:
        names=names | {'recovery_after_'+sink for sink in FAULT_SINKS}
    rows=[dict(name=name,status='passed',observed={}) for name in sorted(names)]
    for row in rows:
        if row['name']=='reference_fixture_counts_and_gold':
            row['observed']=dict(counts=dict(REFERENCE_COUNTS),gold_sums={name:100 for name in GOLD_TABLES})
        if row['name'].startswith('recovery_after_'):
            row['observed']={'matches_uninterrupted':True}
    return dict(job_run_id='123',status='passed',package_version='0.1.2',checks=rows,
                include_extended_checks=extended,include_failure_injection=failures,not_run=['second_principal_permissions'])


def job(failed=False,verified=True):
    return dict(run_id=123,job_id=456,state=dict(life_cycle_state='TERMINATED',
        result_state='SUCCESS_WITH_FAILURES' if failed else 'SUCCESS'),tasks=[
        dict(task_key='cloud_acceptance',run_id=321,state=dict(result_state='FAILED' if failed else 'SUCCESS')),
        dict(task_key='verify_report',run_id=654,state=dict(result_state='SUCCESS' if verified else 'FAILED'))])


def planned_errors():
    return dict(error='ERROR: Some streams terminated before this command could finish!',
                error_trace='\n'.join('InjectedSinkFailure: injected failure after sink '+sink for sink in FAULT_SINKS))


class AcceptanceEvidence(unittest.TestCase):
    def test_complete_recovery_report_can_be_verified_after_expected_task_failure(self):
        result=validate_report(report(),'123',extended=True,failures=True,upstream_state='failed')
        self.assertEqual(result['check_count'],21)
        self.assertIn('second_principal_permissions',result['not_run'])
        self.assertEqual(validate_job_result(job(True),'123',failures=True,error_output=planned_errors()),
                         'succeeded_with_expected_stream_failures')

    def test_basic_acceptance_requires_successful_task(self):
        self.assertEqual(validate_report(report(False,False),'123',extended=False,failures=False,
                                         upstream_state='success')['check_count'],10)
        with self.assertRaises(ValueError):
            validate_report(report(False,False),'123',extended=False,failures=False,upstream_state='failed')

    def test_failed_partial_and_wrong_run_reports_are_rejected(self):
        for change in ({'status':'failed'},{'status':'running'},{'job_run_id':'999'}):
            value={**report(),**change}
            with self.assertRaises(ValueError):
                validate_report(value,'123',extended=True,failures=True)
        value=report();value['checks'].pop()
        with self.assertRaises(ValueError):
            validate_report(value,'123',extended=True,failures=True)

    def test_missing_recovery_and_duplicate_checks_are_rejected(self):
        value=report();value['checks']=[row for row in value['checks'] if row['name']!='recovery_after_audit']
        with self.assertRaises(ValueError):
            validate_report(value,'123',extended=True,failures=True)
        value=report();value['checks'].append(copy.deepcopy(value['checks'][0]))
        with self.assertRaises(ValueError):
            validate_report(value,'123',extended=True,failures=True)

    def test_bad_reference_and_gold_totals_are_rejected(self):
        for key in ('counts','gold_sums'):
            value=report();reference=next(row for row in value['checks'] if row['name']=='reference_fixture_counts_and_gold')
            field='ready_events' if key=='counts' else 'gold_device_day'
            reference['observed'][key][field]=99
            with self.assertRaises(ValueError):
                validate_report(value,'123',extended=True,failures=True)

    def test_unmatched_recovery_does_not_pass(self):
        value=report()
        next(row for row in value['checks'] if row['name']=='recovery_after_audit')['observed']['matches_uninterrupted']=False
        with self.assertRaises(ValueError):
            validate_report(value,'123',extended=True,failures=True)

    def test_failed_verifier_and_unrelated_error_are_rejected(self):
        with self.assertRaises(ValueError):
            validate_job_result(job(True,False),'123',failures=True,error_output=planned_errors())
        with self.assertRaises(ValueError):
            validate_job_result(job(True),'123',failures=True,error_output=dict(error='Permission denied',error_trace=''))
        errors=planned_errors();errors['error_trace']+='\nAcceptance namespace already exists'
        with self.assertRaises(ValueError):
            validate_job_result(job(True),'123',failures=True,error_output=errors)

    def test_incomplete_fault_trace_and_wrong_job_id_are_rejected(self):
        errors=planned_errors();errors['error_trace']=errors['error_trace'].splitlines()[0]
        with self.assertRaises(ValueError):
            validate_job_result(job(True),'123',failures=True,error_output=errors)
        with self.assertRaises(ValueError):
            validate_job_result(job(),'999',failures=False)
        self.assertEqual(validate_job_result(job(),'123',failures=False),'succeeded')


if __name__=='__main__':
    unittest.main()
