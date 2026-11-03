# Completion checklist

Use the existing [completion PR](https://github.com/prudhviy99/security-telemetry-lakehouse/pull/1)
on `codex/complete-lakehouse-validation`. The repository's original `main` branch
is an older baseline while this PR remains open.

| Guide step | Status | Evidence or remaining action |
| --- | --- | --- |
| 0. Establish a baseline | Passed | Original commit `e4ba924`; completion implementation `e5b758b`. |
| 1. Validation and Docker | Passed locally/in CI | Boundary regression tests, 15 Spark tests, image build and container smoke. |
| 2. Recovery and integration CI | Passed locally/in CI | Persistent checkpoint registry, unsafe-reset rejection, isolated rebuild test, three successful CI jobs. |
| 3. Databricks account and terminal auth | Passed in the normal terminal | TELEMETRY successfully deployed and ran the jobs. The isolated agent process still lacks network/credential access. |
| 4. Databricks adapter | Passed for reference pipeline and rerun | Both actual 0.1.1 four-task runs succeeded after the refresh fix. |
| 5. Deploy and run reference job | Passed on 0.1.1 | Runs 673561808875428 and 629043682032596 succeeded. Preserve the same pipeline checkpoints. |
| 6. Cloud acceptance and recovery | Basic passed; extended retry pending | Basic run 363703603959324 succeeded. Extended run failed in fault-test orchestration. Version 0.1.2 adds independent verification and a tested resume path; its cloud execution is pending. |
| 7. Publish evidence | Local evidence complete; cloud evidence pending | Existing draft PR and successful CI are available. Add real workspace run URLs/counts and reports only after execution. |

The observed console rejects automated browser control. The supported next step
is the OAuth/CLI route in [databricks.md](databricks.md), or the [normal-terminal deployment helper](../scripts/deploy_databricks.py) once that terminal is authenticated. A browser session,
passing local tests, and a static bundle check each establish different facts;
none proves a completed Databricks job.

Do not delete checkpoints to resolve startup errors. Follow the isolated rebuild
procedure in [recovery.md](recovery.md). A second-principal permission test remains
untested unless the workspace can supply a genuine reviewer identity.
