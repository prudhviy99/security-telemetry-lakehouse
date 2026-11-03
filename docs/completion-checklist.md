# Completion checklist

Use the existing [completion PR](https://github.com/prudhviy99/security-telemetry-lakehouse/pull/1)
on `codex/complete-lakehouse-validation`. The repository's original `main` branch
is an older baseline while this PR remains open.

| Guide step | Status | Evidence or remaining action |
| --- | --- | --- |
| 0. Establish a baseline | Passed | Original commit `e4ba924`; completion implementation `e5b758b`. |
| 1. Validation and Docker | Passed locally/in CI | Boundary regression tests, 15 Spark tests, image build and container smoke. |
| 2. Recovery and integration CI | Passed locally/in CI | Persistent checkpoint registry, unsafe-reset rejection, isolated rebuild test, three successful CI jobs. |
| 3. Databricks account and terminal auth | Partly complete | Free Edition browser session is signed in and CLI/profile are installed. The agent cannot retrieve the Keychain credential or resolve the workspace host; verify/run the deployment helper from a normal terminal. |
| 4. Databricks adapter | Real run exposed a compatibility bug; fixed, retry pending | `REFRESH TABLE` now runs only locally; both job environments select 0.1.1. Three write-stage regressions pass. |
| 5. Deploy and run reference job | Deployment passed; first run failed in silver | Both jobs deployed. Run 837260759008916 hit unsupported serverless refresh after the dimension write; gold/validation were skipped. Retry 0.1.1 with the same checkpoints. |
| 6. Cloud acceptance and recovery | Pending | Run `telemetry_acceptance`, then its opt-in extended and failure-injection checks. Save actual reports. |
| 7. Publish evidence | Local evidence complete; cloud evidence pending | Existing draft PR and successful CI are available. Add real workspace run URLs/counts and reports only after execution. |

The observed console rejects automated browser control. The supported next step
is the OAuth/CLI route in [databricks.md](databricks.md), or the [normal-terminal deployment helper](../scripts/deploy_databricks.py) once that terminal is authenticated. A browser session,
passing local tests, and a static bundle check each establish different facts;
none proves a completed Databricks job.

Do not delete checkpoints to resolve startup errors. Follow the isolated rebuild
procedure in [recovery.md](recovery.md). A second-principal permission test remains
untested unless the workspace can supply a genuine reviewer identity.
