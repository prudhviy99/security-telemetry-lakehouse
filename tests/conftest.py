"""Make a required integration run fail when Spark tests are absent or skipped.

The default lightweight invocation still permits the optional Spark module to
skip. CI and Docker opt into the stricter gate with --require-spark-tests.
"""

from __future__ import annotations

import os

import pytest


_required = False
_selected: set[str] = set()
_executed: set[str] = set()
_skipped: set[str] = set()


def pytest_addoption(parser):
    parser.addoption(
        "--require-spark-tests",
        action="store_true",
        default=False,
        help="Fail unless every selected Spark integration test executes without skipping.",
    )


def pytest_configure(config):
    global _required
    _required = config.getoption("--require-spark-tests") or os.environ.get(
        "TELEMETRY_REQUIRE_SPARK_TESTS", ""
    ).lower() in {"1", "true", "yes"}
    _selected.clear()
    _executed.clear()
    _skipped.clear()


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items):
    _selected.update(item.nodeid for item in items if item.get_closest_marker("spark"))


def pytest_runtest_logreport(report):
    if report.nodeid not in _selected:
        return
    if report.when == "call":
        _executed.add(report.nodeid)
    if report.skipped:
        _skipped.add(report.nodeid)


def pytest_sessionfinish(session, exitstatus):
    if not _required:
        return
    missing = _selected - _executed
    if _selected and not missing and not _skipped:
        return
    # Preserve collection/usage/internal errors; make otherwise successful or
    # empty runs fail when the required integration coverage never executed.
    if int(session.exitstatus) in {0, 5}:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_sep(
            "=",
            "required Spark tests were not fully executed: "
            f"selected={len(_selected)}, executed={len(_executed)}, skipped={len(_skipped)}. "
            "Install the [dev,spark] extras and Java, and select the Spark tests.",
            red=True,
        )
