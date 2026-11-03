"""Shared fixtures for the stdlib unittest suite (no third-party test deps)."""

import contextlib
import io
import os
import pathlib
import tempfile
import unittest


class TempCase(unittest.TestCase):
    """Gives each test a temp dir (self.tmp) and points the runs dir at it,
    so CLI runs never write into the repo's .doctor_runs."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self._tmp.name)
        self._old_runs = os.environ.get("DELIVERY_DOCTOR_RUNS_DIR")
        os.environ["DELIVERY_DOCTOR_RUNS_DIR"] = str(self.tmp / "runs")

    def tearDown(self):
        if self._old_runs is None:
            os.environ.pop("DELIVERY_DOCTOR_RUNS_DIR", None)
        else:
            os.environ["DELIVERY_DOCTOR_RUNS_DIR"] = self._old_runs
        self._tmp.cleanup()

    def run_cli(self, argv):
        """Run the CLI in-process; return (exit code, stdout, stderr)."""
        from delivery_doctor.__main__ import main
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = main(argv)
            except SystemExit as e:  # argparse usage errors
                code = e.code
        return code, out.getvalue(), err.getvalue()
