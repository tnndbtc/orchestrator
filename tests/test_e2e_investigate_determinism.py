"""E2E test: investigate-determinism against the real examples/phase0/project.json.

Invokes the orchestrator binary as a real subprocess (no mocking, no CliRunner)
to verify the full investigate-determinism command works end-to-end with the
bundled example project.  Skipped automatically when the orchestrator binary is
not installed (e.g. bare CI without running option 1 first).
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Locate repo root and project file
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PROJECT_FILE = _REPO_ROOT / "examples" / "phase0" / "project.json"


def _find_orchestrator_bin() -> str | None:
    """Return path to the orchestrator binary, or None if not installed.

    Search order:
      1. Alongside the current Python executable (handles activated venvs and
         the .venv created by setup.sh option 1).
      2. PATH fallback via shutil.which.
    """
    candidate = Path(sys.executable).parent / "orchestrator"
    if candidate.is_file():
        return str(candidate)
    return shutil.which("orchestrator")


_BIN = _find_orchestrator_bin()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.skipif(_BIN is None, reason="orchestrator binary not installed; run setup.sh option 1 first")
class TestE2EInvestigateDeterminism:

    def test_pass_on_example_project(self, tmp_path):
        """investigate-determinism exits 0 and writes status=pass for examples/phase0."""
        out_dir = tmp_path / "det-out"

        result = subprocess.run(
            [
                _BIN,
                "investigate-determinism",
                "--project", str(_PROJECT_FILE),
                "--out", str(out_dir),
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, (
            f"investigate-determinism exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

        report_path = out_dir / "DeterminismReport.json"
        assert report_path.exists(), "DeterminismReport.json was not written"

        report = json.loads(report_path.read_text(encoding="utf-8"))
        assert report["status"] == "pass", (
            f"Expected status='pass', got {report['status']!r}\n"
            f"diffs:\n{json.dumps(report.get('diffs', []), indent=2)}"
        )
        assert report["diffs"] == [], (
            f"Expected empty diffs, got:\n{json.dumps(report['diffs'], indent=2)}"
        )

    def test_report_written_to_out_dir(self, tmp_path):
        """DeterminismReport.json is written inside --out, not the cwd."""
        out_dir = tmp_path / "nested" / "output"  # must be created by the command

        result = subprocess.run(
            [
                _BIN,
                "investigate-determinism",
                "--project", str(_PROJECT_FILE),
                "--out", str(out_dir),
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stdout + result.stderr
        assert (out_dir / "DeterminismReport.json").exists()
