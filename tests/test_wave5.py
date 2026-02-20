"""Wave 5 tests: verify-system CLI command."""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from orchestrator.cli import cli


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _proc(returncode=0, stdout="", stderr=""):
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


# ===========================================================================
# TestVerifySystemCommand
# ===========================================================================

class TestVerifySystemCommand:
    def test_all_pass(self):
        """All 6 subprocess calls return exit 0 → 'OK: system verified', exit 0."""
        with patch("orchestrator.cli.subprocess.run") as mock_run:
            mock_run.side_effect = [
                _proc(),  # world-engine verify
                _proc(),  # media verify
                _proc(),  # video verify
                _proc(),  # orchestrator run
                _proc(),  # orchestrator validate-run
                _proc(),  # orchestrator diff
            ]
            result = CliRunner(mix_stderr=False).invoke(cli, ["verify-system"])
        assert result.exit_code == 0, result.output
        assert "OK: system verified" in result.output

    def test_external_tool_fail(self):
        """world-engine verify returns exit 1 → 'FAIL: world-engine verify' + error text, exit nonzero."""
        error_text = "world-engine: health check failed\n"
        with patch("orchestrator.cli.subprocess.run") as mock_run:
            mock_run.side_effect = [
                _proc(returncode=1, stderr=error_text),  # world-engine verify
                _proc(),  # media verify
                _proc(),  # video verify
                _proc(),  # orchestrator run
                _proc(),  # orchestrator validate-run
                _proc(),  # orchestrator diff
            ]
            result = CliRunner(mix_stderr=False).invoke(cli, ["verify-system"])
        assert result.exit_code != 0
        assert "FAIL: world-engine verify" in result.output
        assert "health check failed" in result.output

    def test_command_not_found(self):
        """subprocess.run raises FileNotFoundError → step is silently skipped (not a failure)."""
        with patch("orchestrator.cli.subprocess.run") as mock_run:
            mock_run.side_effect = [
                FileNotFoundError("world-engine not found"),  # world-engine verify → skip
                _proc(),  # media verify
                _proc(),  # video verify
                _proc(),  # orchestrator run
                _proc(),  # orchestrator validate-run
                _proc(),  # orchestrator diff
            ]
            result = CliRunner(mix_stderr=False).invoke(cli, ["verify-system"])
        assert result.exit_code == 0
        assert "OK: system verified" in result.output

    def test_pipeline_fail_skips_validate_diff(self):
        """Steps 1–3 pass, step 4 returns exit 1 → only 4 subprocess calls made."""
        error_text = "pipeline failed\n"
        with patch("orchestrator.cli.subprocess.run") as mock_run:
            mock_run.side_effect = [
                _proc(),  # world-engine verify
                _proc(),  # media verify
                _proc(),  # video verify
                _proc(returncode=1, stderr=error_text),  # orchestrator run
            ]
            result = CliRunner(mix_stderr=False).invoke(cli, ["verify-system"])
        assert result.exit_code != 0
        assert "FAIL: orchestrator run" in result.output
        assert mock_run.call_count == 4

    def test_validate_fail_shown(self):
        """Steps 1–4 pass, step 5 returns exit 1 → 'FAIL: orchestrator validate-run' + error text, exit nonzero."""
        error_text = "validate error details\n"
        with patch("orchestrator.cli.subprocess.run") as mock_run:
            mock_run.side_effect = [
                _proc(),  # world-engine verify
                _proc(),  # media verify
                _proc(),  # video verify
                _proc(),  # orchestrator run
                _proc(returncode=1, stderr=error_text),  # orchestrator validate-run
                _proc(),  # orchestrator diff
            ]
            result = CliRunner(mix_stderr=False).invoke(cli, ["verify-system"])
        assert result.exit_code != 0
        assert "FAIL: orchestrator validate-run" in result.output
        assert "validate error details" in result.output

    def test_output_deterministic(self):
        """Two invocations with identical mock results → result1.output == result2.output and both exit 0."""
        def make_side_effects():
            return [_proc(), _proc(), _proc(), _proc(), _proc(), _proc()]

        runner = CliRunner(mix_stderr=False)

        with patch("orchestrator.cli.subprocess.run") as mock_run:
            mock_run.side_effect = make_side_effects()
            result1 = runner.invoke(cli, ["verify-system"])

        with patch("orchestrator.cli.subprocess.run") as mock_run:
            mock_run.side_effect = make_side_effects()
            result2 = runner.invoke(cli, ["verify-system"])

        assert result1.exit_code == 0
        assert result2.exit_code == 0
        assert result1.output == result2.output
