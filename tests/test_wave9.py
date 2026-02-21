"""Wave 9 tests — orchestrator write command (thin CLI shim)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from orchestrator.cli import cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_agent(tmp_path: Path, *, fail: bool = False) -> str:
    """Write a tiny Python script to tmp_path and return the --writing-agent-cmd string."""
    script = tmp_path / "fake_writer.py"
    if fail:
        script.write_text("import sys; sys.exit(1)\n", encoding="utf-8")
    else:
        script.write_text(
            "import sys, json, pathlib, argparse\n"
            "p = argparse.ArgumentParser()\n"
            "p.add_argument('--prompt'); p.add_argument('--out')\n"
            "args = p.parse_args()\n"
            "data = {'schema_id': 'Script', 'schema_version': '1.0.0',\n"
            "        'script_id': 'script-test-00000001', 'title': 'Test'}\n"
            "pathlib.Path(args.out).write_text(json.dumps(data, indent=2), encoding='utf-8')\n",
            encoding="utf-8",
        )
    return f"{sys.executable} {script}"


def _make_prompt(tmp_path: Path) -> Path:
    """Write a minimal StoryPrompt.json and return its path."""
    prompt_path = tmp_path / "StoryPrompt.json"
    prompt_path.write_text(
        json.dumps({"schema_id": "StoryPrompt", "schema_version": "1.0.0", "title": "Test"}),
        encoding="utf-8",
    )
    return prompt_path


# ---------------------------------------------------------------------------
# T1 — success path
# ---------------------------------------------------------------------------

def test_write_success(tmp_path: Path) -> None:
    """orchestrator write exits 0 and produces the expected Script.json."""
    runner = CliRunner()
    prompt_path = _make_prompt(tmp_path)
    out_path = tmp_path / "Script.json"
    agent_cmd = _make_fake_agent(tmp_path)

    result = runner.invoke(
        cli,
        [
            "write",
            "--prompt", str(prompt_path),
            "--out", str(out_path),
            "--writing-agent-cmd", agent_cmd,
        ],
    )

    assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}. Output: {result.output}"
    assert out_path.exists(), "Script.json was not created"

    expected = {
        "schema_id": "Script",
        "schema_version": "1.0.0",
        "script_id": "script-test-00000001",
        "title": "Test",
    }
    actual = json.loads(out_path.read_text(encoding="utf-8"))
    assert actual == expected, f"Script.json content mismatch: {actual}"


# ---------------------------------------------------------------------------
# T2 — agent error
# ---------------------------------------------------------------------------

def test_write_agent_error(tmp_path: Path) -> None:
    """orchestrator write exits non-zero and prints ERROR when agent fails."""
    runner = CliRunner()
    prompt_path = _make_prompt(tmp_path)
    out_path = tmp_path / "Script.json"
    agent_cmd = _make_fake_agent(tmp_path, fail=True)

    result = runner.invoke(
        cli,
        [
            "write",
            "--prompt", str(prompt_path),
            "--out", str(out_path),
            "--writing-agent-cmd", agent_cmd,
        ],
    )

    assert result.exit_code != 0, "Expected non-zero exit code on agent failure"
    assert "ERROR: writing-agent failed" in result.output


# ---------------------------------------------------------------------------
# T3 — byte-identical on re-run
# ---------------------------------------------------------------------------

def test_write_byte_identical_on_rerun(tmp_path: Path) -> None:
    """Running orchestrator write twice with the same prompt produces identical output bytes."""
    runner = CliRunner()
    prompt_path = _make_prompt(tmp_path)
    out_path = tmp_path / "Script.json"
    agent_cmd = _make_fake_agent(tmp_path)

    common_args = [
        "write",
        "--prompt", str(prompt_path),
        "--out", str(out_path),
        "--writing-agent-cmd", agent_cmd,
    ]

    result1 = runner.invoke(cli, common_args)
    assert result1.exit_code == 0, f"First run failed: {result1.output}"
    bytes1 = out_path.read_bytes()

    result2 = runner.invoke(cli, common_args)
    assert result2.exit_code == 0, f"Second run failed: {result2.output}"
    bytes2 = out_path.read_bytes()

    assert bytes1 == bytes2, "Script.json bytes differ between runs"
