"""Wave 7 tests: investigate-determinism CLI command."""

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from orchestrator.cli import cli


# ---------------------------------------------------------------------------
# Project fixture
# ---------------------------------------------------------------------------

_PROJECT = {
    "id": "proj-det-test",
    "title": "Determinism Test",
    "genre": "sci-fi",
}


# ---------------------------------------------------------------------------
# Stub helpers — bypass registry.write_artifact to avoid schema validation
# ---------------------------------------------------------------------------

def _write_artifact_json(
    registry, project_id: str, run_id: str, artifact_type: str, data: dict
) -> None:
    """Write artifact JSON directly to the run directory."""
    run_dir = registry.run_dir(project_id, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / f"{artifact_type}.json").write_text(
        json.dumps(data, indent=2), encoding="utf-8"
    )


def _stage1_stub(project_config, run_id, registry):
    pid = project_config["id"]
    data = {
        "schema_id": "Script",
        "schema_version": "1.0.0",
        "script_id": "script-stub",
        "title": "Test",
    }
    _write_artifact_json(registry, pid, run_id, "Script", data)
    return data


def _stage2_stub(project_config, run_id, registry):
    pid = project_config["id"]
    data = {
        "schema_id": "ShotList",
        "schema_version": "1.0.0",
        "shotlist_id": "sl-001",
        "timing_lock_hash": "abc",
        "total_duration_sec": 60.0,
        "created_at": "2026-01-01T00:00:00Z",
    }
    _write_artifact_json(registry, pid, run_id, "ShotList", data)
    return data


def _stage3_stub(project_config, run_id, registry):
    pid = project_config["id"]
    data = {
        "schema_id": "AssetManifest",
        "schema_version": "1.0.0",
        "manifest_id": "am-001",
        "shotlist_ref": "sl-001",
    }
    _write_artifact_json(registry, pid, run_id, "AssetManifest", data)
    return data


def _stage4_stub(project_config, run_id, registry):
    pid = project_config["id"]
    data = {
        "schema_id": "RenderPlan",
        "schema_version": "1.0.0",
        "plan_id": "rp-001",
        "manifest_ref": "am-001",
    }
    _write_artifact_json(registry, pid, run_id, "RenderPlan", data)
    return data


def _stage5_stub(project_config, run_id, registry):
    pid = project_config["id"]
    run_dir = registry.run_dir(pid, run_id)
    render_dir = run_dir / "render_preview"
    render_dir.mkdir(parents=True, exist_ok=True)
    (render_dir / "video.mp4").write_bytes(b"\x00video")
    (render_dir / "captions.srt").write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nTest\n", encoding="utf-8"
    )
    data = {
        "schema_id": "RenderOutput",
        "schema_version": "1.0.0",
        "output_id": "ro-001",
        "video_uri": "file:///tmp/stable_video.mp4",
        "captions_uri": "file:///tmp/stable_captions.srt",
        "hashes": {"video_sha256": "aabbcc", "captions_sha256": "ddeeff"},
    }
    _write_artifact_json(registry, pid, run_id, "RenderOutput", data)
    return data


# Maps full patch targets → default stubs
_STAGE_PATCH_TARGETS = [
    ("orchestrator.stages.stage1_generate_script.run", _stage1_stub),
    ("orchestrator.stages.stage2_script_to_shotlist.run", _stage2_stub),
    ("orchestrator.stages.stage3_shotlist_to_assetmanifest.run", _stage3_stub),
    ("orchestrator.stages.stage4_build_renderplan.run", _stage4_stub),
    ("orchestrator.stages.stage5_render_preview.run", _stage5_stub),
]


def _patched_stages(overrides=None):
    """Return a context manager that patches all 5 stage run() functions.

    overrides: optional dict mapping full patch target string → replacement function.
    """
    stubs = dict(_STAGE_PATCH_TARGETS)
    if overrides:
        stubs.update(overrides)

    class _Ctx:
        def __enter__(self):
            self._stack = ExitStack()
            for target, fn in stubs.items():
                self._stack.enter_context(patch(target, new=fn))
            return self

        def __exit__(self, *args):
            return self._stack.__exit__(*args)

    return _Ctx()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestInvestigateDeterminism:

    def test_pass_when_runs_are_identical(self, tmp_path):
        """All stages return identical output both times → exit 0, status=pass, diffs=[]."""
        project_file = tmp_path / "project.json"
        project_file.write_text(json.dumps(_PROJECT), encoding="utf-8")
        out_dir = tmp_path / "out"

        runner = CliRunner()
        with _patched_stages():
            result = runner.invoke(cli, [
                "investigate-determinism",
                "--project", str(project_file),
                "--out", str(out_dir),
            ])

        assert result.exit_code == 0, result.output
        assert "OK: determinism pass" in result.output

        report = json.loads(
            (out_dir / "DeterminismReport.json").read_text(encoding="utf-8")
        )
        assert report["status"] == "pass"
        assert report["diffs"] == []

    def test_fail_when_runs_differ(self, tmp_path):
        """Stage 2 returns different total_duration_sec on 2nd call → fail status."""
        project_file = tmp_path / "project.json"
        project_file.write_text(json.dumps(_PROJECT), encoding="utf-8")
        out_dir = tmp_path / "out"

        call_count = [0]

        def stage2_differ(project_config, run_id, registry):
            call_count[0] += 1
            pid = project_config["id"]
            duration = 60.0 if call_count[0] == 1 else 999.0
            data = {
                "schema_id": "ShotList",
                "schema_version": "1.0.0",
                "shotlist_id": "sl-001",
                "timing_lock_hash": "abc",
                "total_duration_sec": duration,
                "created_at": "2026-01-01T00:00:00Z",
            }
            _write_artifact_json(registry, pid, run_id, "ShotList", data)
            return data

        runner = CliRunner()
        with _patched_stages(
            {"orchestrator.stages.stage2_script_to_shotlist.run": stage2_differ}
        ):
            result = runner.invoke(cli, [
                "investigate-determinism",
                "--project", str(project_file),
                "--out", str(out_dir),
            ])

        assert result.exit_code != 0
        assert "FAIL" in result.output

        report = json.loads(
            (out_dir / "DeterminismReport.json").read_text(encoding="utf-8")
        )
        assert report["status"] == "fail"
        assert len(report["diffs"]) > 0
        assert any(
            d["artifact"] == "ShotList.json" and d["type"] == "json_field_mismatch"
            for d in report["diffs"]
        )

    def test_report_is_deterministic(self, tmp_path):
        """Two invocations on the same project → byte-identical DeterminismReport.json."""
        project_file = tmp_path / "project.json"
        project_file.write_text(json.dumps(_PROJECT), encoding="utf-8")
        out1 = tmp_path / "out1"
        out2 = tmp_path / "out2"

        runner = CliRunner()
        with _patched_stages():
            result1 = runner.invoke(cli, [
                "investigate-determinism",
                "--project", str(project_file),
                "--out", str(out1),
            ])
            result2 = runner.invoke(cli, [
                "investigate-determinism",
                "--project", str(project_file),
                "--out", str(out2),
            ])

        assert result1.exit_code == 0, result1.output
        assert result2.exit_code == 0, result2.output

        report1_bytes = (out1 / "DeterminismReport.json").read_bytes()
        report2_bytes = (out2 / "DeterminismReport.json").read_bytes()
        assert report1_bytes == report2_bytes, (
            "DeterminismReport.json bytes differ between two invocations on the same project"
        )

    def test_optional_artifact_compared_when_present(self, tmp_path):
        """render_preview/render_output.json with differing content appears in diffs."""
        project_file = tmp_path / "project.json"
        project_file.write_text(json.dumps(_PROJECT), encoding="utf-8")
        out_dir = tmp_path / "out"

        call_count = [0]

        def stage5_optional_differ(project_config, run_id, registry):
            call_count[0] += 1
            pid = project_config["id"]
            run_dir = registry.run_dir(pid, run_id)
            render_dir = run_dir / "render_preview"
            render_dir.mkdir(parents=True, exist_ok=True)
            (render_dir / "video.mp4").write_bytes(b"\x00video")
            (render_dir / "captions.srt").write_text(
                "1\n00:00:00,000 --> 00:00:01,000\nTest\n", encoding="utf-8"
            )
            # Write optional artifact with different frame_count on 2nd call
            optional_data = {"frame_count": 100 if call_count[0] == 1 else 200}
            (render_dir / "render_output.json").write_text(
                json.dumps(optional_data, indent=2), encoding="utf-8"
            )
            data = {
                "schema_id": "RenderOutput",
                "schema_version": "1.0.0",
                "output_id": "ro-001",
                "video_uri": "file:///tmp/stable_video.mp4",
                "captions_uri": "file:///tmp/stable_captions.srt",
                "hashes": {"video_sha256": "aabbcc", "captions_sha256": "ddeeff"},
            }
            _write_artifact_json(registry, pid, run_id, "RenderOutput", data)
            return data

        runner = CliRunner()
        with _patched_stages(
            {"orchestrator.stages.stage5_render_preview.run": stage5_optional_differ}
        ):
            result = runner.invoke(cli, [
                "investigate-determinism",
                "--project", str(project_file),
                "--out", str(out_dir),
            ])

        assert result.exit_code != 0
        report = json.loads(
            (out_dir / "DeterminismReport.json").read_text(encoding="utf-8")
        )
        assert any(
            d["artifact"] == "render_preview/render_output.json"
            for d in report["diffs"]
        ), f"Expected diff for render_preview/render_output.json, got: {report['diffs']}"
