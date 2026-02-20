"""Wave 8 tests: determinism gate normalization (run-identity field stripping)."""

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from orchestrator.cli import cli, _compare_contract_artifacts


# ---------------------------------------------------------------------------
# Project fixture
# ---------------------------------------------------------------------------

_PROJECT = {
    "id": "proj-det-norm",
    "title": "Determinism Normalization Test",
    "genre": "sci-fi",
}


# ---------------------------------------------------------------------------
# Stub helpers — reuse the pattern from test_wave7.py
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


# Default stubs (stable, no run-id embedding)
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
    data = {
        "schema_id": "RenderOutput",
        "schema_version": "1.0.0",
        "output_id": "ro-001",
        "hashes": {"video_sha256": "aabbcc", "captions_sha256": "ddeeff"},
    }
    _write_artifact_json(registry, pid, run_id, "RenderOutput", data)
    return data


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

class TestDeterminismNormalization:

    def test_run_identity_fields_do_not_cause_diff(self, tmp_path):
        """Stage stubs embed run_id[:8] into IDs; normalization strips them → pass."""
        project_file = tmp_path / "project.json"
        project_file.write_text(json.dumps(_PROJECT), encoding="utf-8")
        out_dir = tmp_path / "out"

        # Stubs that mimic real stage behaviour: IDs embed run_id[:8]
        def stage2_runid(project_config, run_id, registry):
            pid = project_config["id"]
            data = {
                "schema_id": "ShotList",
                "schema_version": "1.0.0",
                "script_id": f"script-{pid}-{run_id[:8]}",
                "shotlist_id": f"shotlist-{pid}-{run_id[:8]}",
                "timing_lock_hash": "abc123",
                "total_duration_sec": 60.0,
            }
            _write_artifact_json(registry, pid, run_id, "ShotList", data)
            return data

        def stage3_runid(project_config, run_id, registry):
            pid = project_config["id"]
            shotlist_id = f"shotlist-{pid}-{run_id[:8]}"
            manifest_id = f"manifest-{pid}-{run_id[:8]}"
            data = {
                "schema_id": "AssetManifest",
                "schema_version": "1.0.0",
                "manifest_id": manifest_id,
                "shotlist_ref": shotlist_id,
            }
            _write_artifact_json(registry, pid, run_id, "AssetManifest", data)
            return data

        def stage4_runid(project_config, run_id, registry):
            pid = project_config["id"]
            manifest_id = f"manifest-{pid}-{run_id[:8]}"
            plan_id = f"plan-{pid}-{run_id[:8]}"
            data = {
                "schema_id": "RenderPlan",
                "schema_version": "1.0.0",
                "plan_id": plan_id,
                "manifest_ref": manifest_id,
            }
            _write_artifact_json(registry, pid, run_id, "RenderPlan", data)
            return data

        def stage5_runid(project_config, run_id, registry):
            pid = project_config["id"]
            data = {
                "schema_id": "RenderOutput",
                "schema_version": "1.0.0",
                "output_id": f"ro-{run_id[:8]}",
                "request_id": f"req-{run_id[:8]}",
                "video_uri": f"file:///tmp/{run_id}/video.mp4",
                "captions_uri": f"file:///tmp/{run_id}/captions.srt",
                "hashes": {"video_sha256": "aabbcc", "captions_sha256": "ddeeff"},
            }
            _write_artifact_json(registry, pid, run_id, "RenderOutput", data)
            return data

        runner = CliRunner()
        with _patched_stages({
            "orchestrator.stages.stage2_script_to_shotlist.run": stage2_runid,
            "orchestrator.stages.stage3_shotlist_to_assetmanifest.run": stage3_runid,
            "orchestrator.stages.stage4_build_renderplan.run": stage4_runid,
            "orchestrator.stages.stage5_render_preview.run": stage5_runid,
        }):
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

    def test_timestamp_differences_do_not_cause_diff(self, tmp_path):
        """RenderOutput with different provenance.rendered_at each run → normalized away → pass."""
        project_file = tmp_path / "project.json"
        project_file.write_text(json.dumps(_PROJECT), encoding="utf-8")
        out_dir = tmp_path / "out"

        call_count = [0]

        def stage5_with_timestamp(project_config, run_id, registry):
            call_count[0] += 1
            pid = project_config["id"]
            data = {
                "schema_id": "RenderOutput",
                "schema_version": "1.0.0",
                "output_id": "ro-001",
                "hashes": {"video_sha256": "aabbcc", "captions_sha256": "ddeeff"},
                "provenance": {
                    "rendered_at": f"2026-01-01T00:00:0{call_count[0]}Z",
                },
            }
            _write_artifact_json(registry, pid, run_id, "RenderOutput", data)
            return data

        runner = CliRunner()
        with _patched_stages({
            "orchestrator.stages.stage5_render_preview.run": stage5_with_timestamp,
        }):
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

    def test_canon_decision_diff_causes_fail(self, tmp_path):
        """CanonDecision.json is never normalized; differing decision_id → mismatch."""
        dir_a = tmp_path / "run_a"
        dir_b = tmp_path / "run_b"
        dir_a.mkdir()
        dir_b.mkdir()

        (dir_a / "CanonDecision.json").write_text(json.dumps({
            "schema_id": "CanonDecision",
            "schema_version": "1.0.0",
            "decision": "allow",
            "decision_id": "canon-id-001",
        }, indent=2), encoding="utf-8")

        (dir_b / "CanonDecision.json").write_text(json.dumps({
            "schema_id": "CanonDecision",
            "schema_version": "1.0.0",
            "decision": "allow",
            "decision_id": "canon-id-002",  # different!
        }, indent=2), encoding="utf-8")

        diffs = _compare_contract_artifacts(dir_a, dir_b)

        assert any(
            d["artifact"] == "CanonDecision.json" and d["type"] == "json_field_mismatch"
            for d in diffs
        ), f"Expected CanonDecision.json mismatch in diffs, got: {diffs}"

    def test_semantic_content_diff_causes_fail(self, tmp_path):
        """total_duration_sec (semantic field) differs → normalization does not strip it → fail."""
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
            }
            _write_artifact_json(registry, pid, run_id, "ShotList", data)
            return data

        runner = CliRunner()
        with _patched_stages({
            "orchestrator.stages.stage2_script_to_shotlist.run": stage2_differ,
        }):
            result = runner.invoke(cli, [
                "investigate-determinism",
                "--project", str(project_file),
                "--out", str(out_dir),
            ])

        assert result.exit_code != 0

        report = json.loads(
            (out_dir / "DeterminismReport.json").read_text(encoding="utf-8")
        )
        assert report["status"] == "fail"
        assert any(
            d["artifact"] == "ShotList.json" and d["type"] == "json_field_mismatch"
            for d in report["diffs"]
        ), f"Expected ShotList.json json_field_mismatch in diffs, got: {report['diffs']}"
