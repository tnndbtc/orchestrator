"""Tests for stage5_render_preview — env-var renderer, stdout JSON, file URI checks."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from orchestrator.registry import ArtifactRegistry
from orchestrator.stages import stage5_render_preview


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_subprocess_result(returncode: int, stdout_bytes: bytes, stderr_bytes: bytes):
    """Return a MagicMock that looks like a CompletedProcess."""
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout_bytes
    m.stderr = stderr_bytes
    return m


def _minimal_ro(
    video_uri: str = "https://cdn.example.com/v.mp4",
    captions_uri: str = "https://cdn.example.com/c.srt",
) -> dict:
    """Return a schema-valid RenderOutput with configurable URIs."""
    return {
        "schema_version": "1.0.0",
        "output_id": "test-output-001",
        "video_uri": video_uri,
        "captions_uri": captions_uri,
        "hashes": {
            "video_sha256": "a" * 64,
            "captions_sha256": "b" * 64,
        },
    }


def _write_minimal_artifacts(registry: ArtifactRegistry, pid: str, run_id: str) -> None:
    """Write placeholder AssetManifest and RenderPlan so registry paths exist."""
    run_dir = registry.run_dir(pid, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    # These just need to be present on disk; stage5 passes their paths to the (mocked) renderer
    registry.artifact_path(pid, run_id, "AssetManifest").write_text("{}", encoding="utf-8")
    registry.artifact_path(pid, run_id, "RenderPlan").write_text("{}", encoding="utf-8")


# ---------------------------------------------------------------------------
# EnvironmentError when VIDEO_RENDERER_REPO is absent / empty
# ---------------------------------------------------------------------------

class TestMissingEnvVar:
    def test_unset_raises_environment_error(self, tmp_path, monkeypatch):
        monkeypatch.delenv("VIDEO_RENDERER_REPO", raising=False)
        registry = ArtifactRegistry(tmp_path)
        with pytest.raises(EnvironmentError, match="VIDEO_RENDERER_REPO"):
            stage5_render_preview.run({"id": "p"}, "r1", registry)

    def test_empty_string_raises_environment_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VIDEO_RENDERER_REPO", "")
        registry = ArtifactRegistry(tmp_path)
        with pytest.raises(EnvironmentError, match="VIDEO_RENDERER_REPO"):
            stage5_render_preview.run({"id": "p"}, "r1", registry)


# ---------------------------------------------------------------------------
# RuntimeError on non-zero subprocess exit
# ---------------------------------------------------------------------------

class TestRendererNonZeroExit:
    def test_nonzero_exit_raises_runtime_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VIDEO_RENDERER_REPO", "/fake/repo")
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)
        bad_result = _make_subprocess_result(1, b"", b"fatal: something went wrong")
        with patch("subprocess.run", return_value=bad_result):
            with pytest.raises(RuntimeError, match="Renderer exited with code 1"):
                stage5_render_preview.run({"id": pid}, run_id, registry)

    def test_runtime_error_includes_stderr(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VIDEO_RENDERER_REPO", "/fake/repo")
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)
        bad_result = _make_subprocess_result(2, b"", b"my detailed error message")
        with patch("subprocess.run", return_value=bad_result):
            with pytest.raises(RuntimeError, match="my detailed error message"):
                stage5_render_preview.run({"id": pid}, run_id, registry)


# ---------------------------------------------------------------------------
# ValueError on non-JSON stdout
# ---------------------------------------------------------------------------

class TestNonJsonStdout:
    def test_bad_json_raises_value_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VIDEO_RENDERER_REPO", "/fake/repo")
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)
        bad_result = _make_subprocess_result(0, b"this is not json", b"")
        with patch("subprocess.run", return_value=bad_result):
            with pytest.raises(ValueError, match="not valid JSON"):
                stage5_render_preview.run({"id": pid}, run_id, registry)


# ---------------------------------------------------------------------------
# FileNotFoundError for missing file:// URIs
# ---------------------------------------------------------------------------

class TestFileUriMissing:
    def test_video_uri_missing_raises_file_not_found(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VIDEO_RENDERER_REPO", "/fake/repo")
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)

        ro = _minimal_ro(
            video_uri=f"file://{tmp_path}/nonexistent_video.mp4",
            captions_uri=f"file://{tmp_path}/nonexistent_captions.srt",
        )
        ok_result = _make_subprocess_result(0, json.dumps(ro).encode(), b"")

        with patch("subprocess.run", return_value=ok_result):
            with pytest.raises(FileNotFoundError, match="video_uri"):
                stage5_render_preview.run({"id": pid}, run_id, registry)

    def test_captions_uri_missing_raises_file_not_found(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VIDEO_RENDERER_REPO", "/fake/repo")
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)

        # video file exists, captions file does not
        video_file = tmp_path / "video.mp4"
        video_file.write_bytes(b"fake video")

        ro = _minimal_ro(
            video_uri=f"file://{video_file}",
            captions_uri=f"file://{tmp_path}/nonexistent_captions.srt",
        )
        ok_result = _make_subprocess_result(0, json.dumps(ro).encode(), b"")

        with patch("subprocess.run", return_value=ok_result):
            with pytest.raises(FileNotFoundError, match="captions_uri"):
                stage5_render_preview.run({"id": pid}, run_id, registry)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestHappyPath:
    def test_non_file_uri_skips_disk_check(self, tmp_path, monkeypatch):
        """https:// URIs should be returned without disk existence checks."""
        monkeypatch.setenv("VIDEO_RENDERER_REPO", "/fake/repo")
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)

        ro = _minimal_ro()  # uses https:// URIs by default
        ok_result = _make_subprocess_result(0, json.dumps(ro).encode(), b"")

        with patch("subprocess.run", return_value=ok_result):
            result = stage5_render_preview.run({"id": pid}, run_id, registry)

        assert result == ro
        assert registry.exists_and_valid(pid, run_id, "RenderOutput")

    def test_file_uri_happy_path(self, tmp_path, monkeypatch):
        """file:// URIs that exist on disk pass the check; artifact is written."""
        monkeypatch.setenv("VIDEO_RENDERER_REPO", "/fake/repo")
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)

        # Create the actual files the renderer would have produced
        video_file = tmp_path / "video.mp4"
        captions_file = tmp_path / "captions.srt"
        video_file.write_bytes(b"fake video content")
        captions_file.write_bytes(b"fake captions")

        ro = _minimal_ro(
            video_uri=f"file://{video_file}",
            captions_uri=f"file://{captions_file}",
        )
        ok_result = _make_subprocess_result(0, json.dumps(ro).encode(), b"")

        with patch("subprocess.run", return_value=ok_result):
            result = stage5_render_preview.run({"id": pid}, run_id, registry)

        assert result == ro
        assert registry.exists_and_valid(pid, run_id, "RenderOutput")

    def test_return_value_is_unchanged(self, tmp_path, monkeypatch):
        """Renderer stdout JSON must be returned without mutation."""
        monkeypatch.setenv("VIDEO_RENDERER_REPO", "/fake/repo")
        pid, run_id = "p", "r1"
        registry = ArtifactRegistry(tmp_path)
        _write_minimal_artifacts(registry, pid, run_id)

        ro = _minimal_ro()
        ro["provenance"] = {"rendered_at": "2025-01-15T12:00:00Z"}  # real timestamp
        ok_result = _make_subprocess_result(0, json.dumps(ro).encode(), b"")

        with patch("subprocess.run", return_value=ok_result):
            result = stage5_render_preview.run({"id": pid}, run_id, registry)

        # Must NOT be mutated to "1970-01-01T00:00:00Z"
        assert result["provenance"]["rendered_at"] == "2025-01-15T12:00:00Z"
