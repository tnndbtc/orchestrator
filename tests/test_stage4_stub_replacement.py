"""Regression tests for stage4 stub-file replacement.

Bug: Media-agent's local resolver can return minimal 1×1 test-fixture PNGs
(≈ 69 bytes, ``is_placeholder=False``) for character/background assets.  Stage 4
used to check ``if is_placeholder`` only — so those stub files passed through
unchanged.  The renderer's ``_is_stub_file`` guard then fired (files ≤ 100 bytes
are stubs) and exited 0 with empty stdout, causing stage5 to raise
``ValueError: Renderer stdout is not valid JSON``.

Fix: stage4 now also calls ``_is_stub_uri()`` and regenerates any stub-sized
file:// URI with a proper solid-colour PNG via ``_generate_visual_placeholder``.

These tests exercise:
  1.  ``_is_stub_uri`` helper logic.
  2.  Stage 4 end-to-end: stub file:// URI → proper PNG on disk → RenderPlan URI
      updated, ``is_placeholder`` kept False.
"""

import json
import struct
import zlib
from pathlib import Path

import pytest

from orchestrator.registry import ArtifactRegistry
from orchestrator.stages import stage4_build_renderplan as stage4

# ---------------------------------------------------------------------------
# File size helpers
# ---------------------------------------------------------------------------

def _stub_bytes() -> bytes:
    """1×1 solid PNG — 69 bytes, below the 100-byte stub threshold."""
    def chunk(name: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(name + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + name + data + struct.pack(">I", crc)

    sig  = b"\x89PNG\r\n\x1a\n"
    ihdr = chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    raw  = b"\x00" + bytes([128, 128, 128])       # 1 row × 1 pixel (grey)
    idat = chunk(b"IDAT", zlib.compress(raw))
    iend = chunk(b"IEND", b"")
    return sig + ihdr + idat + iend


def _large_bytes() -> bytes:
    """200 bytes of distinct values — well above the 100-byte stub threshold.

    ``_is_stub_uri`` only checks file size, so non-PNG content is fine here.
    """
    return bytes(range(200))


# ---------------------------------------------------------------------------
# Unit tests for _is_stub_uri
# ---------------------------------------------------------------------------

class TestIsStubUri:
    """Direct unit tests for the _is_stub_uri() helper."""

    def test_placeholder_uri_returns_false(self):
        assert stage4._is_stub_uri("placeholder://character/char-analyst") is False

    def test_empty_string_returns_false(self):
        assert stage4._is_stub_uri("") is False

    def test_none_handled_safely(self):
        # None is not a valid uri but should not raise
        assert stage4._is_stub_uri(None) is False  # type: ignore[arg-type]

    def test_missing_file_returns_false(self, tmp_path):
        """A file:// URI pointing to a non-existent path → False (OSError swallowed)."""
        missing = tmp_path / "does_not_exist.png"
        assert stage4._is_stub_uri(f"file://{missing}") is False

    def test_stub_file_returns_true(self, tmp_path):
        """A file:// URI pointing to a file ≤ 100 bytes → True."""
        stub = tmp_path / "stub.png"
        stub.write_bytes(_stub_bytes())
        assert stub.stat().st_size <= 100, "sanity: _stub_bytes must produce ≤ 100 bytes"
        assert stage4._is_stub_uri(f"file://{stub}") is True

    def test_real_file_returns_false(self, tmp_path):
        """A file:// URI pointing to a file > 100 bytes → False."""
        real = tmp_path / "real.bin"
        real.write_bytes(_large_bytes())
        assert real.stat().st_size > 100, "sanity: _large_bytes must produce > 100 bytes"
        assert stage4._is_stub_uri(f"file://{real}") is False

    def test_exactly_100_bytes_is_stub(self, tmp_path):
        """Exactly 100 bytes is still a stub (≤, not <)."""
        at_threshold = tmp_path / "edge.bin"
        at_threshold.write_bytes(b"x" * 100)
        assert stage4._is_stub_uri(f"file://{at_threshold}") is True

    def test_101_bytes_is_not_stub(self, tmp_path):
        """101 bytes is not a stub."""
        above = tmp_path / "above.bin"
        above.write_bytes(b"x" * 101)
        assert stage4._is_stub_uri(f"file://{above}") is False


# ---------------------------------------------------------------------------
# Helpers for stage4 integration tests
# ---------------------------------------------------------------------------

PROJECT_CONFIG = {"id": "test-project", "title": "Test Stub", "genre": "sci-fi"}
RUN_ID = "run-stub-test"

# 64 zero hex digits — valid timing_lock_hash
_ZERO_HASH = "0" * 64


def _make_registry(tmp_path: Path) -> ArtifactRegistry:
    return ArtifactRegistry(tmp_path)


def _write_prereqs(registry: ArtifactRegistry, run_dir: Path) -> None:
    """Write the minimal upstream artifacts stage4 requires.

    ShotList shape matches the schema golden (additionalProperties: false):
      required: created_at, schema_id, schema_version, script_id, shotlist_id,
                shots, timing_lock_hash, total_duration_sec
    """
    run_dir.mkdir(parents=True, exist_ok=True)

    registry.write_artifact(
        "test-project", RUN_ID, "ShotList",
        {
            "schema_id": "ShotList",
            "schema_version": "1.0.0",
            "created_at": "1970-01-01T00:00:00Z",
            "script_id": "script-stub-test",
            "shotlist_id": "sl-stub-test",
            "shots": [],
            "timing_lock_hash": _ZERO_HASH,
            "total_duration_sec": 0.0,
        },
        parent_refs=[], creation_params={"stage": "stage2"},
    )

    registry.write_artifact(
        "test-project", RUN_ID, "AssetManifest_draft",
        {
            "schema_id": "AssetManifest_draft",
            "schema_version": "1.0.0",
            "manifest_id": "mfst-stub-test",
            "project_id": "test-project",
            "episode_id": "s01e01",
            "shotlist_ref": "sl-stub-test",
            "character_packs": [],
            "backgrounds": [],
            "vo_items": [],
        },
        parent_refs=[], creation_params={"stage": "stage3"},
    )


def _resolved_item(
    asset_id: str,
    asset_type: str,
    uri: str,
    is_placeholder: bool,
    license_type: str = "CC0",
) -> dict:
    """Build a minimal valid resolved-asset item for AssetManifest.media.json."""
    return {
        "schema_id": "urn:media:resolved-asset",
        "schema_version": "1.0.0",
        "producer": "test",
        "asset_id": asset_id,
        "asset_type": asset_type,
        "uri": uri,
        "is_placeholder": is_placeholder,
        "metadata": {
            "license_type": license_type,
            "retrieval_date": "1970-01-01T00:00:00Z",
        },
        "source": {"type": "local"},
        "license": {"spdx_id": "CC0-1.0", "attribution_required": False},
    }


def _write_media_manifest(run_dir: Path, items: list[dict]) -> None:
    data = {
        "schema_id": "AssetManifest.media",
        "schema_version": "1.0.0",
        "manifest_id": "mfst-stub-test",
        "producer": "test",
        "items": items,
    }
    (run_dir / "AssetManifest.media.json").write_text(
        json.dumps(data, indent=2), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Stage 4 integration: stub replacement
# ---------------------------------------------------------------------------

class TestStage4StubReplacement:
    """Stage 4 must replace stub file:// URIs with proper Pillow-generated PNGs."""

    def test_stub_character_uri_is_replaced(self, tmp_path):
        """A character resolved from the test library (69-byte stub PNG, is_placeholder=False)
        must be replaced with a > 100 byte solid-colour PNG so the renderer guard never fires.
        """
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        _write_prereqs(registry, run_dir)

        # Simulated media-agent "found" a stub in its test library
        lib_dir = tmp_path / "lib" / "images"
        lib_dir.mkdir(parents=True)
        stub_png = lib_dir / "char-analyst.png"
        stub_png.write_bytes(_stub_bytes())
        assert stub_png.stat().st_size <= 100  # confirm it IS a stub

        _write_media_manifest(run_dir, [
            _resolved_item(
                "char-analyst", "character",
                f"file://{stub_png}",
                is_placeholder=False,  # media-agent says "resolved"!
            ),
        ])

        render_plan = stage4.run(PROJECT_CONFIG, RUN_ID, registry)

        resolved = {a["asset_id"]: a for a in render_plan["resolved_assets"]}
        assert "char-analyst" in resolved
        entry = resolved["char-analyst"]

        # URI must have been updated away from the 69-byte stub
        assert entry["uri"] != f"file://{stub_png}", (
            "Stage 4 must replace the stub URI with a newly generated PNG"
        )
        assert entry["uri"].startswith("file://"), (
            f"Expected a file:// URI, got {entry['uri']!r}"
        )

        # Generated PNG must be > 100 bytes (renderer Guard 2 threshold)
        new_path = Path(entry["uri"][len("file://"):])
        assert new_path.exists(), f"Generated PNG does not exist: {new_path}"
        size = new_path.stat().st_size
        assert size > 100, (
            f"Regenerated PNG is still a stub ({size} bytes ≤ 100); "
            "renderer Guard 2 will fire again"
        )

        # is_placeholder starts False, must stay False
        assert entry["is_placeholder"] is False

    def test_stub_background_uri_is_replaced(self, tmp_path):
        """Same replacement must happen for background assets."""
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        _write_prereqs(registry, run_dir)

        lib_dir = tmp_path / "lib"
        lib_dir.mkdir(parents=True)
        stub_png = lib_dir / "bg-scene-1.png"
        stub_png.write_bytes(_stub_bytes())

        _write_media_manifest(run_dir, [
            _resolved_item(
                "bg-scene-1", "background",
                f"file://{stub_png}",
                is_placeholder=False,
            ),
        ])

        render_plan = stage4.run(PROJECT_CONFIG, RUN_ID, registry)
        resolved = {a["asset_id"]: a for a in render_plan["resolved_assets"]}
        entry = resolved["bg-scene-1"]

        assert entry["uri"] != f"file://{stub_png}"
        new_path = Path(entry["uri"][len("file://"):])
        assert new_path.exists()
        assert new_path.stat().st_size > 100

    def test_real_character_uri_is_not_replaced(self, tmp_path):
        """A real (> 100 byte) file:// URI must be passed through unchanged."""
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        _write_prereqs(registry, run_dir)

        lib_dir = tmp_path / "lib"
        lib_dir.mkdir(parents=True)
        real_png = lib_dir / "char-commander.png"
        real_png.write_bytes(_large_bytes())
        assert real_png.stat().st_size > 100  # sanity

        original_uri = f"file://{real_png}"
        _write_media_manifest(run_dir, [
            _resolved_item(
                "char-commander", "character",
                original_uri,
                is_placeholder=False,
            ),
        ])

        render_plan = stage4.run(PROJECT_CONFIG, RUN_ID, registry)
        resolved = {a["asset_id"]: a for a in render_plan["resolved_assets"]}
        entry = resolved["char-commander"]

        # URI must be preserved exactly — no needless regeneration
        assert entry["uri"] == original_uri, (
            f"Real PNG URI was unexpectedly changed: {original_uri!r} → {entry['uri']!r}"
        )
        assert entry["is_placeholder"] is False

    def test_placeholder_visual_asset_is_replaced(self, tmp_path):
        """An explicit placeholder:// visual asset continues to be replaced (existing behaviour)."""
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        _write_prereqs(registry, run_dir)

        _write_media_manifest(run_dir, [
            _resolved_item(
                "char-analyst", "character",
                "placeholder://character/char-analyst",
                is_placeholder=True,
            ),
        ])

        render_plan = stage4.run(PROJECT_CONFIG, RUN_ID, registry)
        resolved = {a["asset_id"]: a for a in render_plan["resolved_assets"]}
        entry = resolved["char-analyst"]

        assert entry["uri"].startswith("file://"), (
            f"placeholder:// URI was not replaced with file://: {entry['uri']!r}"
        )
        new_path = Path(entry["uri"][len("file://"):])
        assert new_path.exists() and new_path.stat().st_size > 100
        assert entry["is_placeholder"] is False

    def test_vo_stub_is_not_replaced(self, tmp_path):
        """VO assets are not visual — stub URIs must NOT be replaced."""
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        _write_prereqs(registry, run_dir)

        lib_dir = tmp_path / "lib"
        lib_dir.mkdir(parents=True)
        stub_wav = lib_dir / "vo-scene-1-commander-000.wav"
        stub_wav.write_bytes(b"x" * 44)  # 44-byte WAV stub

        original_uri = f"file://{stub_wav}"
        _write_media_manifest(run_dir, [
            _resolved_item(
                "vo-scene-1-commander-000", "vo",
                original_uri,
                is_placeholder=False,
            ),
        ])

        render_plan = stage4.run(PROJECT_CONFIG, RUN_ID, registry)
        resolved = {a["asset_id"]: a for a in render_plan["resolved_assets"]}
        entry = resolved["vo-scene-1-commander-000"]

        # VO stub must be passed through unchanged — only visual assets get PNG generation
        assert entry["uri"] == original_uri, (
            f"VO stub URI was unexpectedly modified: {original_uri!r} → {entry['uri']!r}"
        )

    def test_both_stubs_and_real_in_same_manifest(self, tmp_path):
        """When a manifest has mix of stubs and real assets, only stubs get replaced."""
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        _write_prereqs(registry, run_dir)

        lib_dir = tmp_path / "lib"
        lib_dir.mkdir(parents=True)

        stub_char = lib_dir / "char-analyst.png"
        stub_char.write_bytes(_stub_bytes())          # ≤ 100 bytes — will be replaced

        real_char = lib_dir / "char-commander.png"
        real_char.write_bytes(_large_bytes())         # > 100 bytes — keep as-is

        stub_bg = lib_dir / "bg-scene-1.png"
        stub_bg.write_bytes(_stub_bytes())            # ≤ 100 bytes — will be replaced

        _write_media_manifest(run_dir, [
            _resolved_item("char-analyst",   "character", f"file://{stub_char}", is_placeholder=False),
            _resolved_item("char-commander", "character", f"file://{real_char}", is_placeholder=False),
            _resolved_item("bg-scene-1",     "background", f"file://{stub_bg}",  is_placeholder=False),
        ])

        render_plan = stage4.run(PROJECT_CONFIG, RUN_ID, registry)
        resolved = {a["asset_id"]: a for a in render_plan["resolved_assets"]}

        # Stubs get new URIs pointing to > 100 byte files
        for aid in ("char-analyst", "bg-scene-1"):
            entry = resolved[aid]
            assert entry["uri"].startswith("file://")
            new_path = Path(entry["uri"][len("file://"):])
            assert new_path.stat().st_size > 100, (
                f"{aid}: regenerated file is still a stub ({new_path.stat().st_size} bytes)"
            )

        # Real file keeps its URI
        cmd_entry = resolved["char-commander"]
        assert cmd_entry["uri"] == f"file://{real_char}", (
            f"Real PNG URI was unexpectedly changed for char-commander"
        )
