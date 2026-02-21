"""Tests for contracts/tools/verify_contracts.py.

T1 — Current goldens pass all checks.
T2 — A pretty-printed golden fails with NOT_CANONICAL.
T3 — A canonical but schema-invalid golden fails with SCHEMA_INVALID.
T4 — file:///placeholder/... URI passes determinism check.
T5 — file:///tmp, file:///home, file:///prod URIs fail determinism check.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load verify_contracts without an install step
# ---------------------------------------------------------------------------
_TOOL_PATH = (
    Path(__file__).parent.parent / "contracts" / "tools" / "verify_contracts.py"
)
_spec = importlib.util.spec_from_file_location("verify_contracts", _TOOL_PATH)
vc = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(vc)  # type: ignore[union-attr]

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CONTRACTS_DIR = Path(__file__).parent.parent / "contracts"
SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tmp_contracts(tmp_path: Path) -> Path:
    """Create a minimal temp contracts dir with schemas/ copied and empty allowlist."""
    # Copy schemas
    schemas_dir = tmp_path / "schemas"
    schemas_dir.mkdir()
    for src in SCHEMAS_DIR.glob("*.json"):
        (schemas_dir / src.name).write_bytes(src.read_bytes())

    # compat dir with empty allowlist
    compat_dir = tmp_path / "compat"
    compat_dir.mkdir()
    (compat_dir / "field_allowlist.json").write_text(
        '{"_comment":"test"}\n', encoding="utf-8"
    )

    return tmp_path


# ---------------------------------------------------------------------------
# T1 — Current goldens pass
# ---------------------------------------------------------------------------

def test_current_goldens_pass() -> None:
    """All contracts/goldens/**/*.json must pass canonical, schema, and determinism checks."""
    errors, count = vc.run_checks(CONTRACTS_DIR)
    assert errors == [], f"Unexpected contract errors:\n" + "\n".join(errors)
    assert count > 0, "Expected at least one golden to be checked"


# ---------------------------------------------------------------------------
# T2 — Non-canonical golden fails
# ---------------------------------------------------------------------------

def test_non_canonical_golden_fails(tmp_path: Path) -> None:
    """A pretty-printed golden file must be reported as NOT_CANONICAL."""
    contracts_dir = _make_tmp_contracts(tmp_path)

    golden_dir = contracts_dir / "goldens" / "test"
    golden_dir.mkdir(parents=True)

    # Write a pretty-printed (non-canonical) Script.json
    data = {
        "schema_version": "1.0.0",
        "script_id": "script-test",
        "project_id": "test-project",
        "title": "Test Script",
        "scenes": [],
    }
    (golden_dir / "Script.json").write_text(
        json.dumps(data, indent=2) + "\n", encoding="utf-8"
    )

    errors, count = vc.run_checks(contracts_dir)
    assert count == 1
    assert any("NOT_CANONICAL" in e for e in errors), (
        f"Expected NOT_CANONICAL error, got: {errors}"
    )


# ---------------------------------------------------------------------------
# T3 — Schema-invalid golden fails
# ---------------------------------------------------------------------------

def test_schema_invalid_golden_fails(tmp_path: Path) -> None:
    """A canonical but schema-invalid golden must be reported as SCHEMA_INVALID."""
    contracts_dir = _make_tmp_contracts(tmp_path)

    golden_dir = contracts_dir / "goldens" / "test"
    golden_dir.mkdir(parents=True)

    # Write a canonical Script.json that is missing required fields
    data = {"schema_version": "1.0.0"}
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n"
    (golden_dir / "Script.json").write_bytes(canonical.encode("utf-8"))

    errors, count = vc.run_checks(contracts_dir)
    assert count == 1
    assert any("SCHEMA_INVALID" in e for e in errors), (
        f"Expected SCHEMA_INVALID error, got: {errors}"
    )


# ---------------------------------------------------------------------------
# T4 — file:///placeholder URI passes determinism
# ---------------------------------------------------------------------------

def test_file_uri_placeholder_passes() -> None:
    """file:///placeholder and file:///placeholder/... must pass determinism."""
    allowed = [
        "file:///placeholder",
        "file:///placeholder/",
        "file:///placeholder/video/foo.mp4",
        "file:///placeholder/captions/bar.srt",
    ]
    for uri in allowed:
        data = {"video_uri": uri}
        errors = vc.check_determinism(data, "RenderOutput.json", {})
        assert errors == [], (
            f"Expected no error for {uri!r}, got: {errors}"
        )


# ---------------------------------------------------------------------------
# T5 — non-placeholder file:// URIs fail determinism
# ---------------------------------------------------------------------------

def test_file_uri_nonplaceholder_fails() -> None:
    """file:// URIs that are not file:///placeholder[/...] must be flagged."""
    bad_uris = [
        "file:///tmp/a",
        "file:///home/a",
        "file:///prod/a",
        "file:///private/data",
        "file:///placeholderX",   # starts with placeholder but not followed by / or end
    ]
    for uri in bad_uris:
        data = {"video_uri": uri}
        errors = vc.check_determinism(data, "RenderOutput.json", {})
        assert any("NON_DETERMINISTIC" in e for e in errors), (
            f"Expected NON_DETERMINISTIC for {uri!r}, got: {errors}"
        )
