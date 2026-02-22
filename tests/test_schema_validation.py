"""Tests for artifact schema validation (all 5 artifact types)."""

import json
import pytest
import jsonschema

from orchestrator.validator import ARTIFACT_SCHEMAS, SCHEMAS_DIR, validate_artifact


# ---------------------------------------------------------------------------
# Fixture factories
# ---------------------------------------------------------------------------


def make_valid_script() -> dict:
    return {
        "schema_id": "Script",
        "schema_version": "1.0.0",
        "script_id": "test-script-001",
        "project_id": "test-project",
        "title": "Test Script",
        "scenes": [
            {
                "scene_id": "scene-001",
                "location": "INT. TEST ROOM",
                "time_of_day": "DAY",
                "actions": [],
            }
        ],
    }


def make_valid_shotlist() -> dict:
    return {
        "schema_id": "ShotList",
        "schema_version": "1.0.0",
        "shotlist_id": "test-shotlist-001",
        "script_id": "test-script-001",
        "created_at": "1970-01-01T00:00:00Z",
        "timing_lock_hash": "a" * 64,
        "total_duration_sec": 3.5,
        "shots": [
            {
                "shot_id": "scene-001-shot-001",
                "scene_id": "scene-001",
                "duration_sec": 3.5,
                "camera_framing": "wide",
                "camera_movement": "STATIC",
                "audio_intent": {
                    "vo_speaker_id": None,
                    "vo_text": None,
                    "sfx_tags": [],
                    "music_mood": None,
                },
            }
        ],
    }


def make_valid_asset_manifest() -> dict:
    return {
        "schema_id": "AssetManifest",
        "schema_version": "1.0.0",
        "manifest_id": "test-manifest-001",
        "project_id": "test-project",
        "shotlist_ref": "test-shotlist-001",
        "character_packs": [],
        "backgrounds": [],
        "vo_items": [
            {
                "item_id": "vo-001",
                "speaker_id": "character-a",
                "text": "Hello world",
                "license_type": "generated_local",
            }
        ],
    }


def make_valid_render_plan() -> dict:
    return {
        "schema_id": "RenderPlan",
        "schema_version": "1.0.0",
        "plan_id": "test-plan-001",
        "project_id": "test-project",
        "manifest_ref": "test-manifest-001",
        "timing_lock_hash": "a" * 64,
        "profile": "preview_local",
        "resolution": "1280x720",
        "aspect_ratio": "16:9",
        "fps": 24,
        "resolved_assets": [
            {
                "asset_id": "asset-001",
                "asset_type": "background",
                "uri": "placeholder://background/scene-001",
                "license_type": "generated_local",
                "is_placeholder": True,
            }
        ],
    }


def make_valid_render_output() -> dict:
    return {
        "schema_id": "RenderOutput",
        "schema_version": "1.0.0",
        "output_id": "test-output-001",
        "video_uri": "file:///tmp/test/output.mp4",
        "captions_uri": "file:///tmp/test/output.srt",
        "hashes": {
            "video_sha256": "a" * 64,
            "captions_sha256": "b" * 64,
        },
    }


# ---------------------------------------------------------------------------
# Script
# ---------------------------------------------------------------------------


class TestScript:
    def test_valid_script(self):
        validate_artifact(make_valid_script(), "Script")  # no exception

    def test_invalid_script_missing_title(self):
        data = make_valid_script()
        del data["title"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "Script")

    def test_invalid_script_missing_scenes(self):
        data = make_valid_script()
        del data["scenes"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "Script")

    def test_invalid_script_missing_schema_version(self):
        data = make_valid_script()
        del data["schema_version"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "Script")


# ---------------------------------------------------------------------------
# ShotList
# ---------------------------------------------------------------------------


class TestShotList:
    def test_valid_shotlist(self):
        validate_artifact(make_valid_shotlist(), "ShotList")

    def test_invalid_shotlist_missing_timing_lock_hash(self):
        data = make_valid_shotlist()
        del data["timing_lock_hash"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "ShotList")

    def test_invalid_shotlist_missing_required(self):
        data = make_valid_shotlist()
        del data["shotlist_id"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "ShotList")

    def test_invalid_shotlist_missing_created_at(self):
        data = make_valid_shotlist()
        del data["created_at"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "ShotList")

    def test_invalid_shotlist_missing_total_duration_sec(self):
        data = make_valid_shotlist()
        del data["total_duration_sec"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "ShotList")


# ---------------------------------------------------------------------------
# AssetManifest
# ---------------------------------------------------------------------------


class TestAssetManifest:
    def test_valid_asset_manifest(self):
        validate_artifact(make_valid_asset_manifest(), "AssetManifest")

    def test_invalid_asset_manifest_missing_shotlist_ref(self):
        data = make_valid_asset_manifest()
        del data["shotlist_ref"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "AssetManifest")

    def test_invalid_asset_manifest_missing_required(self):
        data = make_valid_asset_manifest()
        del data["manifest_id"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "AssetManifest")


# ---------------------------------------------------------------------------
# RenderPlan
# ---------------------------------------------------------------------------


class TestRenderPlan:
    def test_valid_render_plan(self):
        validate_artifact(make_valid_render_plan(), "RenderPlan")

    def test_invalid_renderplan_bad_profile(self):
        """Profile must be one of the enum values."""
        data = make_valid_render_plan()
        data["profile"] = "ultra_hd_invalid"
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "RenderPlan")

    def test_invalid_render_plan_missing_manifest_ref(self):
        data = make_valid_render_plan()
        del data["manifest_ref"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "RenderPlan")

    def test_invalid_render_plan_missing_required(self):
        data = make_valid_render_plan()
        del data["plan_id"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "RenderPlan")


# ---------------------------------------------------------------------------
# RenderOutput
# ---------------------------------------------------------------------------


class TestRenderOutput:
    def test_valid_render_output(self):
        validate_artifact(make_valid_render_output(), "RenderOutput")

    def test_invalid_render_output_missing_video_uri(self):
        data = make_valid_render_output()
        del data["video_uri"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "RenderOutput")

    def test_invalid_render_output_missing_required(self):
        data = make_valid_render_output()
        del data["output_id"]
        with pytest.raises(jsonschema.ValidationError):
            validate_artifact(data, "RenderOutput")


# ---------------------------------------------------------------------------
# Schema structure — pipeline contract agreement
# ---------------------------------------------------------------------------


# Artifact types that _enforce_schema_metadata checks at runtime.
# Both schema_id and schema_version must be in required so that external
# producers (e.g. world-engine) are rejected at the schema level, not only
# at the pipeline gate.
_PIPELINE_ARTIFACT_TYPES = [
    "Script",
    "ShotList",
    "AssetManifest",
    "RenderPlan",
    "RenderOutput",
]


def test_pipeline_required_fields_in_all_schemas() -> None:
    """Every pipeline artifact schema must list schema_id and schema_version
    as required fields, consistent with what _enforce_schema_metadata enforces."""
    for artifact_type in _PIPELINE_ARTIFACT_TYPES:
        schema_file = SCHEMAS_DIR / ARTIFACT_SCHEMAS[artifact_type]
        schema = json.loads(schema_file.read_bytes())
        required = schema.get("required", [])
        assert "schema_id" in required, (
            f"{artifact_type}: 'schema_id' missing from required"
        )
        assert "schema_version" in required, (
            f"{artifact_type}: 'schema_version' missing from required"
        )
