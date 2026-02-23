"""Regression tests for asset ID hyphen-normalisation (slug consistency).

Bug: stage2 used .replace(" ", "_") and stage3 passed raw scene_id/character_id
through to asset_id fields.  The media-agent's make_placeholder() stores the
*normalised* form (underscores→hyphens) so any underscore in an orchestrator-
produced ID caused a mismatch between AssetManifest.json and RenderPlan.json.

These tests exercise the exact inputs that triggered the bug and assert that
every asset_id produced by stages 2 and 3 is in lowercase-hyphen form.
"""

import json
import re

import pytest

from orchestrator.registry import ArtifactRegistry
from orchestrator.stages import stage2_script_to_shotlist as stage2
from orchestrator.stages import stage3_shotlist_to_assetmanifest as stage3
from orchestrator.stages import stage4_build_renderplan as stage4

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SLUG_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")


def _is_slug(value: str) -> bool:
    """Return True if *value* is lowercase-hyphen-only (no underscores, no spaces)."""
    return bool(_SLUG_RE.match(value))


def _make_registry(tmp_path):
    return ArtifactRegistry(tmp_path)


PROJECT_CONFIG = {"id": "test-project", "title": "Test", "genre": "sci-fi"}
RUN_ID = "run-test0001"


# ---------------------------------------------------------------------------
# Scripts that trigger the bug
# ---------------------------------------------------------------------------

def _script_with_multiword_character():
    """Scene with a two-word character name → stage2 used to produce underscore IDs."""
    return {
        "schema_id": "Script",
        "schema_version": "1.0.0",
        "script_id": "script-test",
        "project_id": "test-project",
        "title": "Test",
        "genre": "sci-fi",
        "scenes": [
            {
                "scene_id": "scene-001",
                "location": "INT. BRIDGE",
                "time_of_day": "NIGHT",
                "actions": [
                    {"type": "dialogue", "character": "FIRST OFFICER", "text": "Report."},
                ],
            }
        ],
    }


def _script_with_underscore_scene_id():
    """Scene whose scene_id already contains an underscore (world-engine style)."""
    return {
        "schema_id": "Script",
        "schema_version": "1.0.0",
        "script_id": "script-test",
        "project_id": "test-project",
        "title": "Test",
        "genre": "sci-fi",
        "scenes": [
            {
                "scene_id": "scene_1",          # underscore — the classic trigger
                "location": "INT. COMMAND CENTER",
                "time_of_day": "NIGHT",
                "actions": [
                    {"type": "dialogue", "character": "COMMANDER", "text": "Go."},
                ],
            },
            {
                "scene_id": "scene_2",
                "location": "EXT. LAUNCH PAD",
                "time_of_day": "DAWN",
                "actions": [
                    {"type": "dialogue", "character": "ANALYST", "text": "Ready."},
                ],
            },
        ],
    }


# ---------------------------------------------------------------------------
# Stage 2: speaker_id / character_id slug tests
# ---------------------------------------------------------------------------

class TestStage2SlugIds:
    """Stage 2 stub must produce hyphen-only character / speaker IDs."""

    def _run_stage2(self, tmp_path, script: dict) -> dict:
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        run_dir.mkdir(parents=True, exist_ok=True)
        registry.write_artifact(
            "test-project", RUN_ID, "Script", script,
            parent_refs=[], creation_params={"stage": "stage1"},
        )
        return stage2.run(PROJECT_CONFIG, RUN_ID, registry)

    def test_multiword_character_name_produces_hyphen_speaker_id(self, tmp_path):
        """'FIRST OFFICER' → speaker_id must be 'first-officer', not 'first_officer'."""
        shotlist = self._run_stage2(tmp_path, _script_with_multiword_character())
        for shot in shotlist["shots"]:
            for char in shot.get("characters", []):
                cid = char["character_id"]
                assert "_" not in cid, (
                    f"character_id {cid!r} contains underscore; expected hyphen-only"
                )
                assert _is_slug(cid), f"character_id {cid!r} is not a valid slug"
            intent = shot.get("audio_intent", {})
            spk = intent.get("vo_speaker_id")
            if spk:
                assert "_" not in spk, (
                    f"vo_speaker_id {spk!r} contains underscore; expected hyphen-only"
                )
                assert _is_slug(spk), f"vo_speaker_id {spk!r} is not a valid slug"

    def test_multiword_character_specific_value(self, tmp_path):
        """'FIRST OFFICER' → exactly 'first-officer'."""
        shotlist = self._run_stage2(tmp_path, _script_with_multiword_character())
        speaker_ids = [
            shot["audio_intent"]["vo_speaker_id"]
            for shot in shotlist["shots"]
            if shot["audio_intent"].get("vo_speaker_id")
        ]
        assert speaker_ids, "expected at least one speaker_id"
        assert all(sid == "first-officer" for sid in speaker_ids), (
            f"expected 'first-officer', got {speaker_ids}"
        )


# ---------------------------------------------------------------------------
# Stage 3: asset_id slug tests
# ---------------------------------------------------------------------------

class TestStage3SlugIds:
    """Stage 3 must produce hyphen-only asset_id in all item types."""

    def _run_stages_2_3(self, tmp_path, script: dict) -> dict:
        """Run stage2 then stage3; return the AssetManifest_draft dict."""
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        run_dir.mkdir(parents=True, exist_ok=True)
        registry.write_artifact(
            "test-project", RUN_ID, "Script", script,
            parent_refs=[], creation_params={"stage": "stage1"},
        )
        stage2.run(PROJECT_CONFIG, RUN_ID, registry)
        return stage3.run(PROJECT_CONFIG, RUN_ID, registry)

    def _assert_no_underscore_asset_ids(self, manifest: dict):
        for pack in manifest.get("character_packs", []):
            aid = pack["asset_id"]
            assert "_" not in aid, f"character_pack asset_id {aid!r} has underscore"
            assert _is_slug(aid), f"character_pack asset_id {aid!r} is not a valid slug"

        for bg in manifest.get("backgrounds", []):
            aid = bg["asset_id"]
            assert "_" not in aid, f"background asset_id {aid!r} has underscore"
            assert _is_slug(aid), f"background asset_id {aid!r} is not a valid slug"

        for vo in manifest.get("vo_items", []):
            iid = vo["item_id"]
            assert "_" not in iid, f"vo item_id {iid!r} has underscore"
            assert _is_slug(iid), f"vo item_id {iid!r} is not a valid slug"
            spk = vo.get("speaker_id", "")
            assert "_" not in spk, f"vo speaker_id {spk!r} has underscore"

    def test_underscore_scene_id_produces_hyphen_bg_asset_id(self, tmp_path):
        """scene_id 'scene_1' → bg asset_id must be 'bg-scene-1', not 'bg-scene_1'."""
        manifest = self._run_stages_2_3(tmp_path, _script_with_underscore_scene_id())
        self._assert_no_underscore_asset_ids(manifest)

    def test_underscore_scene_id_specific_values(self, tmp_path):
        """scene_id 'scene_1' → exactly 'bg-scene-1'."""
        manifest = self._run_stages_2_3(tmp_path, _script_with_underscore_scene_id())
        bg_ids = [bg["asset_id"] for bg in manifest["backgrounds"]]
        assert "bg-scene-1" in bg_ids, f"expected 'bg-scene-1' in {bg_ids}"
        assert "bg-scene-2" in bg_ids, f"expected 'bg-scene-2' in {bg_ids}"
        assert "bg-scene_1" not in bg_ids, f"underscore form must not appear: {bg_ids}"

    def test_multiword_character_produces_hyphen_char_asset_id(self, tmp_path):
        """'FIRST OFFICER' → char asset_id must be 'char-first-officer'."""
        manifest = self._run_stages_2_3(tmp_path, _script_with_multiword_character())
        self._assert_no_underscore_asset_ids(manifest)
        char_ids = [p["asset_id"] for p in manifest["character_packs"]]
        assert "char-first-officer" in char_ids, (
            f"expected 'char-first-officer' in {char_ids}"
        )

    def test_assetmanifest_json_bridge_also_uses_hyphens(self, tmp_path):
        """The AssetManifest.json bridge file written for the media-agent must
        also use hyphen-only asset IDs."""
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        run_dir.mkdir(parents=True, exist_ok=True)
        registry.write_artifact(
            "test-project", RUN_ID, "Script", _script_with_underscore_scene_id(),
            parent_refs=[], creation_params={"stage": "stage1"},
        )
        stage2.run(PROJECT_CONFIG, RUN_ID, registry)
        stage3.run(PROJECT_CONFIG, RUN_ID, registry)

        bridge_path = run_dir / "AssetManifest.json"
        assert bridge_path.exists(), "AssetManifest.json bridge file was not written"
        bridge = json.loads(bridge_path.read_text(encoding="utf-8"))

        for bg in bridge.get("backgrounds", []):
            aid = bg["asset_id"]
            assert "_" not in aid, (
                f"AssetManifest.json bridge background asset_id {aid!r} has underscore — "
                "media-agent would normalise it causing a mismatch with RenderPlan.json"
            )
        for pack in bridge.get("character_packs", []):
            aid = pack["asset_id"]
            assert "_" not in aid, (
                f"AssetManifest.json bridge character asset_id {aid!r} has underscore"
            )


# ---------------------------------------------------------------------------
# End-to-end cross-document consistency: stage3 → media-agent sim → stage4
# ---------------------------------------------------------------------------

def _media_agent_normalize(s: str) -> str:
    """Mirror the media-agent's _normalize_id() exactly."""
    return s.strip().lower().replace(" ", "-").replace("_", "-")


def _resolved_asset(asset_id: str, asset_type: str, license_type: str) -> dict:
    """Build a fully schema-valid ResolvedAsset with the given (normalised) asset_id."""
    return {
        "schema_id": "urn:media:resolved-asset",
        "schema_version": "1.0.0",
        "producer": "test-media-sim",
        "asset_id": asset_id,
        "asset_type": asset_type,
        "uri": f"placeholder://{asset_type}/{asset_id}",
        "is_placeholder": True,
        "metadata": {
            "license_type": license_type,
            "retrieval_date": "1970-01-01T00:00:00Z",
        },
        "source": {"type": "generated_placeholder"},
        "license": {"spdx_id": "LicenseRef-proprietary", "attribution_required": False},
    }


def _build_media_manifest(bridge: dict, manifest_id: str) -> dict:
    """Simulate what the media-agent produces from the AssetManifest.json bridge.

    In Phase 0, the media-agent goes through the placeholder path for every
    asset (no real asset library).  make_placeholder() stores the *normalised*
    asset_id (underscores → hyphens).  We replicate that here so the test
    exercises the same transformation the real agent applies.
    """
    items = []
    for pack in bridge.get("character_packs", []):
        norm_id = _media_agent_normalize(pack["asset_id"])
        items.append(_resolved_asset(norm_id, "character",
                                     pack.get("license_type", "proprietary_cleared")))
    for bg in bridge.get("backgrounds", []):
        norm_id = _media_agent_normalize(bg["asset_id"])
        items.append(_resolved_asset(norm_id, "background",
                                     bg.get("license_type", "proprietary_cleared")))
    return {
        "schema_id": "AssetManifest.media",
        "schema_version": "1.0.0",
        "manifest_id": manifest_id,
        "producer": "test-media-sim",
        "items": items,
    }


class TestAssetIdCrossDocumentConsistency:
    """Full stage3 → simulated media-agent → stage4 consistency check.

    This is the definitive regression test for the reported bug.  It exercises
    the exact failure mode: if stage3 emits underscore IDs, the simulated
    media-agent normalises them to hyphens, and then stage4 would produce
    RenderPlan asset_ids that differ from the AssetManifest.json ones.
    """

    def _run_pipeline(self, tmp_path, script: dict) -> tuple[dict, dict]:
        """Run stages 2→3→4 and return (bridge_manifest, render_plan)."""
        registry = _make_registry(tmp_path)
        run_dir = registry.run_dir("test-project", RUN_ID)
        run_dir.mkdir(parents=True, exist_ok=True)

        # Stage 1 output (pre-write Script)
        registry.write_artifact(
            "test-project", RUN_ID, "Script", script,
            parent_refs=[], creation_params={"stage": "stage1"},
        )

        # Stage 2: Script → ShotList
        stage2.run(PROJECT_CONFIG, RUN_ID, registry)

        # Stage 3: ShotList → AssetManifest_draft + AssetManifest.json bridge
        stage3.run(PROJECT_CONFIG, RUN_ID, registry)

        bridge = json.loads((run_dir / "AssetManifest.json").read_text(encoding="utf-8"))

        # Simulate the media-agent: normalise IDs as make_placeholder() would,
        # then write AssetManifest.media.json for stage4 to consume.
        draft = registry.read_artifact("test-project", RUN_ID, "AssetManifest_draft")
        media_manifest = _build_media_manifest(bridge, draft["manifest_id"])
        (run_dir / "AssetManifest.media.json").write_text(
            json.dumps(media_manifest, indent=2), encoding="utf-8"
        )

        # Stage 4: merge manifests → RenderPlan
        render_plan = stage4.run(PROJECT_CONFIG, RUN_ID, registry)

        return bridge, render_plan

    def test_renderplan_asset_ids_match_assetmanifest(self, tmp_path):
        """Every asset_id in RenderPlan.resolved_assets must equal the
        corresponding asset_id in AssetManifest.json (byte-identical).

        Re-introducing the bug (removing _to_slug from stage3) causes
        AssetManifest.json to have 'bg-scene_1' while the simulated media-agent
        output (and therefore RenderPlan) would have 'bg-scene-1' → this test
        fails with a clear diff.
        """
        bridge, render_plan = self._run_pipeline(
            tmp_path, _script_with_underscore_scene_id()
        )

        # Collect all asset_ids from the bridge (what AssetManifest.json contains)
        bridge_ids = set()
        for pack in bridge.get("character_packs", []):
            bridge_ids.add(pack["asset_id"])
        for bg in bridge.get("backgrounds", []):
            bridge_ids.add(bg["asset_id"])

        # Collect all visual asset_ids from RenderPlan (non-visual VO items are
        # not in the bridge, so restrict comparison to character and background)
        visual_types = {"character", "background"}
        rp_ids = {
            a["asset_id"]
            for a in render_plan.get("resolved_assets", [])
            if a["asset_type"] in visual_types
        }

        assert bridge_ids == rp_ids, (
            f"asset_id mismatch between AssetManifest.json and RenderPlan.json:\n"
            f"  AssetManifest.json : {sorted(bridge_ids)}\n"
            f"  RenderPlan.json    : {sorted(rp_ids)}\n"
            f"  only in manifest   : {sorted(bridge_ids - rp_ids)}\n"
            f"  only in renderplan : {sorted(rp_ids - bridge_ids)}"
        )

    def test_no_underscores_in_renderplan_asset_ids(self, tmp_path):
        """Rendered asset_ids must be hyphen-only slugs (no underscores)."""
        _, render_plan = self._run_pipeline(
            tmp_path, _script_with_underscore_scene_id()
        )
        for asset in render_plan.get("resolved_assets", []):
            aid = asset["asset_id"]
            assert "_" not in aid, (
                f"RenderPlan resolved_asset asset_id {aid!r} contains underscore"
            )
